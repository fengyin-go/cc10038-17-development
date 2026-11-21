"""可恢复归档批次领域逻辑。

把归档从“只改策略配置状态”推进到“按条件迁移批次原始正文”：
- 每个归档批次保存所选原始行的完整正文、摘要和归档位置；
- 原始行按 热存 → 已核验 → 已释放 → 已恢复 的顺序流转；
- 只有正文完整性核验通过的归档品才能释放热存、回退到仅剩归档的状态；
- 重放通过原批次号解析正文来源（热存/归档存储），重放占用期间暂缓释放热存；
- 重试归档按已确认行幂等跳过，不重复迁移。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.store import store

ARCHIVE_MODULE = "archive_batch"
LEASE_MODULE = "replay_lease"

# 归档批次状态：只允许沿顺序向前流转；核验失败是唯一的重试分支。
# 已核验只允许进入已归档/已恢复；复核不通过时也只能落到核验失败，不能停在仅剩归档。
BATCH_STATE_ORDER = ["待归档", "归档中", "已核验", "已归档", "已恢复"]
BATCH_STATE_FAILED = "核验失败"
BATCH_TRANSITIONS: dict[str, set[str]] = {
    "待归档": {"归档中"},
    "归档中": {"已核验", BATCH_STATE_FAILED},
    BATCH_STATE_FAILED: {"归档中"},
    "已核验": {"已归档", "已恢复", BATCH_STATE_FAILED},
    "已归档": {"已恢复"},
    "已恢复": set(),
}

# 原始行归档状态：热存 → 已核验 → 已释放 → 已恢复。
ROW_HOT = "热存"
ROW_VERIFIED = "已核验"
ROW_RELEASED = "已释放"
ROW_RESTORED = "已恢复"
ROW_TRANSITIONS: dict[str, set[str]] = {
    ROW_HOT: {ROW_VERIFIED},
    ROW_VERIFIED: {ROW_RELEASED, ROW_RESTORED},
    ROW_RELEASED: {ROW_RESTORED},
    ROW_RESTORED: set(),
}

SOURCE_HOT = "热存"
SOURCE_ARCHIVE = "归档存储"
SOURCE_MISSING = "不可用"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _digest(body: str) -> str:
    return hashlib.sha256((body or "").encode("utf-8")).hexdigest()


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def _line_key(row: dict[str, Any]) -> tuple[int, str]:
    raw = str(row.get("原始行号", ""))
    try:
        return (int(raw), raw)
    except (TypeError, ValueError):
        return (10**9, raw)


class ArchiveLedger:
    """归档批次账本：归档、核验、释放、恢复和重放占用的统一入口。"""

    # ---------- 查询 ----------
    def batches(self) -> list[dict[str, Any]]:
        return store.rows(ARCHIVE_MODULE)

    def leases(self) -> list[dict[str, Any]]:
        return store.rows(LEASE_MODULE)

    def find_batch(self, archive_no: str) -> dict[str, Any] | None:
        for entry in self.batches():
            if entry.get("归档批次号") == archive_no:
                return entry
        return None

    def batch_for_source(self, batch_no: str) -> dict[str, Any] | None:
        matched = [e for e in self.batches() if e.get("源批次号") == batch_no]
        if not matched:
            return None
        return max(matched, key=lambda e: int(e.get("id", 0)))

    def list_batches(
        self, *, status: str | None = None, batch_no: str | None = None
    ) -> list[dict[str, Any]]:
        rows = list(self.batches())
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if batch_no:
            rows = [row for row in rows if row.get("源批次号") == batch_no]
        return rows

    def raw_rows(self, batch_no: str) -> list[dict[str, Any]]:
        return sorted(
            [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no],
            key=_line_key,
        )

    # ---------- 归档创建与迁移 ----------
    def create_batch(
        self,
        batch_no: str,
        *,
        policy_id: int | None = None,
        policy: dict[str, Any] | None = None,
        conditions: dict[str, Any] | None = None,
        auto_release: bool = True,
    ) -> dict[str, Any]:
        source_rows = self.raw_rows(batch_no)
        if not source_rows:
            return {"ok": False, "message": f"批次 {batch_no} 没有可归档的原始日志", "batch": None}

        existing = self.batch_for_source(batch_no)
        if existing is not None and existing.get("status") in ("已归档", "已恢复", "已核验"):
            return {
                "ok": True,
                "message": f"批次 {batch_no} 已有归档批次 {existing['归档批次号']}，无需重复迁移",
                "batch": existing,
                "reused": True,
            }
        if existing is not None and existing.get("status") in ("归档中", BATCH_STATE_FAILED):
            return self.run_archive(existing["归档批次号"], conditions=conditions)

        archive_no = f"ARC-{_next_id(ARCHIVE_MODULE):04d}"
        entry: dict[str, Any] = {
            "id": _next_id(ARCHIVE_MODULE),
            "归档批次号": archive_no,
            "策略ID": policy_id,
            "策略名称": (policy or {}).get("策略名称", "手工归档"),
            "源批次号": batch_no,
            "归档格式": (policy or {}).get("归档格式", "raw-text"),
            "目标存储": (policy or {}).get("目标存储", "archive://cold-store"),
            "归档条件": self._condition_text(conditions),
            "归档条件字典": dict(conditions or {}),
            "status": "待归档",
            "归档状态": "待归档",
            "pending": True,
            "abnormal": False,
            "核验状态": "待核验",
            "归档进度": 0,
            "原始行数": len(source_rows),
            "已归档行数": 0,
            "已核验行数": 0,
            "已释放行数": 0,
            "已恢复行数": 0,
            "待释放行数": 0,
            "重试次数": 0,
            "本次迁移行数": 0,
            "跳过已确认行数": 0,
            "迁移失败行数": 0,
            "manifest": [],
            "manifest_text": "",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        self.batches().append(entry)
        run = self.run_archive(archive_no, conditions=conditions, auto_release=auto_release)
        return run

    def _condition_text(self, conditions: dict[str, Any] | None) -> str:
        if not conditions:
            return "整批原始日志"
        return json.dumps(conditions, ensure_ascii=False, sort_keys=True)

    def _select_rows(
        self, batch_no: str, conditions: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        rows = self.raw_rows(batch_no)
        conditions = conditions or {}
        statuses = conditions.get("解析状态") or conditions.get("parsed_statuses")
        if statuses:
            wanted = {statuses} if isinstance(statuses, str) else set(statuses)
            rows = [r for r in rows if r.get("解析状态") in wanted]
        before = conditions.get("采集时间戳不晚于") or conditions.get("before")
        if before:
            rows = [r for r in rows if str(r.get("采集时间戳", "")) <= str(before)]
        line_kw = conditions.get("原始行号包含") or conditions.get("line_contains")
        if line_kw:
            rows = [r for r in rows if str(line_kw) in str(r.get("原始行号", ""))]
        return rows

    def run_archive(
        self,
        archive_no: str,
        *,
        conditions: dict[str, Any] | None = None,
        auto_release: bool = True,
    ) -> dict[str, Any]:
        entry = self.find_batch(archive_no)
        if entry is None:
            return {"ok": False, "message": f"归档批次 {archive_no} 不存在", "batch": None}
        if entry.get("status") in ("已归档", "已恢复"):
            return {
                "ok": True,
                "message": f"归档批次 {archive_no} 已处于 {entry['status']}，不再重复迁移",
                "batch": entry,
                "skipped": True,
            }
        if entry.get("status") == "已核验" and entry.get("核验状态") == "通过":
            return {
                "ok": True,
                "message": f"归档批次 {archive_no} 已核验，等待释放热存",
                "batch": entry,
                "skipped": True,
            }
        if entry.get("status") not in ("待归档", BATCH_STATE_FAILED, "归档中"):
            return {"ok": False, "message": f"归档批次状态 {entry.get('status')} 不允许迁移", "batch": entry}

        if entry.get("status") != "归档中":
            if not self._set_state(entry, "归档中"):
                return {"ok": False, "message": "归档状态不能倒序流转", "batch": entry}

        select_conditions = conditions if conditions is not None else entry.get("归档条件字典")
        if conditions is not None:
            entry["归档条件字典"] = dict(conditions)
            entry["归档条件"] = self._condition_text(conditions)
        selected = self._select_rows(entry["源批次号"], select_conditions)
        if not selected:
            # 已有确认清单时（例如条件在重试时收窄），沿用原归档范围继续流转。
            if entry.get("manifest"):
                selected = [
                    store.find("raw_records", int(item.get("原始记录ID", 0)))
                    for item in entry.get("manifest", [])
                ]
                selected = [row for row in selected if row is not None]
            if not selected:
                entry["异常原因"] = "归档条件下没有命中原始日志"
                entry["abnormal"] = True
                return {"ok": False, "message": entry["异常原因"], "batch": entry}

        entry["原始行数"] = len(selected)
        manifest: list[dict[str, Any]] = entry.setdefault("manifest", [])
        by_record = {str(item.get("原始记录ID")): item for item in manifest}

        migrated = 0
        skipped = 0
        failed = 0
        for raw in selected:
            record_no = str(raw.get("id"))
            body = str(raw.get("原始正文") or "")
            item = by_record.get(record_no)

            # 已确认行直接跳过：重试归档不得重复迁移。
            if item is not None and item.get("迁移确认"):
                if item.get("正文") and _digest(str(item.get("正文"))) == item.get("正文摘要"):
                    skipped += 1
                    continue
                # 归档副本损坏：核验失败后重试时用仍可读的热存正文重新迁移该行。
                if body:
                    item["迁移确认"] = False
                else:
                    skipped += 1
                    continue
            if not body:
                # 正文已经释放但归档副本还在，同样视为已确认。
                if item is not None and item.get("正文"):
                    item["迁移确认"] = True
                    skipped += 1
                    continue
                failed += 1
                continue

            digest = _digest(body)
            location = f"archive://{archive_no}/line/{raw.get('原始行号')}"
            if item is None:
                item = {
                    "原始记录ID": raw.get("id"),
                    "原始行号": raw.get("原始行号"),
                    "批次编号": entry["源批次号"],
                    "归档批次号": archive_no,
                }
                manifest.append(item)
                by_record[record_no] = item
            else:
                item["原始行号"] = raw.get("原始行号")
            item.update({
                "正文": body,
                "正文摘要": digest,
                "字节数": len(body.encode("utf-8")),
                "归档位置": location,
                "归档格式": entry.get("归档格式", "raw-text"),
                "迁移确认": True,
                "迁移次数": int(item.get("迁移次数", 0)) + 1,
                "迁移时间": _now_iso(),
                "归档状态": ROW_VERIFIED,
                "释放状态": item.get("释放状态", "未释放"),
                "恢复状态": item.get("恢复状态", "未恢复"),
                "正文来源": SOURCE_ARCHIVE,
            })
            raw["归档批次号"] = archive_no
            raw["归档位置"] = location
            raw["正文摘要"] = digest
            raw["归档格式"] = entry.get("归档格式", "raw-text")
            raw["归档状态"] = raw.get("归档状态") or ROW_HOT
            raw["正文来源"] = SOURCE_HOT if raw.get("原始正文") else SOURCE_ARCHIVE
            migrated += 1

        entry["重试次数"] = int(entry.get("重试次数", 0)) + 1
        entry["本次迁移行数"] = migrated
        entry["跳过已确认行数"] = skipped
        entry["迁移失败行数"] = failed
        entry["updated_at"] = _now_iso()
        self._recompute_progress(entry)
        self._refresh_manifest(entry)

        verify = self.verify(archive_no)
        if not verify.get("ok"):
            return verify

        if auto_release:
            release = self.release(archive_no)
            return {
                "ok": True,
                "message": release.get("message", verify.get("message", "归档完成")),
                "batch": entry,
                "migrated": migrated,
                "skipped": skipped,
                "failed": failed,
                "verify": verify,
                "release": release,
            }
        return {
            "ok": True,
            "message": f"归档批次 {archive_no} 已迁移并核验，等待释放热存",
            "batch": entry,
            "migrated": migrated,
            "skipped": skipped,
            "failed": failed,
            "verify": verify,
        }

    # ---------- 正文完整性核验 ----------
    def verify(self, archive_no: str) -> dict[str, Any]:
        entry = self.find_batch(archive_no)
        if entry is None:
            return {"ok": False, "message": f"归档批次 {archive_no} 不存在", "batch": None}
        if entry.get("status") in ("已归档", "已恢复"):
            return {"ok": True, "message": f"归档批次 {archive_no} 已通过核验", "batch": entry}
        if entry.get("status") not in ("归档中", BATCH_STATE_FAILED, "已核验"):
            return {
                "ok": False,
                "message": f"归档批次状态 {entry.get('status')} 不能执行正文完整性核验",
                "batch": entry,
            }

        selected = self._select_rows(entry["源批次号"], entry.get("归档条件字典"))
        manifest = entry.get("manifest", [])
        by_record = {str(item.get("原始记录ID")): item for item in manifest}
        issues: list[str] = []
        missing = [
            str(raw.get("原始行号")) for raw in selected
            if str(raw.get("id")) not in by_record
        ]
        if missing:
            issues.append(f"原始行 {'、'.join(missing)} 不在归档清单中")
        extra = [
            str(item.get("原始行号")) for item in manifest
            if str(item.get("原始记录ID")) not in {str(raw.get("id")) for raw in selected}
        ]
        if extra:
            issues.append(f"归档清单包含未选中的原始行 {'、'.join(extra)}")

        for raw in selected:
            record_no = str(raw.get("id"))
            item = by_record.get(record_no)
            if item is None or not item.get("迁移确认"):
                issues.append(f"原始行 {raw.get('原始行号')} 缺少已确认的归档副本")
                continue
            if str(item.get("原始行号")) != str(raw.get("原始行号")):
                issues.append(f"原始行 {raw.get('原始行号')} 归档后行号变为 {item.get('原始行号')}")
            body = str(item.get("正文") or "")
            if not body:
                issues.append(f"原始行 {raw.get('原始行号')} 归档正文为空")
                continue
            if _digest(body) != item.get("正文摘要"):
                issues.append(f"原始行 {raw.get('原始行号')} 归档正文摘要不匹配")
            hot_body = str(raw.get("原始正文") or "")
            # 热存正文已经被释放时，摘要只能以归档副本为准，不能把缺失当作不一致。
            if hot_body and hot_body != body:
                issues.append(f"原始行 {raw.get('原始行号')} 热存正文与归档副本不一致")

        if issues:
            entry["核验状态"] = "失败"
            entry["核验问题"] = issues[:20]
            entry["异常原因"] = "归档正文完整性核验失败"
            entry["abnormal"] = True
            entry["updated_at"] = _now_iso()
            # 只对核验不通过的行撤销迁移确认，重试归档时重迁这些行；
            # 已确认且完好的行保持原样，重试不会重复迁移。
            for raw in selected:
                item = by_record.get(str(raw.get("id")))
                if item is None:
                    continue
                body = str(item.get("正文") or "")
                hot_body = str(raw.get("原始正文") or "")
                if (not body
                        or _digest(body) != item.get("正文摘要")
                        or (hot_body and hot_body != body)):
                    item["迁移确认"] = False
            if entry.get("status") in ("归档中", "已核验"):
                # 复核失败也只能落到核验失败：热存正文保持不释放，禁止停在仅剩归档。
                self._set_state(entry, BATCH_STATE_FAILED)
            self._refresh_manifest(entry)
            return {"ok": False, "message": "归档正文完整性核验失败，禁止释放热存", "batch": entry,
                    "issues": issues[:20]}

        entry["核验状态"] = "通过"
        entry["核验时间"] = _now_iso()
        entry["核验问题"] = []
        entry["abnormal"] = False
        if entry.get("status") == BATCH_STATE_FAILED:
            # 失败态重试：先记录一次顺序流转，再进入已核验。
            self._set_state(entry, "归档中")
        if entry.get("status") == "待归档":
            self._set_state(entry, "归档中")
        if entry.get("status") == "归档中":
            self._set_state(entry, "已核验")
        for raw in selected:
            if not raw.get("归档状态"):
                raw["归档状态"] = ROW_HOT
            if raw.get("归档状态") == ROW_HOT:
                self._set_row_state(raw, ROW_VERIFIED)
        self._recompute_progress(entry)
        self._refresh_manifest(entry)
        return {"ok": True, "message": "归档正文完整性核验通过", "batch": entry}

    # ---------- 释放热存 ----------
    def release(self, archive_no: str) -> dict[str, Any]:
        entry = self.find_batch(archive_no)
        if entry is None:
            return {"ok": False, "message": f"归档批次 {archive_no} 不存在", "batch": None}
        if entry.get("status") == "已归档":
            return {"ok": True, "message": f"归档批次 {archive_no} 已释放热存", "batch": entry}
        if entry.get("status") == "已恢复":
            return {"ok": True, "message": f"归档批次 {archive_no} 已恢复，热存正文可用", "batch": entry}
        if entry.get("status") != "已核验":
            return {
                "ok": False,
                "message": f"归档批次状态为 {entry.get('status')}，必须先核验通过才能释放热存",
                "batch": entry,
            }
        if entry.get("核验状态") != "通过":
            return {
                "ok": False,
                "message": "归档品未通过正文完整性核验，不允许回退到仅剩归档的状态",
                "batch": entry,
            }

        # 释放前再核对一次归档副本：避免核验后被篡改的副本顶替热存正文。
        tampered = [
            str(item.get("原始行号"))
            for item in entry.get("manifest", [])
            if item.get("迁移确认")
            and (not item.get("正文") or _digest(str(item.get("正文"))) != item.get("正文摘要"))
        ]
        if tampered:
            entry["核验状态"] = "失败"
            entry["核验问题"] = [f"原始行 {line} 归档正文摘要不匹配" for line in tampered]
            entry["异常原因"] = "释放前复核发现归档正文不完整"
            entry["abnormal"] = True
            self._set_state(entry, BATCH_STATE_FAILED)
            self._refresh_manifest(entry)
            return {
                "ok": False,
                "message": "释放前复核发现归档正文不完整，已拒绝释放热存",
                "batch": entry,
                "issues": entry["核验问题"],
            }

        active_lease = self.active_lease(entry["源批次号"])
        manifest = entry.get("manifest", [])
        released = 0
        deferred: list[str] = []
        for item in manifest:
            if not item.get("迁移确认"):
                continue
            if active_lease:
                deferred.append(str(item.get("原始行号")))
                continue
            raw = store.find("raw_records", int(item.get("原始记录ID", 0)))
            if raw is None:
                deferred.append(str(item.get("原始行号")))
                continue
            if raw.get("原始行号") != item.get("原始行号"):
                deferred.append(str(item.get("原始行号")))
                continue
            raw["原始正文"] = ""
            raw["归档批次号"] = archive_no
            raw["归档位置"] = item.get("归档位置")
            raw["正文摘要"] = item.get("正文摘要")
            raw["正文来源"] = SOURCE_ARCHIVE
            self._set_row_state(raw, ROW_RELEASED)
            raw["归档释放时间"] = _now_iso()
            item["释放状态"] = "已释放"
            item["释放时间"] = _now_iso()
            released += 1

        entry["待释放行数"] = len(deferred)
        entry["updated_at"] = _now_iso()
        if deferred:
            entry["归档中断原因"] = (
                f"批次 {entry['源批次号']} 正在被重放占用，暂缓释放 {len(deferred)} 行热存"
            )
            self._recompute_progress(entry)
            self._refresh_manifest(entry)
            return {
                "ok": True,
                "message": entry["归档中断原因"],
                "batch": entry,
                "released": released,
                "deferred": deferred,
                "blocked_by_replay": True,
            }

        entry["归档中断原因"] = ""
        self._set_state(entry, "已归档")
        entry["归档完成时间"] = _now_iso()
        entry["归档进度"] = 100
        self._recompute_progress(entry)
        self._refresh_manifest(entry)
        return {
            "ok": True,
            "message": f"归档批次 {archive_no} 已归档，{released} 行热存已释放",
            "batch": entry,
            "released": released,
            "deferred": [],
            "blocked_by_replay": False,
        }

    # ---------- 恢复正文 ----------
    def restore(self, archive_no: str) -> dict[str, Any]:
        entry = self.find_batch(archive_no)
        if entry is None:
            return {"ok": False, "message": f"归档批次 {archive_no} 不存在", "batch": None}
        if entry.get("status") == "已恢复":
            return {"ok": True, "message": f"归档批次 {archive_no} 已恢复", "batch": entry}
        if entry.get("status") not in ("已核验", "已归档"):
            return {
                "ok": False,
                "message": f"归档批次状态为 {entry.get('status')}，不能恢复正文",
                "batch": entry,
            }
        if entry.get("核验状态") != "通过":
            return {"ok": False, "message": "归档品未通过正文完整性核验，不能恢复", "batch": entry}

        manifest = [item for item in entry.get("manifest", []) if item.get("迁移确认")]
        if not manifest:
            return {"ok": False, "message": "归档清单为空，无法恢复正文", "batch": entry}

        # 先整体校验，再统一写回，避免恢复一半留下不一致状态。
        issues: list[str] = []
        targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for item in manifest:
            raw = store.find("raw_records", int(item.get("原始记录ID", 0)))
            if raw is None:
                issues.append(f"原始行 {item.get('原始行号')} 对应的原始记录不存在")
                continue
            if str(raw.get("原始行号")) != str(item.get("原始行号")):
                issues.append(f"原始行 {item.get('原始行号')} 行号已被改动")
                continue
            if not item.get("正文"):
                issues.append(f"原始行 {item.get('原始行号')} 归档正文缺失")
                continue
            if _digest(str(item.get("正文"))) != item.get("正文摘要"):
                issues.append(f"原始行 {item.get('原始行号')} 归档正文摘要不匹配")
                continue
            targets.append((raw, item))
        if issues:
            entry["abnormal"] = True
            entry["异常原因"] = "恢复前校验失败"
            entry["核验问题"] = issues[:20]
            return {"ok": False, "message": "恢复前校验失败，未写入任何正文", "batch": entry,
                    "issues": issues[:20]}

        for raw, item in targets:
            raw["原始正文"] = item.get("正文")
            raw["归档批次号"] = archive_no
            raw["归档位置"] = item.get("归档位置")
            raw["正文摘要"] = item.get("正文摘要")
            raw["正文来源"] = SOURCE_HOT
            raw["归档恢复时间"] = _now_iso()
            self._set_row_state(raw, ROW_RESTORED)
            item["恢复状态"] = "已恢复"
            item["恢复时间"] = _now_iso()

        self._set_state(entry, "已恢复")
        entry["恢复时间"] = _now_iso()
        entry["异常原因"] = ""
        entry["abnormal"] = False
        self._recompute_progress(entry)
        self._refresh_manifest(entry)
        return {
            "ok": True,
            "message": f"归档批次 {archive_no} 已恢复 {len(targets)} 行正文，原始行号保持不变",
            "batch": entry,
            "restored": len(targets),
        }

    # ---------- 正文来源解析 ----------
    def _manifest_item(
        self, batch_no: str, raw: dict[str, Any]
    ) -> dict[str, Any] | None:
        preferred = raw.get("归档批次号")
        candidates = [
            entry for entry in self.batches()
            if entry.get("源批次号") == batch_no
        ]
        candidates.sort(key=lambda e: (e.get("归档批次号") != preferred, -int(e.get("id", 0))))
        for entry in candidates:
            for item in entry.get("manifest", []):
                if str(item.get("原始记录ID")) == str(raw.get("id")) and item.get("正文"):
                    return item
        return None

    def resolve_rows(self, batch_no: str) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        for raw in self.raw_rows(batch_no):
            hot_body = str(raw.get("原始正文") or "")
            item = self._manifest_item(batch_no, raw)
            archive_body = str((item or {}).get("正文") or "")
            # 正文来源只报当前首选可用位置：热存优先，其次归档存储；归档副本位置另见归档位置。
            if hot_body:
                body, source = hot_body, SOURCE_HOT
            elif archive_body:
                body, source = archive_body, SOURCE_ARCHIVE
            else:
                body, source = "", SOURCE_MISSING

            if body and item is not None and not raw.get("归档位置"):
                raw["归档位置"] = item.get("归档位置")
            archive_no = (
                raw.get("归档批次号")
                or (item or {}).get("归档批次号")
                or (self.batch_for_source(batch_no) or {}).get("归档批次号", "")
            )
            resolved.append({
                "原始记录ID": raw.get("id"),
                "原始行号": raw.get("原始行号"),
                "正文": body,
                "正文来源": source,
                "归档批次号": archive_no or "",
                "归档位置": raw.get("归档位置") or (item or {}).get("归档位置", ""),
                "归档状态": raw.get("归档状态", ROW_HOT),
                "正文摘要": _digest(body) if body else "",
            })
        return resolved

    def availability(self, batch_no: str) -> dict[str, Any]:
        rows = self.resolve_rows(batch_no)
        summary = {SOURCE_HOT: 0, SOURCE_ARCHIVE: 0, SOURCE_MISSING: 0}
        for row in rows:
            summary[row["正文来源"]] = summary.get(row["正文来源"], 0) + 1
        return {
            "批次编号": batch_no,
            "行数": len(rows),
            "正文来源": summary,
            "可重放": bool(rows) and summary[SOURCE_MISSING] == 0,
            "重放占用中": self.active_lease(batch_no) is not None,
            "行明细": rows,
        }

    def resolve_body(self, batch_no: str, line_no: str | None = None) -> dict[str, Any]:
        rows = self.resolve_rows(batch_no)
        if line_no is not None:
            rows = [row for row in rows if str(row.get("原始行号")) == str(line_no)]
        if not rows:
            return {
                "ok": False,
                "message": f"批次 {batch_no} 没有可读取的原始正文",
                "批次编号": batch_no,
                "行明细": [],
            }
        return {
            "ok": True,
            "message": f"批次 {batch_no} 正文可用来源已解析",
            "批次编号": batch_no,
            "行明细": rows,
        }

    # ---------- 重放占用 ----------
    def active_lease(self, batch_no: str) -> dict[str, Any] | None:
        for lease in self.leases():
            if lease.get("批次编号") == batch_no and lease.get("状态") == "活跃":
                return lease
        return None

    def begin_replay_lease(self, batch_no: str, replay_id: int | str) -> dict[str, Any]:
        existing = self.active_lease(batch_no)
        if existing is not None:
            existing.setdefault("重放ID集合", [])
            if str(replay_id) not in existing["重放ID集合"]:
                existing["重放ID集合"].append(str(replay_id))
            return existing
        lease = {
            "id": _next_id(LEASE_MODULE),
            "批次编号": batch_no,
            "重放ID": str(replay_id),
            "重放ID集合": [str(replay_id)],
            "状态": "活跃",
            "占用开始时间": _now_iso(),
            "占用结束时间": "",
        }
        self.leases().append(lease)
        return lease

    def end_replay_lease(self, batch_no: str, replay_id: int | str) -> list[str]:
        lease = self.active_lease(batch_no)
        if lease is None:
            return []
        ids = lease.get("重放ID集合") or [lease.get("重放ID")]
        if str(replay_id) in ids:
            ids = [item for item in ids if item != str(replay_id)]
        lease["重放ID集合"] = ids
        if ids:
            return []
        lease["状态"] = "已结束"
        lease["占用结束时间"] = _now_iso()

        # 重放结束后继续完成被占用期间暂缓的热存释放。
        completed: list[str] = []
        for entry in self.batches():
            if entry.get("源批次号") != batch_no:
                continue
            if entry.get("status") == "已核验" and entry.get("核验状态") == "通过":
                result = self.release(entry["归档批次号"])
                if result.get("ok") and entry.get("status") == "已归档":
                    completed.append(entry["归档批次号"])
        return completed

    # ---------- 进度与清单 ----------
    def _recompute_progress(self, entry: dict[str, Any]) -> None:
        manifest = entry.get("manifest", [])
        confirmed = sum(1 for item in manifest if item.get("迁移确认"))
        entry["迁移确认行数"] = confirmed
        batch_no = entry.get("源批次号", "")
        archive_no = entry.get("归档批次号", "")
        rows = self.raw_rows(batch_no)
        released = sum(
            1 for row in rows
            if row.get("归档批次号") == archive_no and row.get("归档状态") == ROW_RELEASED
        )
        restored = sum(
            1 for row in rows
            if row.get("归档批次号") == archive_no and row.get("归档状态") == ROW_RESTORED
        )
        total = max(int(entry.get("原始行数", 0) or 0), 1)
        # 进度按“迁移 → 核验 → 释放”三段展示：重放占用导致释放暂缓时停在核验段。
        confirmation = int(entry.get("迁移确认行数", 0) or 0)
        if entry.get("status") in ("归档中", BATCH_STATE_FAILED):
            progress = 55 if confirmation else 15
        elif entry.get("status") == "已核验":
            progress = 70
        elif entry.get("status") in ("已归档", "已恢复"):
            progress = 100
        else:
            progress = 0
        entry["归档进度"] = progress
        entry["已归档行数"] = confirmed
        entry["已核验行数"] = confirmed if entry.get("核验状态") == "通过" else 0
        entry["已释放行数"] = released
        entry["已恢复行数"] = restored
        entry["状态计数"] = {
            ROW_HOT: sum(1 for row in rows if row.get("归档状态", ROW_HOT) == ROW_HOT),
            ROW_VERIFIED: sum(1 for row in rows if row.get("归档状态") == ROW_VERIFIED),
            ROW_RELEASED: released,
            ROW_RESTORED: restored,
        }

    def _refresh_manifest(self, entry: dict[str, Any]) -> None:
        self._recompute_progress(entry)
        rows = entry.get("manifest", [])
        # 清单里的行状态和正文来源以原始行的实时状态为准，保证归档侧和重放侧看到同一来源。
        for item in rows:
            raw = store.find("raw_records", int(item.get("原始记录ID", 0) or 0))
            if raw is None:
                item["行状态"] = "原始行缺失"
                item["正文来源"] = SOURCE_MISSING
                continue
            item["行状态"] = raw.get("归档状态", ROW_HOT)
            hot_body = str(raw.get("原始正文") or "")
            archive_copy = str(item.get("正文") or "")
            # 与重放侧一致：正文来源报当前首选可用位置（热存优先），归档位置单独列示。
            if hot_body:
                item["正文来源"] = SOURCE_HOT
            elif archive_copy:
                item["正文来源"] = SOURCE_ARCHIVE
            else:
                item["正文来源"] = SOURCE_MISSING
        header = [
            f"# 归档批次 {entry.get('归档批次号')} 可读清单",
            "",
            f"- 源批次号：{entry.get('源批次号')}",
            f"- 归档策略：{entry.get('策略名称')}",
            f"- 归档条件：{entry.get('归档条件')}",
            f"- 归档格式：{entry.get('归档格式')}",
            f"- 目标存储：{entry.get('目标存储')}",
            f"- 归档状态：{entry.get('status')}（进度 {entry.get('归档进度')}%）",
            f"- 核验状态：{entry.get('核验状态')}",
            f"- 行数：原始 {entry.get('原始行数')} / 已核验 {entry.get('已核验行数')} / "
            f"已释放 {entry.get('已释放行数')} / 已恢复 {entry.get('已恢复行数')}",
            f"- 重试次数：{entry.get('重试次数')}，本次跳过已确认行：{entry.get('跳过已确认行数')}",
            f"- 更新时间：{entry.get('updated_at')}",
            "",
            "| 原始行号 | 原始记录ID | 正文摘要 | 字节数 | 归档位置 | 行状态 | 正文来源 | 迁移确认 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for item in rows:
            header.append(
                "| {line} | {rid} | {digest} | {size} | {loc} | {state} | {source} | {confirmed} |".format(
                    line=item.get("原始行号", ""),
                    rid=item.get("原始记录ID", ""),
                    digest=str(item.get("正文摘要", ""))[:16],
                    size=item.get("字节数", 0),
                    loc=item.get("归档位置", ""),
                    state=item.get("行状态", item.get("归档状态", "")),
                    source=item.get("正文来源", ""),
                    confirmed="是" if item.get("迁移确认") else "否",
                )
            )
        if entry.get("归档中断原因"):
            header.extend(["", f"> {entry['归档中断原因']}"])
        entry["manifest_text"] = "\n".join(header)
        entry["manifest_rows"] = len(rows)
        entry["manifest_at"] = _now_iso()

    def manifest(self, archive_no: str, fmt: str = "md") -> tuple[bool, str]:
        entry = self.find_batch(archive_no)
        if entry is None:
            return False, f"归档批次 {archive_no} 不存在"
        self._refresh_manifest(entry)
        fmt = (fmt or "md").lower()
        if fmt in ("md", "markdown", "text"):
            return True, entry.get("manifest_text", "")
        rows = entry.get("manifest", [])
        header = ["原始行号", "原始记录ID", "正文摘要", "字节数", "归档位置", "行状态", "正文来源", "迁移确认"]
        if fmt == "json":
            return True, json.dumps(
                {
                    "归档批次号": entry.get("归档批次号"),
                    "源批次号": entry.get("源批次号"),
                    "归档状态": entry.get("status"),
                    "核验状态": entry.get("核验状态"),
                    "归档进度": entry.get("归档进度"),
                    "manifest": rows,
                },
                ensure_ascii=False,
                indent=2,
            )
        if fmt == "csv":
            lines = [",".join(header)]
            for item in rows:
                values = [
                    item.get("原始行号", ""),
                    item.get("原始记录ID", ""),
                    item.get("正文摘要", ""),
                    item.get("字节数", 0),
                    item.get("归档位置", ""),
                    item.get("行状态", item.get("归档状态", "")),
                    item.get("正文来源", ""),
                    "是" if item.get("迁移确认") else "否",
                ]
                lines.append(",".join(str(value).replace(",", "，") for value in values))
            return True, "\n".join(lines)
        return False, f"不支持的清单格式：{fmt}"

    def progress(self, batch_no: str) -> dict[str, Any]:
        entry = self.batch_for_source(batch_no)
        availability = self.availability(batch_no)
        lease = self.active_lease(batch_no)
        if entry is None:
            return {
                "批次编号": batch_no,
                "归档批次号": "",
                "归档状态": "未归档",
                "归档进度": 0,
                "核验状态": "未核验",
                "已核验行数": 0,
                "已释放行数": 0,
                "已恢复行数": 0,
                "热存行数": availability["正文来源"].get(SOURCE_HOT, 0),
                "归档存储行数": availability["正文来源"].get(SOURCE_ARCHIVE, 0),
                "正文来源": availability["正文来源"],
                "重放占用中": lease is not None,
                "可重放": availability["可重放"],
            }
        return {
            "批次编号": batch_no,
            "归档批次号": entry.get("归档批次号"),
            "归档状态": entry.get("status"),
            "归档进度": entry.get("归档进度", 0),
            "核验状态": entry.get("核验状态"),
            "已核验行数": entry.get("已核验行数", 0),
            "已释放行数": entry.get("已释放行数", 0),
            "已恢复行数": entry.get("已恢复行数", 0),
            "待释放行数": entry.get("待释放行数", 0),
            "热存行数": availability["正文来源"].get(SOURCE_HOT, 0),
            "归档存储行数": availability["正文来源"].get(SOURCE_ARCHIVE, 0),
            "正文来源": availability["正文来源"],
            "重放占用中": lease is not None,
            "归档中断原因": entry.get("归档中断原因", ""),
            "可重放": availability["可重放"],
        }

    # ---------- 状态流转 ----------
    def _set_state(self, entry: dict[str, Any], target: str) -> bool:
        current = str(entry.get("status") or "待归档")
        if current == target:
            return True
        if target not in BATCH_TRANSITIONS.get(current, set()):
            return False
        entry["status"] = target
        entry["归档状态"] = target
        entry["pending"] = target not in ("已归档", "已恢复")
        entry["abnormal"] = target == BATCH_STATE_FAILED
        entry["updated_at"] = _now_iso()
        return True

    def _set_row_state(self, row: dict[str, Any], target: str) -> bool:
        current = str(row.get("归档状态") or ROW_HOT)
        if current == target:
            return True
        if target not in ROW_TRANSITIONS.get(current, set()):
            return False
        row["归档状态"] = target
        return True


archive_ledger = ArchiveLedger()
