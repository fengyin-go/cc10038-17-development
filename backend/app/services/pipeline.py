"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.services.archive_service import (
    SOURCE_ARCHIVE,
    SOURCE_HOT,
    SOURCE_MISSING,
    archive_ledger,
)
from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def _replay_batch_numbers(self, history_batch_range: str) -> list[str]:
        """按原批次号定位重放目标：支持单批次或逗号分隔的批次号。"""
        parts = [part.strip() for part in str(history_batch_range).split(",") if part.strip()]
        if not parts:
            return []
        existing = {str(b.get("批次编号")) for b in store.rows("ingest_batch")}
        known: set[str] = set()
        for part in parts:
            archived = archive_ledger.batch_for_source(part)
            if archived is not None:
                known.add(str(archived.get("源批次号")))
        resolved: list[str] = []
        for part in parts:
            if part in existing or part in known:
                resolved.append(part)
                continue
            matches = [
                str(b.get("批次编号")) for b in store.rows("ingest_batch")
                if part in str(b.get("批次编号"))
            ]
            resolved.extend(matches or [part])
        return resolved

    def start_replay(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """登记重放任务并占用批次正文：占用期间归档会暂缓释放热存。"""
        replayers = store.rows("replay_engine")
        batch_numbers = self._replay_batch_numbers(history_batch_range)
        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "重放中",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
            "正文来源统计": {},
            "归档进度": {},
            "重放行数": 0,
            "缺失正文行数": 0,
            "批次编号列表": batch_numbers,
            "重放进度": "占用正文中",
        }
        replayers.append(replay_entry)

        progress_snapshots: dict[str, Any] = {}
        for batch_no in batch_numbers:
            archive_ledger.begin_replay_lease(batch_no, replay_id)
            progress_snapshots[batch_no] = archive_ledger.progress(batch_no)
        replay_entry["归档进度"] = progress_snapshots
        return {
            "ok": True,
            "message": f"重放 {replay_id} 已开始，正在占用批次正文",
            "replay": replay_entry,
            "归档进度": progress_snapshots,
        }

    def advance_replay(self, replay_id: int | str) -> dict[str, Any]:
        """执行已登记的重放任务：按原批次号逐行解析正文并释放占用。"""
        replayers = store.rows("replay_engine")
        replay_entry = None
        for entry in replayers:
            if str(entry.get("id")) == str(replay_id):
                replay_entry = entry
                break
        if replay_entry is None:
            return {"ok": False, "message": f"重放任务 {replay_id} 不存在"}
        if replay_entry.get("status") != "重放中":
            return {
                "ok": replay_entry.get("status") == "已完成",
                "message": f"重放任务 {replay_id} 当前状态为 {replay_entry.get('status')}，不能继续执行",
                "replay": replay_entry,
            }

        new_parser_rule = str(replay_entry.get("新解析规则版本"))
        batch_numbers = replay_entry.get("批次编号列表") or self._replay_batch_numbers(
            str(replay_entry.get("历史批次范围"))
        )

        parsers = store.rows("field_parser")
        rule = None
        for p in parsers:
            if p.get("规则名称") == new_parser_rule:
                rule = p
                break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            for batch_no in batch_numbers:
                archive_ledger.end_replay_lease(batch_no, replay_id)
            replay_entry["status"] = "部分失败"
            replay_entry["重放状态"] = "部分失败"
            replay_entry["pending"] = False
            replay_entry["abnormal"] = True
            replay_entry["重放进度"] = "无可用解析规则"
            return {
                "ok": False,
                "message": "无可用解析规则，重放未读取任何正文",
                "replay": replay_entry,
                "replayed_count": 0,
            }

        parsed = store.rows("parsed_entity")
        journal = store.rows("parse_journal")
        total_replayed = 0
        total_missing = 0
        source_totals: dict[str, int] = {}
        progress_snapshots: dict[str, Any] = {}
        per_batch: dict[str, Any] = {}

        for batch_no in batch_numbers:
            availability = archive_ledger.availability(batch_no)
            if not availability["行数"]:
                per_batch[batch_no] = {"ok": False, "reason": "批次没有原始日志", "replayed": 0,
                                       "missing": 0, "正文来源": {}}
                total_missing += 1
                archive_ledger.end_replay_lease(batch_no, replay_id)
                progress_snapshots[batch_no] = archive_ledger.progress(batch_no)
                continue

            batch_replayed = 0
            batch_missing = 0
            try:
                for row in availability["行明细"]:
                    if row["正文来源"] == SOURCE_MISSING or not row.get("正文"):
                        batch_missing += 1
                        continue
                    source_totals[row["正文来源"]] = source_totals.get(row["正文来源"], 0) + 1
                    raw_id = str(row["原始记录ID"])
                    try:
                        fields = self._parse_body(str(row["正文"]), rule)
                    except Exception:
                        batch_missing += 1
                        continue

                    entity = None
                    for candidate in parsed:
                        if (candidate.get("来源批次") == batch_no
                                and str(candidate.get("原始记录ID")) == raw_id):
                            entity = candidate
                            break
                    if entity is None:
                        entity = {
                            "id": _next_id("parsed_entity"),
                            "来源批次": batch_no,
                            "原始记录ID": raw_id,
                            "解析规则版本": new_parser_rule,
                            "标准化字段集": "{}",
                            "校验状态": "",
                            "status": "待校验",
                            "pending": True,
                            "abnormal": False,
                            "schema_version": "v1.0",
                        }
                        parsed.append(entity)
                    entity["标准化字段集"] = json.dumps(fields, ensure_ascii=False)
                    entity["解析规则版本"] = new_parser_rule
                    entity["校验状态"] = ""
                    entity["status"] = "待校验"
                    entity["pending"] = True
                    entity["abnormal"] = False
                    entity["replay_id"] = str(replay_id)
                    entity["replayed_at"] = _now_iso()
                    entity["source_of_body"] = row["正文来源"]
                    entity["content_digest"] = _stable_digest(fields)
                    batch_replayed += 1
            finally:
                # 无论解析是否中断都释放占用，避免归档一直无法回收热存。
                completed = archive_ledger.end_replay_lease(batch_no, replay_id)
                if completed:
                    per_batch.setdefault(batch_no, {})["归档续做"] = completed

            per_batch[batch_no] = {
                "ok": batch_missing == 0,
                "replayed": batch_replayed,
                "missing": batch_missing,
                "正文来源": availability["正文来源"],
                **per_batch.get(batch_no, {}),
            }
            total_replayed += batch_replayed
            total_missing += batch_missing
            journal.append({
                "id": _next_id("parse_journal"),
                "操作类型": "回溯重放",
                "批次编号": batch_no,
                "执行时间": _now_iso(),
                "影响的记录数": str(batch_replayed),
                "执行结果": "成功" if batch_missing == 0 else "部分成功",
                "status": "成功" if batch_missing == 0 else "部分成功",
                "pending": batch_missing > 0,
                "abnormal": batch_missing > 0,
                "replay_id": str(replay_id),
                "正文来源": availability["正文来源"],
            })
            progress_snapshots[batch_no] = archive_ledger.progress(batch_no)

        replay_entry["产出记录数"] = str(total_replayed)
        replay_entry["重放行数"] = total_replayed
        replay_entry["缺失正文行数"] = total_missing
        replay_entry["正文来源统计"] = source_totals
        replay_entry["归档进度"] = progress_snapshots
        replay_entry["批次明细"] = per_batch
        replay_entry["status"] = "已完成" if total_missing == 0 and total_replayed else "部分失败"
        replay_entry["pending"] = replay_entry["status"] != "已完成"
        replay_entry["abnormal"] = replay_entry["status"] != "已完成"
        replay_entry["重放状态"] = replay_entry["status"]
        replay_entry["重放进度"] = "已完成"

        return {
            "ok": replay_entry["status"] == "已完成",
            "message": (
                f"历史批次 {replay_entry.get('历史批次范围')} 用规则 {new_parser_rule} 重放完成"
                if replay_entry["status"] == "已完成"
                else f"历史批次 {replay_entry.get('历史批次范围')} 重放部分失败：正文缺失 {total_missing} 行"
            ),
            "replay": replay_entry,
            "replayed_count": total_replayed,
            "missing_count": total_missing,
            "正文来源统计": source_totals,
            "归档进度": progress_snapshots,
            "per_batch": per_batch,
        }

    def cancel_replay(self, replay_id: int | str) -> dict[str, Any]:
        """取消重放：释放占用，被暂缓的归档释放会继续完成。"""
        replay_entry = None
        for entry in store.rows("replay_engine"):
            if str(entry.get("id")) == str(replay_id):
                replay_entry = entry
                break
        if replay_entry is None:
            return {"ok": False, "message": f"重放任务 {replay_id} 不存在"}
        if replay_entry.get("status") not in ("重放中", "待重放"):
            return {
                "ok": False,
                "message": f"重放任务 {replay_id} 当前状态为 {replay_entry.get('status')}，无需取消",
                "replay": replay_entry,
            }

        batch_numbers = replay_entry.get("批次编号列表") or self._replay_batch_numbers(
            str(replay_entry.get("历史批次范围"))
        )
        progress_snapshots: dict[str, Any] = {}
        resumed: list[str] = []
        for batch_no in batch_numbers:
            resumed.extend(archive_ledger.end_replay_lease(batch_no, replay_id))
            progress_snapshots[batch_no] = archive_ledger.progress(batch_no)
        replay_entry["status"] = "部分失败"
        replay_entry["重放状态"] = "已取消"
        replay_entry["pending"] = False
        replay_entry["abnormal"] = True
        replay_entry["重放进度"] = "已取消"
        replay_entry["归档进度"] = progress_snapshots
        return {
            "ok": True,
            "message": f"重放任务 {replay_id} 已取消，归档释放占用已解除",
            "replay": replay_entry,
            "归档进度": progress_snapshots,
            "归档续做": resumed,
        }

    def replay_status(self, replay_id: int | str) -> dict[str, Any]:
        for entry in store.rows("replay_engine"):
            if str(entry.get("id")) == str(replay_id):
                batch_numbers = entry.get("批次编号列表") or self._replay_batch_numbers(
                    str(entry.get("历史批次范围"))
                )
                progress = {
                    batch_no: archive_ledger.progress(batch_no) for batch_no in batch_numbers
                }
                return {
                    "ok": True,
                    "replay": entry,
                    "归档进度": progress,
                    "重放占用": {
                        batch_no: archive_ledger.active_lease(batch_no) is not None
                        for batch_no in batch_numbers
                    },
                }
        return {"ok": False, "message": f"重放任务 {replay_id} 不存在", "replay": None}

    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """一次完成登记与执行，便于整体重放和自动化验证。"""
        started = self.start_replay(history_batch_range, new_parser_rule)
        result = self.advance_replay(started["replay"]["id"])
        return result

    # ---------- 归档批次编排 ----------
    def archive_batch(
        self,
        batch_no: str,
        *,
        policy_id: int | None = None,
        conditions: dict[str, Any] | None = None,
        auto_release: bool = True,
    ) -> dict[str, Any]:
        """按归档条件把批次原始正文迁移为可恢复的归档批次。"""
        policy = store.find("data_archive", policy_id) if policy_id is not None else None
        result = archive_ledger.create_batch(
            batch_no,
            policy_id=policy_id,
            policy=policy,
            conditions=conditions,
            auto_release=auto_release,
        )
        entry = result.get("batch")
        if entry is not None and policy is not None:
            policy["status"] = "已执行"
            policy["pending"] = False
            self._sync_policy(entry)
        return result

    def archive_by_policy(self, policy_id: int, batch_no: str) -> dict[str, Any]:
        """归档策略执行入口：策略条件可用于筛选批次原始行。"""
        policy = store.find("data_archive", policy_id)
        if policy is None:
            return {"ok": False, "message": f"归档策略 {policy_id} 不存在", "batch": None}
        conditions = policy.get("归档条件") or {}
        if isinstance(conditions, str):
            try:
                conditions = json.loads(conditions)
            except json.JSONDecodeError:
                conditions = {}
        return self.archive_batch(batch_no, policy_id=policy_id, conditions=conditions)

    def archive_progress(self, batch_no: str) -> dict[str, Any]:
        return archive_ledger.progress(batch_no)

    def verify_archive(self, archive_no: str) -> dict[str, Any]:
        result = archive_ledger.verify(archive_no)
        entry = result.get("batch")
        if entry is not None:
            self._sync_policy(entry)
        return result

    def release_archive(self, archive_no: str) -> dict[str, Any]:
        result = archive_ledger.release(archive_no)
        entry = result.get("batch")
        if entry is not None:
            self._sync_policy(entry)
        return result

    def restore_archive(self, archive_no: str) -> dict[str, Any]:
        result = archive_ledger.restore(archive_no)
        entry = result.get("batch")
        if entry is not None:
            self._sync_policy(entry)
        return result

    def pipeline_manifest(self, archive_no: str, fmt: str = "md") -> tuple[bool, str]:
        return archive_ledger.manifest(archive_no, fmt)

    def _sync_policy(self, entry: dict[str, Any]) -> None:
        """把归档批次结果回写归档策略，策略清单直接可读。"""
        policy_id = entry.get("策略ID")
        if policy_id is None:
            return
        policy = store.find("data_archive", int(policy_id))
        if policy is None:
            return
        policy["归档状态"] = entry.get("status")
        policy["归档批次号"] = entry.get("归档批次号")
        policy["已归档行数"] = entry.get("已归档行数", 0)
        policy["已恢复行数"] = entry.get("已恢复行数", 0)
        policy["归档进度"] = entry.get("归档进度", 0)
        policy["核验状态"] = entry.get("核验状态")
        policy["清单摘要"] = (entry.get("manifest_text") or "").splitlines()[0]
        policy["最近执行时间"] = _now_iso()
        policy["abnormal"] = entry.get("abnormal", False)

    def replay_bodies(self, batch_no: str, line_no: str | None = None) -> dict[str, Any]:
        """按原批次号展示每行正文当前从哪里可用。"""
        result = archive_ledger.resolve_body(batch_no, line_no)
        result["归档进度"] = archive_ledger.progress(batch_no)
        return result


    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()