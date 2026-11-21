"""可恢复归档批次测试：正文迁移、完整性核验、释放/恢复、重放按原批次号取正文。"""
from __future__ import annotations

import pytest

from app.services.archive_service import (
    BATCH_STATE_FAILED,
    ROW_RELEASED,
    ROW_RESTORED,
    SOURCE_ARCHIVE,
    SOURCE_HOT,
    archive_ledger,
)
from app.store import store


def _seed_batch(pipeline, source: str, count: int = 3) -> str:
    logs = [
        {"body": f"2026-01-15T10:3{i}:00|INFO|msg-{i}"}
        for i in range(count)
    ]
    pipeline.register_source(source, "http", "default")
    pipeline.validate_source(source)
    batch = pipeline.start_ingest_batch(source, "2026-01-15", logs)
    batch_no = batch["batch"]["批次编号"]
    pipeline.parse_batch(batch_no)
    pipeline.validate_entities(batch_no)
    return batch_no


def _raw_rows(batch_no: str) -> list[dict]:
    return sorted(
        [row for row in store.rows("raw_records") if row.get("批次编号") == batch_no],
        key=lambda row: int(row["原始行号"]),
    )


class TestArchiveBatch:
    """归档批次创建、迁移与清单。"""

    def test_archive_migrates_full_body_and_releases_hot(self, pipeline):
        batch_no = _seed_batch(pipeline, "arc-src")
        bodies = {row["id"]: row["原始正文"] for row in _raw_rows(batch_no)}

        result = pipeline.archive_batch(batch_no, policy_id=1)
        assert result["ok"] is True
        archive_no = result["batch"]["归档批次号"]
        assert result["batch"]["status"] == "已归档"
        assert result["batch"]["核验状态"] == "通过"
        assert result["batch"]["已归档行数"] == 3

        manifest = result["batch"]["manifest"]
        assert len(manifest) == 3
        for item in manifest:
            assert item["正文"] == bodies[item["原始记录ID"]]
            assert item["迁移确认"] is True
            assert item["归档位置"].startswith(f"archive://{archive_no}/line/")
        for row in _raw_rows(batch_no):
            assert row["原始正文"] == ""
            assert row["归档批次号"] == archive_no
            assert row["归档位置"]
            assert row["归档状态"] == ROW_RELEASED

    def test_archive_policy_result_written_back_readable(self, pipeline):
        batch_no = _seed_batch(pipeline, "policy-src")
        result = pipeline.archive_batch(batch_no, policy_id=2)
        policy = store.find("data_archive", 2)
        assert policy["status"] == "已执行"
        assert policy["归档批次号"] == result["batch"]["归档批次号"]
        assert policy["归档进度"] == 100
        assert policy["核验状态"] == "通过"
        assert "可读清单" in policy["清单摘要"]

    def test_archive_conditions_limit_selected_rows(self, pipeline):
        batch_no = _seed_batch(pipeline, "cond-src", count=4)
        result = pipeline.archive_batch(
            batch_no, conditions={"解析状态": "已解析", "原始行号包含": "1"},
        )
        assert result["ok"] is True
        assert result["batch"]["原始行数"] == 1
        assert [item["原始行号"] for item in result["batch"]["manifest"]] == ["1"]
        released = [row for row in _raw_rows(batch_no) if row.get("归档状态") == ROW_RELEASED]
        assert len(released) == 1

    def test_policy_execution_uses_policy_conditions(self, pipeline):
        from app.services import SERVICES

        batch_no = _seed_batch(pipeline, "policy-cond-src", count=3)
        policy = store.find("data_archive", 1)
        policy["归档条件"] = {"原始行号包含": "2"}

        result = SERVICES["data_archive"].exec_archive(1, batch_no)
        assert result["ok"] is True
        assert result["batch"]["原始行数"] == 1
        assert policy["归档批次号"] == result["batch"]["归档批次号"]
        assert policy["已归档行数"] == 1

    def test_manifest_readable_formats(self, pipeline):
        batch_no = _seed_batch(pipeline, "manifest-src")
        archive_no = pipeline.archive_batch(batch_no)["batch"]["归档批次号"]

        ok, text = pipeline.pipeline_manifest(archive_no, "md")
        assert ok is True
        assert "可读清单" in text
        assert "正文摘要" in text
        assert "原始行号" in text
        ok, csv_text = pipeline.pipeline_manifest(archive_no, "csv")
        assert ok is True
        assert csv_text.splitlines()[0].startswith("原始行号")
        ok, json_text = pipeline.pipeline_manifest(archive_no, "json")
        assert ok is True
        assert f'"归档批次号": "{archive_no}"' in json_text

    def test_retry_archive_does_not_remigrate_confirmed_rows(self, pipeline):
        batch_no = _seed_batch(pipeline, "idem-src")
        first = pipeline.archive_batch(batch_no, policy_id=1)
        archive_no = first["batch"]["归档批次号"]
        migrated_at = {
            item["原始记录ID"]: item["迁移时间"] for item in first["batch"]["manifest"]
        }

        second = pipeline.archive_batch(batch_no, policy_id=1)
        assert second["batch"]["归档批次号"] == archive_no
        for item in second["batch"]["manifest"]:
            assert item["迁移时间"] == migrated_at[item["原始记录ID"]]
            assert item["迁移次数"] == 1

        retried = archive_ledger.run_archive(archive_no)
        # 已归档批次不再迁移，清单里的已确认行保持不变。
        assert retried["batch"]["manifest"] == first["batch"]["manifest"]


class TestVerificationGate:
    """正文完整性核验是释放热存的硬门槛。"""

    def test_release_blocked_without_verification(self, pipeline):
        batch_no = _seed_batch(pipeline, "gate-src")
        result = pipeline.archive_batch(batch_no, auto_release=False)
        archive_no = result["batch"]["归档批次号"]
        assert result["batch"]["status"] == "已核验"

        # 篡改归档副本后复核不通过，状态落到核验失败。
        result["batch"]["manifest"][0]["正文"] = "tampered-body"
        verify = pipeline.verify_archive(archive_no)
        assert verify["ok"] is False
        assert verify["batch"]["status"] == BATCH_STATE_FAILED
        assert verify["batch"]["异常原因"]

        release = pipeline.release_archive(archive_no)
        assert release["ok"] is False
        assert "核验" in release["message"]
        # 未核验通过时热存正文保持可读，绝不会出现仅剩归档的状态。
        assert all(row["原始正文"] for row in _raw_rows(batch_no))
        assert all(row["归档状态"] != ROW_RELEASED for row in _raw_rows(batch_no))

    def test_state_cannot_skip_forward(self, pipeline):
        entry = {"status": "待归档", "归档状态": "待归档", "pending": True, "abnormal": False}
        assert archive_ledger._set_state(entry, "已归档") is False
        assert entry["status"] == "待归档"
        assert archive_ledger._set_state(entry, "归档中") is True
        # 归档中不能直接跳到已归档，必须先核验。
        assert archive_ledger._set_state(entry, "已归档") is False
        assert entry["status"] == "归档中"
        assert archive_ledger._set_state(entry, "已核验") is True
        assert archive_ledger._set_state(entry, "已归档") is True

    def test_release_requires_verified_state(self, pipeline):
        batch_no = _seed_batch(pipeline, "order-src")
        started = pipeline.start_replay(batch_no, "rule-order")
        result = pipeline.archive_batch(batch_no, auto_release=False)
        entry = result["batch"]
        assert entry["status"] == "已核验"
        # 有重放占用时释放被暂缓，状态不会越过核验。
        release = pipeline.release_archive(entry["归档批次号"])
        assert release["blocked_by_replay"] is True
        assert entry["status"] == "已核验"
        pipeline.advance_replay(started["replay"]["id"])
        # 占用结束后按 已核验 → 已归档 顺序前进。
        assert entry["status"] == "已归档"

    def test_release_rechecks_archive_copy(self, pipeline):
        batch_no = _seed_batch(pipeline, "recheck-src")
        result = pipeline.archive_batch(batch_no, auto_release=False)
        archive_no = result["batch"]["归档批次号"]
        result["batch"]["manifest"][1]["正文"] = "silently-changed"

        release = pipeline.release_archive(archive_no)
        assert release["ok"] is False
        assert release["batch"]["status"] == BATCH_STATE_FAILED
        assert all(row["原始正文"] for row in _raw_rows(batch_no))

    def test_retry_archive_after_failed_verify_remigrates_damaged_row(self, pipeline):
        batch_no = _seed_batch(pipeline, "retry-verify-src")
        original = [row["原始正文"] for row in _raw_rows(batch_no)]
        result = pipeline.archive_batch(batch_no, auto_release=False)
        archive_no = result["batch"]["归档批次号"]
        result["batch"]["manifest"][0]["正文"] = "damaged-copy"
        assert pipeline.verify_archive(archive_no)["ok"] is False

        retry = pipeline.archive_batch(batch_no)
        assert retry["ok"] is True
        entry = retry["batch"]
        assert entry["归档批次号"] == archive_no
        item = entry["manifest"][0]
        assert item["正文"] == original[0]
        assert item["迁移次数"] == 2
        # 未损坏的行不重复迁移。
        assert entry["manifest"][1]["迁移次数"] == 1
        assert entry["核验状态"] == "通过"

    def test_cancel_replay_releases_lease_and_finishes_release(self, pipeline):
        batch_no = _seed_batch(pipeline, "cancel-src")
        started = pipeline.start_replay(batch_no, "rule-cancel")
        archived = pipeline.archive_batch(batch_no)
        assert archived["batch"]["status"] == "已核验"

        cancelled = pipeline.cancel_replay(started["replay"]["id"])
        assert cancelled["ok"] is True
        assert cancelled["归档进度"][batch_no]["归档状态"] == "已归档"
        assert all(row["原始正文"] == "" for row in _raw_rows(batch_no))

    def test_overlapping_replays_keep_lease_until_last_finishes(self, pipeline):
        batch_no = _seed_batch(pipeline, "multi-lease-src")
        first = pipeline.start_replay(batch_no, "rule-a")
        second = pipeline.start_replay(batch_no, "rule-b")
        archived = pipeline.archive_batch(batch_no)
        assert archived["batch"]["status"] == "已核验"

        first_done = pipeline.advance_replay(first["replay"]["id"])
        assert first_done["归档进度"][batch_no]["归档状态"] == "已核验"
        assert pipeline.replay_status(second["replay"]["id"])["重放占用"][batch_no] is True

        final = pipeline.advance_replay(second["replay"]["id"])
        assert final["归档进度"][batch_no]["归档状态"] == "已归档"


class TestRestore:
    """恢复后的正文读取和状态持久化。"""

    def test_restore_keeps_line_numbers_and_status(self, pipeline):
        batch_no = _seed_batch(pipeline, "restore-src")
        archive_no = pipeline.archive_batch(batch_no)["batch"]["归档批次号"]
        original = {
            row["原始行号"]: item["正文"]
            for row in _raw_rows(batch_no)
            for item in archive_ledger.find_batch(archive_no)["manifest"]
            if str(item["原始记录ID"]) == str(row["id"])
        }

        restored = pipeline.restore_archive(archive_no)
        assert restored["ok"] is True
        assert restored["batch"]["status"] == "已恢复"
        rows = _raw_rows(batch_no)
        assert [row["原始行号"] for row in rows] == ["1", "2", "3"]
        for row in rows:
            assert row["原始正文"] == original[row["原始行号"]]
            assert row["归档状态"] == ROW_RESTORED
        assert archive_ledger.progress(batch_no)["归档状态"] == "已恢复"
        assert archive_ledger.progress(batch_no)["已恢复行数"] == 3

    def test_restore_rejects_tampered_manifest_atomically(self, pipeline):
        batch_no = _seed_batch(pipeline, "restore-bad-src")
        result = pipeline.archive_batch(batch_no)
        archive_no = result["batch"]["归档批次号"]
        result["batch"]["manifest"][0]["正文"] = "changed"

        restored = pipeline.restore_archive(archive_no)
        assert restored["ok"] is False
        # 全部行保持已释放，没有写回任何半成品正文。
        assert all(row["原始正文"] == "" for row in _raw_rows(batch_no))
        assert all(row["归档状态"] == ROW_RELEASED for row in _raw_rows(batch_no))


class TestReplayBodyResolution:
    """重放按原批次号找到正文，归档和重放看到同一来源。"""

    def test_replay_reads_archive_after_release(self, pipeline):
        batch_no = _seed_batch(pipeline, "replay-arc-src")
        archive_no = pipeline.archive_batch(batch_no)["batch"]["归档批次号"]

        body_view = pipeline.replay_bodies(batch_no)
        assert body_view["ok"] is True
        assert {row["正文来源"] for row in body_view["行明细"]} == {SOURCE_ARCHIVE}
        assert body_view["归档进度"]["归档状态"] == "已归档"

        result = pipeline.replay_batch(batch_no, "rule-from-archive")
        assert result["ok"] is True
        assert result["replayed_count"] == 3
        assert result["正文来源统计"] == {SOURCE_ARCHIVE: 3}
        assert result["replay"]["status"] == "已完成"

    def test_replay_by_original_line_number(self, pipeline):
        batch_no = _seed_batch(pipeline, "line-src")
        archive_no = pipeline.archive_batch(batch_no)["batch"]["归档批次号"]
        item = archive_ledger.find_batch(archive_no)["manifest"][1]
        result = pipeline.replay_bodies(batch_no, line_no=str(item["原始行号"]))
        assert result["ok"] is True
        assert len(result["行明细"]) == 1
        assert result["行明细"][0]["正文"] == item["正文"]
        assert result["行明细"][0]["原始行号"] == item["原始行号"]

    def test_replay_after_restore_uses_hot_body(self, pipeline):
        batch_no = _seed_batch(pipeline, "replay-hot-src")
        archive_no = pipeline.archive_batch(batch_no)["batch"]["归档批次号"]
        pipeline.restore_archive(archive_no)

        result = pipeline.replay_batch(batch_no, "rule-from-hot")
        assert result["ok"] is True
        assert result["正文来源统计"] == {SOURCE_HOT: 3}
        assert pipeline.replay_bodies(batch_no)["归档进度"]["已恢复行数"] == 3

    def test_replay_reports_missing_body_as_partial_failure(self, pipeline):
        batch_no = _seed_batch(pipeline, "replay-miss-src")
        # 模拟正文既不在热存也不在任何归档清单里。
        for row in _raw_rows(batch_no):
            row["原始正文"] = ""
        result = pipeline.replay_batch(batch_no, "rule-missing")
        assert result["ok"] is False
        assert result["replay"]["status"] == "部分失败"
        assert result["missing_count"] == 3


class TestArchiveReplayConflict:
    """值班员刚发起重放时，归档正在回收热存。"""

    def test_release_deferred_while_replay_holds_bodies(self, pipeline):
        batch_no = _seed_batch(pipeline, "conflict-src")
        started = pipeline.start_replay(batch_no, "rule-conflict")
        assert started["ok"] is True
        replay_id = started["replay"]["id"]

        archived = pipeline.archive_batch(batch_no, policy_id=1)
        assert archived["ok"] is True
        entry = archived["batch"]
        assert entry["status"] == "已核验"
        assert entry["待释放行数"] == 3
        assert "重放占用" in entry["归档中断原因"]
        assert all(row["原始正文"] for row in _raw_rows(batch_no))

        status = pipeline.replay_status(replay_id)
        progress = status["归档进度"][batch_no]
        assert status["重放占用"][batch_no] is True
        assert progress["归档状态"] == "已核验"
        assert progress["归档进度"] == 70
        assert progress["正文来源"] == {SOURCE_HOT: 3, SOURCE_ARCHIVE: 0, "不可用": 0}
        assert progress["可重放"] is True

        advanced = pipeline.advance_replay(replay_id)
        assert advanced["ok"] is True
        assert advanced["replayed_count"] == 3
        # 重放结束自动续做被暂缓的释放。
        assert advanced["归档进度"][batch_no]["归档状态"] == "已归档"
        assert all(row["原始正文"] == "" for row in _raw_rows(batch_no))

    def test_both_tasks_observe_same_available_source(self, pipeline):
        batch_no = _seed_batch(pipeline, "observe-src")
        started = pipeline.start_replay(batch_no, "rule-observe")
        archived = pipeline.archive_batch(batch_no)
        archive_progress = archived["batch"]["status"]
        archive_source = {
            item["正文来源"] for item in archived["batch"]["manifest"]
        }
        replay_source = {
            row["正文来源"]
            for row in pipeline.replay_bodies(batch_no)["行明细"]
        }
        assert archive_progress == "已核验"
        assert archive_source == replay_source == {SOURCE_HOT}
        # 归档副本位置在两侧都可见，重放仍能按原批次号回落到归档存储。
        assert all(
            item["归档位置"].startswith(f"archive://{archived['batch']['归档批次号']}/line/")
            for item in archived["batch"]["manifest"]
        )
        pipeline.advance_replay(started["replay"]["id"])


class TestCombinedRun:
    """用一个仍可重放的批次同时试跑归档和原文读取。"""

    def test_archive_and_body_read_on_replayable_batch(self, pipeline):
        batch_no = _seed_batch(pipeline, "combined-src", count=4)
        original_bodies = {row["原始行号"]: row["原始正文"] for row in _raw_rows(batch_no)}

        archived = pipeline.archive_batch(batch_no, policy_id=1)
        archive_no = archived["batch"]["归档批次号"]
        assert archived["batch"]["status"] == "已归档"
        assert archived["batch"]["核验状态"] == "通过"

        body_view = pipeline.replay_bodies(batch_no)
        assert body_view["ok"] is True
        assert body_view["归档进度"]["可重放"] is True
        assert body_view["归档进度"]["归档批次号"] == archive_no
        # 归档状态、正文有效来源和重新解析出的行数分别校验。
        assert {row["正文来源"] for row in body_view["行明细"]} == {SOURCE_ARCHIVE}
        assert [row["原始行号"] for row in body_view["行明细"]] == ["1", "2", "3", "4"]
        for row in body_view["行明细"]:
            assert row["正文"] == original_bodies[row["原始行号"]]
            assert row["归档位置"].startswith(f"archive://{archive_no}/line/")

        replay = pipeline.replay_batch(batch_no, "rule-combined")
        assert replay["ok"] is True
        assert replay["replayed_count"] == 4
        assert replay["missing_count"] == 0
        assert replay["正文来源统计"] == {SOURCE_ARCHIVE: 4}
        entities = [
            entity for entity in store.rows("parsed_entity")
            if entity.get("来源批次") == batch_no
        ]
        assert len(entities) == 4
        assert {entity["source_of_body"] for entity in entities} == {SOURCE_ARCHIVE}

        # 原始日志列表关联归档位置。
        from app.services import SERVICES

        listed = SERVICES["raw_records"].list_entries(
            filters={"批次编号": batch_no}, page=1, size=10
        )[0]
        assert len(listed) == 4
        for row in listed:
            assert row["归档批次号"] == archive_no
            assert row["归档位置"]
            assert row["归档状态"] == ROW_RELEASED
            assert row["正文来源"] == SOURCE_ARCHIVE
