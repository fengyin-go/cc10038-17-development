"""归档策略领域服务：策略执行现在会产出可恢复的归档批次并回写可读清单。"""
from __future__ import annotations

from typing import Any

from app.services.archive_service import BATCH_STATE_FAILED, BATCH_STATE_ORDER, archive_ledger
from app.services.base import BaseModuleService


class DataArchiveService(BaseModuleService):
    MODULE = "data_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略名称", "归档周期", "保留时长", "归档格式", "目标存储"]
    REQUIRED_FIELDS = ["策略名称", "归档周期", "保留时长"]
    STATUS_ORDER = ["待执行", "已执行", "已暂停"]
    ACTION_RULES = {"执行归档": "已执行", "暂停归档": "已暂停"}
    NEGATIVE_ACTIONS = []

    def run_action(
        self, entry_id: int, action: str, remark: str | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """执行归档必须带源批次号：只改策略状态的“执行”不再被接受。"""
        if action == "执行归档" and not str(remark or "").strip():
            return None, "执行归档需要指定源批次号（动作备注或归档批次面板）"
        entry, message = super().run_action(entry_id, action, remark)
        if entry is not None and action == "执行归档" and remark:
            from app.services.pipeline import pipeline

            result = pipeline.archive_by_policy(entry_id, str(remark).strip())
            message = result.get("message", message)
            entry = result.get("batch") or entry
            if not result.get("ok"):
                return None, message
        return entry, message

    def exec_archive(self, entry_id: int, batch_no: str) -> dict[str, Any]:
        policy = self.get_entry(entry_id)
        if policy is None:
            return {"ok": False, "message": f"归档策略 {entry_id} 不存在"}
        from app.services.pipeline import pipeline

        result = pipeline.archive_by_policy(entry_id, batch_no)
        result["policy"] = policy
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        batches = archive_ledger.batches()
        snapshot["archive_batches"] = {
            "count": len(batches),
            "by_status": {
                status: sum(1 for entry in batches if entry.get("status") == status)
                for status in [*BATCH_STATE_ORDER, BATCH_STATE_FAILED]
            },
            "verified_rows": sum(int(entry.get("已核验行数", 0)) for entry in batches),
            "released_rows": sum(int(entry.get("已释放行数", 0)) for entry in batches),
            "restored_rows": sum(int(entry.get("已恢复行数", 0)) for entry in batches),
        }
        return snapshot
