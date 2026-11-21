"""原始日志领域服务：归档批次接入后，列表直接带出归档位置与正文来源。"""
from __future__ import annotations

from typing import Any

from app.services.archive_service import ROW_HOT, SOURCE_ARCHIVE, SOURCE_HOT, SOURCE_MISSING, archive_ledger
from app.services.base import BaseModuleService
from app.store import store


class RawRecordsService(BaseModuleService):
    MODULE = "raw_records"
    ENTITY = "日志记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "原始行号", "采集时间戳", "原始正文", "解析状态"]
    REQUIRED_FIELDS = ["批次编号", "原始行号", "采集时间戳"]
    STATUS_ORDER = ["待解析", "已解析", "解析失败"]
    ACTION_RULES = {"触发解析": "已解析", "标记忽略": "解析失败"}
    NEGATIVE_ACTIONS = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size
        )
        return [self._with_archive_pointer(row) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = super().get_entry(entry_id)
        return self._with_archive_pointer(row) if row is not None else None

    @staticmethod
    def _with_archive_pointer(row: dict[str, Any]) -> dict[str, Any]:
        """原始行只暴露归档位置和状态，不复制正文，避免列表与归档清单各说各话。"""
        item = dict(row)
        archive_no = item.get("归档批次号")
        if archive_no:
            request = archive_ledger.find_batch(str(archive_no))
            manifest_item = None
            if request is not None:
                for entry in request.get("manifest", []):
                    if str(entry.get("原始记录ID")) == str(item.get("id")):
                        manifest_item = entry
                        break
            item["归档位置"] = item.get("归档位置") or (manifest_item or {}).get("归档位置", "")
            if manifest_item is not None and not item.get("正文摘要"):
                item["正文摘要"] = manifest_item.get("正文摘要", "")
        item.setdefault("归档状态", ROW_HOT)
        # 与重放侧同一口径：正文来源报当前首选可用位置，归档副本另见“归档位置”。
        item["正文来源"] = SOURCE_HOT if item.get("原始正文") else (
            SOURCE_ARCHIVE if item.get("归档位置") else SOURCE_MISSING
        )
        return item

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        snapshot["archive_linked_rows"] = sum(1 for row in rows if row.get("归档批次号"))
        snapshot["released_body_rows"] = sum(
            1 for row in rows if row.get("归档状态") == "已释放"
        )
        return snapshot
