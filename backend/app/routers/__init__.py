"""业务路由汇总：固定前缀的流水线路由先注册，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import resource, pipeline_router

# pipeline_router 的固定路径（/api/pipeline/...）必须先于 /api/{module} 注册，
# 否则 /api/pipeline/snapshot 会被通用资源路由当成 entry_id 解析。
ROUTERS = [pipeline_router, resource]
