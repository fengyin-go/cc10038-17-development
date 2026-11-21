"""ASGI 级接口测试：归档批次与重放协作的 HTTP 契约。"""
from __future__ import annotations

import asyncio
import json

import pytest

from app.main import app
from app.store import store


def _call(method: str, path: str, body: dict | None = None, query: str = "") -> tuple[int, str]:
    payload = json.dumps(body).encode() if body is not None else b""

    async def run() -> tuple[int, str]:
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
            "root_path": "",
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
            ],
            "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 8013),
        }
        sent = False

        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": payload, "more_body": False}
            return {"type": "http.disconnect"}

        messages: list[dict] = []

        async def send(message):
            messages.append(message)

        await app(scope, receive, send)
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        buf = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
        return status, buf.decode()

    return asyncio.run(run())


@pytest.fixture
def batch_with_body(pipeline):
    logs = [{"body": f"2026-01-15T10:3{i}:00|INFO|http-{i}"} for i in range(3)]
    pipeline.register_source("http-src", "http", "default")
    pipeline.validate_source("http-src")
    batch_no = pipeline.start_ingest_batch("http-src", "2026-01-15", logs)["batch"]["批次编号"]
    pipeline.parse_batch(batch_no)
    pipeline.validate_entities(batch_no)
    return batch_no


def test_pipeline_route_not_shadowed_by_resource_router():
    status, body = _call("GET", "/api/pipeline/snapshot")
    assert status == 200
    assert "modules" in json.loads(body)


def test_archive_http_flow(batch_with_body):
    status, body = _call(
        "POST", "/api/pipeline/archive/batch",
        {"batch_no": batch_with_body, "policy_id": 1},
    )
    assert status == 200
    result = json.loads(body)
    assert result["ok"] is True
    archive_no = result["batch"]["归档批次号"]
    assert result["batch"]["status"] == "已归档"

    status, body = _call("GET", f"/api/pipeline/archive/batch/{archive_no}/manifest", query="fmt=md")
    assert status == 200
    assert "可读清单" in body

    status, body = _call("GET", f"/api/pipeline/archive/progress/{batch_with_body}")
    assert status == 200
    assert json.loads(body)["归档状态"] == "已归档"

    status, body = _call("GET", f"/api/pipeline/body/{batch_with_body}", query="line_no=2")
    assert status == 200
    line = json.loads(body)["行明细"][0]
    assert line["原始行号"] == "2"
    assert line["正文来源"] == "归档存储"

    status, body = _call("GET", "/api/pipeline/archive/batches", query=f"batch_no={batch_with_body}")
    assert status == 200
    assert json.loads(body)["total"] == 1


def test_replay_http_flow_and_conflict(batch_with_body):
    status, body = _call(
        "POST", "/api/pipeline/replay/start",
        {"history_batch_range": batch_with_body, "new_parser_rule": "http-rule-v2"},
    )
    assert status == 200
    replay_id = json.loads(body)["replay"]["id"]

    status, body = _call(
        "POST", "/api/pipeline/archive/batch",
        {"batch_no": batch_with_body, "policy_id": 2},
    )
    assert status == 200
    archived = json.loads(body)
    assert archived["batch"]["status"] == "已核验"
    assert archived["release"]["blocked_by_replay"] is True

    status, body = _call("GET", f"/api/pipeline/replay/{replay_id}/status")
    assert status == 200
    progress = json.loads(body)["归档进度"][batch_with_body]
    assert progress["重放占用中"] is True
    assert progress["归档进度"] == 70

    status, body = _call("POST", f"/api/pipeline/replay/{replay_id}/advance")
    assert status == 200
    advanced = json.loads(body)
    assert advanced["ok"] is True
    assert advanced["replayed_count"] == 3
    assert advanced["正文来源统计"] == {"热存": 3}
    assert advanced["归档进度"][batch_with_body]["归档状态"] == "已归档"

    status, body = _call("POST", f"/api/pipeline/replay/{replay_id}/advance")
    assert status == 200
    assert json.loads(body)["ok"] is True
    # 重复推进不会重复生成明细。
    parsed = [e for e in store.rows("parsed_entity") if e.get("来源批次") == batch_with_body]
    assert len(parsed) == 3
