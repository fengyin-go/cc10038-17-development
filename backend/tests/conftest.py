import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import INTERNAL_TABLES, store
from app.seed import SEED_ROWS


@pytest.fixture(autouse=True)
def reset_store():
    """每个测试前重置 store 为初始种子数据。"""
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    for name in INTERNAL_TABLES:
        store._tables[name] = []
    yield


@pytest.fixture
def pipeline():
    from app.services.pipeline import pipeline as p
    return p


@pytest.fixture
def sample_raw_logs():
    return [
        {"body": "2026-01-15T10:30:00|INFO|server started"},
        {"body": "2026-01-15T10:31:00|WARN|memory high"},
        {"body": "2026-01-15T10:32:00|ERROR|connection timeout"},
        {"body": "2026-01-15T10:33:00|INFO|request processed"},
        {"body": "2026-01-15T10:34:00|WARN|disk usage 85%"},
    ]