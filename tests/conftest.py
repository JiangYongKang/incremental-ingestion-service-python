"""pytest 夹具：每个测试使用隔离的数据目录，并打印可追溯的判定日志。"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pytest

from app.retry import RetryPolicy
from app.service import IngestionService
from app.storage import LocalStorage

# 让测试日志打印事件标识、检查点与判定依据（配合 pytest -s）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s")


@pytest.fixture
def tmp_data_dir() -> Path:
    with tempfile.TemporaryDirectory(prefix="ingest-test-") as d:
        yield Path(d)


@pytest.fixture
def service(tmp_data_dir: Path) -> IngestionService:
    # 测试中默认不随机失败；需要瞬时失败时用 _fail_next 或 force_fail
    return IngestionService(
        LocalStorage(tmp_data_dir),
        RetryPolicy(max_attempts=3, backoff_seconds=0),
        transient_rate=0.0)


@pytest.fixture
def data_dir(tmp_data_dir: Path) -> Path:
    return tmp_data_dir


def make_event(seq: int, event_id: str | None = None, op: str = "upsert",
               key: str | None = None, value=None, **extra) -> dict:
    """构造合法事件的便捷函数。"""
    event_id = event_id or f"evt-{seq}"
    payload: dict = {"key": key or f"k{seq}"}
    if op == "upsert":
        payload["value"] = value if value is not None else f"v{seq}"
    payload.update(extra)
    return {"event_id": event_id, "seq": seq, "op": op, "payload": payload}


def log_case(title: str, **fields) -> None:
    """统一格式打印测试判定依据：事件标识、检查点、结论。"""
    detail = " ".join(f"{k}={v}" for k, v in fields.items())
    logging.getLogger("ingestion.test").info("【%s】%s", title, detail)
