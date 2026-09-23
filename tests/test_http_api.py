"""通过 FastAPI 接口验证全部判定结论与 HTTP 状态码可区分。"""
from __future__ import annotations

import pytest

from conftest import make_event

from app.main import app
from app.retry import RetryPolicy
from app.service import IngestionService
from app.storage import LocalStorage

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    svc = IngestionService(LocalStorage(tmp_path / "data"),
                           RetryPolicy(max_attempts=3, backoff_seconds=0),
                           transient_rate=0.0)
    monkeypatch.setattr("app.main.service", svc)
    with TestClient(app) as c:
        yield c


def test_api_verdicts_are_distinguishable(client):
    ev = make_event(seq=1, event_id="api-1")
    r1 = client.post("/sources/web/events", json=ev)
    assert r1.status_code == 200
    assert r1.json()["verdict"] == "accepted"

    r2 = client.post("/sources/web/events", json=ev)
    assert r2.status_code == 200
    body = r2.json()
    assert body["verdict"] == "duplicate"
    assert body["checkpoint"] == 1

    # 畸形事件：400 + 可区分 error_class
    r3 = client.post("/sources/web/events", json={"event_id": "x", "seq": 2})
    assert r3.status_code == 400
    assert r3.json()["error_class"] == "missing_field"

    # 检查点回退：409
    r4 = client.post("/sources/web/checkpoint", json={"seq": 0})
    assert r4.status_code == 409
    assert r4.json()["error_class"] == "checkpoint_regression"

    # 超前事件：202 buffered
    r5 = client.post("/sources/web/events",
                     json=make_event(seq=3, event_id="api-3"))
    assert r5.status_code == 202
    assert r5.json()["verdict"] == "buffered"

    # 状态与落库接口
    status = client.get("/sources/web/status").json()
    assert status["checkpoint"] == 1 and status["buffered"] == ["3"]
    rows = client.get("/sources/web/target").json()["rows"]
    assert rows == {"k1": {"key": "k1", "value": "v1"}}


def test_api_retry_and_dead_letter_flow(client):
    fail = make_event(seq=1, event_id="api-dl", _fail_next=True)
    r1 = client.post("/sources/web/events", json=fail)
    assert r1.status_code == 202 and r1.json()["verdict"] == "retrying"
    r2 = client.post("/sources/web/retry", json={"force_fail": True})
    assert r2.json()["resumed"][0]["attempts"] == 2
    r3 = client.post("/sources/web/retry", json={"force_fail": True})
    assert r3.json()["resumed"][0]["verdict"] == "dead_letter"
    # 后续事件不阻塞
    r4 = client.post("/sources/web/events",
                     json=make_event(seq=2, event_id="api-next"))
    assert r4.status_code == 200 and r4.json()["checkpoint"] == 2
    # 重放死信
    r5 = client.post("/sources/web/dead-letters/1/replay")
    assert r5.status_code == 200 and r5.json()["verdict"] == "accepted"
