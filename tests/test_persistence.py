"""服务重启（重建 service/app）后状态保持：不丢事件、不重复落库。"""

from __future__ import annotations

import tempfile

from fastapi.testclient import TestClient

from app.api import create_app


def _event(event_id: str, seq: int, source: str = "shop") -> dict:
    return {"event_id": event_id, "source": source, "seq": seq, "payload": {"v": seq}}


def test_state_survives_restart(tmp_path):
    data_dir = tempfile.mkdtemp(prefix="ingest-restart-")

    c1 = TestClient(create_app(data_dir))
    assert c1.post("/v1/sources/shop/events", json=_event("e1", 1)).json()[
        "status"
    ] == "APPLIED"
    # 一个未完成事件
    c1.post("/v1/admin/faults/shop/e2?fail_times=99")
    assert c1.post("/v1/sources/shop/events", json=_event("e2", 2)).json()[
        "status"
    ] == "RETRYING"
    c1.post("/v1/checkpoints", json={"source": "shop", "seq": 8})

    # “重启”：用同一数据目录重新创建应用
    c2 = TestClient(create_app(data_dir))
    assert c2.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 8
    assert c2.get("/v1/sources/shop/records").json()["count"] == 1

    # 已完成事件重复投递仍是 DUPLICATE，不重复落库
    assert c2.post("/v1/sources/shop/events", json=_event("e1", 1)).json()[
        "status"
    ] == "DUPLICATE"
    assert c2.get("/v1/sources/shop/records").json()["count"] == 1

    # 未完成事件续跑：清掉故障后 resume，最终成功且只落一条
    c2.delete("/v1/admin/faults")
    out = c2.post("/v1/sources/shop/resume").json()
    assert out["results"][-1]["status"] == "APPLIED"
    assert c2.get("/v1/sources/shop/records").json()["count"] == 2
    assert c2.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 8


def test_rebuild_rejects_checkpoint_rollback(tmp_path):
    data_dir = tempfile.mkdtemp(prefix="ingest-cp-")
    c1 = TestClient(create_app(data_dir))
    c1.post("/v1/checkpoints", json={"source": "shop", "seq": 5})
    c2 = TestClient(create_app(data_dir))
    r = c2.post("/v1/checkpoints", json={"source": "shop", "seq": 1})
    assert r.status_code == 409 and r.json()["reason"] == "CHECKPOINT_ROLLBACK"
