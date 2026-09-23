"""位点单调推进：回退拒绝、跨来源隔离、重启/续跑不丢不重。"""

from __future__ import annotations


def _event(event_id: str, seq: int, source: str = "shop") -> dict:
    return {"event_id": event_id, "source": source, "seq": seq, "payload": {"v": seq}}


def test_checkpoint_monotonic_and_rollback_rejected(client):
    assert client.post("/v1/checkpoints", json={"source": "shop", "seq": 10}).json()[
        "last_seq"
    ] == 10
    # 持平不算推进，也不报错
    assert client.post("/v1/checkpoints", json={"source": "shop", "seq": 10}).json()[
        "last_seq"
    ] == 10
    # 回退必须被明确拒绝
    r = client.post("/v1/checkpoints", json={"source": "shop", "seq": 9})
    assert r.status_code == 409
    assert r.json()["reason"] == "CHECKPOINT_ROLLBACK"
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 10


def test_checkpoint_per_source_isolated(client):
    client.post("/v1/sources/shop/events", json=_event("a", 5))
    client.post("/v1/sources/crm/events", json=_event("b", 2, source="crm"))
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 5
    assert client.get("/v1/sources/crm/checkpoint").json()["last_seq"] == 2


def test_cross_source_redelivery_rejected(client):
    # 事件体内来源与 URL 来源不一致：明确拒绝，两侧均无落库
    r = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "x", "source": "crm", "seq": 1, "payload": {}},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "SOURCE_MISMATCH"
    assert client.get("/v1/sources/shop/records").json()["count"] == 0
    assert client.get("/v1/sources/crm/records").json()["count"] == 0


def test_retry_event_resume_does_not_lose_or_duplicate(client):
    # 故障 2 次：首次失败进入 RETRYING；之后 resume 续跑成功
    client.post("/v1/admin/faults/shop/r1?fail_times=2")
    ev = _event("r1", 4)
    r1 = client.post("/v1/sources/shop/events", json=ev)
    assert r1.status_code == 202 and r1.json()["status"] == "RETRYING"
    assert client.get("/v1/sources/shop/records").json()["count"] == 0

    # 模拟“重启后重复消费”：再次投递同一未完成事件即续跑
    r2 = client.post("/v1/sources/shop/events", json=ev)
    assert r2.json()["status"] == "RETRYING" and r2.json()["attempts"] == 2
    resumed = client.post("/v1/sources/shop/resume").json()
    assert resumed["resumed"] >= 1
    assert resumed["results"][-1]["status"] == "APPLIED"

    # 位点推进到 4，目标库恰好一条
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 4
    assert client.get("/v1/sources/shop/records").json()["count"] == 1

    # 再次 resume 为空（没有未完成事件）
    assert client.post("/v1/sources/shop/resume").json()["resumed"] == 0
