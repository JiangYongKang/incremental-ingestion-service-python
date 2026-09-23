"""上游暂时失败：可解释的有界重试、终止死信、显式重放。"""

from __future__ import annotations


def _event(event_id: str, seq: int) -> dict:
    return {"event_id": event_id, "source": "shop", "seq": seq, "payload": {"v": seq}}


def test_transient_failure_then_success_on_redelivery(client):
    client.post("/v1/admin/faults/shop/f1?fail_times=2")
    ev = _event("f1", 1)
    r1 = client.post("/v1/sources/shop/events", json=ev)
    assert r1.status_code == 202
    b1 = r1.json()
    assert b1["status"] == "RETRYING" and b1["attempts"] == 1
    assert "1/5" in b1["detail"]
    assert client.get("/v1/sources/shop/records").json()["count"] == 0

    r2 = client.post("/v1/sources/shop/events", json=ev)
    assert r2.json()["attempts"] == 2 and r2.json()["status"] == "RETRYING"
    r3 = client.post("/v1/sources/shop/events", json=ev)
    assert r3.status_code == 200 and r3.json()["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
    # 之后再投递是重复，不二次写入
    assert client.post("/v1/sources/shop/events", json=ev).json()["status"] == "DUPLICATE"


def test_retry_exhaustion_moves_to_dead_letter(client):
    client.post("/v1/admin/faults/shop/f2?fail_times=99")
    ev = _event("f2", 1)
    last = None
    for i in range(1, 6):
        last = client.post("/v1/sources/shop/events", json=ev)
    assert last.status_code == 503
    body = last.json()
    assert body["status"] == "DEAD_LETTER"
    assert body["reason"] == "RETRY_EXHAUSTED"
    assert body["attempts"] == 5

    dl = client.get("/v1/dead-letters?source=shop").json()
    assert dl["count"] == 1 and dl["items"][0]["event_id"] == "f2"
    # 死信未落库
    assert client.get("/v1/sources/shop/records").json()["count"] == 0
    # 死信重复投递维持终止态，不会偷偷重写
    again = client.post("/v1/sources/shop/events", json=ev)
    assert again.json()["status"] == "DEAD_LETTER"


def test_dead_letter_replay_after_recovery(client):
    client.post("/v1/admin/faults/shop/f3?fail_times=99")
    ev = _event("f3", 1)
    for _ in range(5):
        client.post("/v1/sources/shop/events", json=ev)
    assert client.get("/v1/dead-letters").json()["count"] == 1

    # 故障解除后显式重放
    client.delete("/v1/admin/faults")
    r = client.post("/v1/dead-letters/shop/f3/replay")
    assert r.status_code == 200 and r.json()["status"] == "APPLIED"
    assert client.get("/v1/dead-letters").json()["count"] == 0
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 1


def test_resume_retries_until_terminal(client):
    # 故障次数较多，resume 内部循环直到成功（故障被消耗完）
    client.post("/v1/admin/faults/shop/f4?fail_times=3")
    ev = _event("f4", 1)
    assert client.post("/v1/sources/shop/events", json=ev).json()["status"] == "RETRYING"
    out = client.post("/v1/sources/shop/resume").json()
    # resume 内循环续跑：中间失败尝试也逐条记录，最终到达 APPLIED
    assert out["resumed"] >= 1
    assert out["results"][-1]["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
