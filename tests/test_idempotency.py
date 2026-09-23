"""重复投递与并发重复投递：只能生效一次，响应可区分。"""

from __future__ import annotations

import concurrent.futures

import pytest


def _event(event_id: str, seq: int, source: str = "shop") -> dict:
    return {
        "event_id": event_id,
        "source": source,
        "seq": seq,
        "payload": {"sku": "A-1", "qty": seq},
    }


def test_duplicate_delivery_applied_once_with_distinct_response(client):
    ev = _event("evt-1", 1)
    r1 = client.post("/v1/sources/shop/events", json=ev)
    r2 = client.post("/v1/sources/shop/events", json=ev)
    r3 = client.post("/v1/sources/shop/events", json=ev)

    assert r1.status_code == 200
    assert r1.json()["status"] == "APPLIED"
    assert r1.json()["attempts"] == 1

    # 重复请求结论可区分
    assert r2.status_code == 200
    assert r2.json()["status"] == "DUPLICATE"
    assert r2.json()["duplicate_of"] == "evt-1"
    assert r3.json()["status"] == "DUPLICATE"

    # 目标库只有一条，未二次写入
    records = client.get("/v1/sources/shop/records").json()
    assert records["count"] == 1
    assert records["records"][0]["payload"] == {"sku": "A-1", "qty": 1}

    # 位点只按首次应用推进一次，仍为 1
    cp = client.get("/v1/sources/shop/checkpoint").json()
    assert cp["last_seq"] == 1


def test_concurrent_duplicate_applies_exactly_once(client):
    ev = _event("evt-concurrent", 7)
    n = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        futures = [
            pool.submit(client.post, "/v1/sources/shop/events", json=ev)
            for _ in range(n)
        ]
        responses = [f.result() for f in futures]

    statuses = sorted(r.json()["status"] for r in responses)
    applied = [s for s in statuses if s == "APPLIED"]
    duplicates = [s for s in statuses if s == "DUPLICATE"]

    # 恰好一个 APPLIED，其余全部 DUPLICATE
    assert len(applied) == 1, statuses
    assert len(duplicates) == n - 1, statuses
    # 全部 200，没有二次写入
    assert all(r.status_code == 200 for r in responses)

    records = client.get("/v1/sources/shop/records").json()
    assert records["count"] == 1
    events = client.get("/v1/sources/shop/events").json()
    assert events["count"] == 1
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 7


def test_duplicate_does_not_mutate_existing_result(client):
    ev1 = _event("evt-mut", 3)
    client.post("/v1/sources/shop/events", json=ev1)
    # 用不同 payload 重复投递同一 event_id
    tampered = dict(ev1, payload={"sku": "HACKED", "qty": 999})
    r = client.post("/v1/sources/shop/events", json=tampered)
    assert r.json()["status"] == "DUPLICATE"
    records = client.get("/v1/sources/shop/records").json()
    assert records["records"][0]["payload"] == {"sku": "A-1", "qty": 3}


def test_same_event_id_across_sources_is_independent(client):
    # 相同 event_id 在不同来源各自生效一次，不串用
    e1 = _event("shared-id", 1, source="shop")
    e2 = _event("shared-id", 1, source="crm")
    assert client.post("/v1/sources/shop/events", json=e1).json()["status"] == "APPLIED"
    assert client.post("/v1/sources/crm/events", json=e2).json()["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
    assert client.get("/v1/sources/crm/records").json()["count"] == 1
