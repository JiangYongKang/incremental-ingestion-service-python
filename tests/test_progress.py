"""来源级进度对账：位点与各状态条数同账本/落库结果一致。"""

from __future__ import annotations

import concurrent.futures


def _event(event_id: str, seq: int, source: str = "shop") -> dict:
    return {"event_id": event_id, "source": source, "seq": seq, "payload": {"v": seq}}


def test_progress_counts_match_ledger_and_records(client):
    # 2 条生效、1 条隔离、1 条重试中、1 条死信
    client.post("/v1/sources/shop/events", json=_event("ok-1", 1))
    client.post("/v1/sources/shop/events", json=_event("ok-2", 2))
    client.post(
        "/v1/sources/shop/events",
        json={"event_id": "bad-1", "source": "shop", "seq": 3},
    )
    client.post("/v1/admin/faults/shop/retry-1?fail_times=1")
    client.post("/v1/sources/shop/events", json=_event("retry-1", 4))
    # dead-1 连续失败 5 次，耗尽重试进入死信
    client.post("/v1/admin/faults/shop/dead-1?fail_times=5")
    for _ in range(5):
        client.post("/v1/sources/shop/events", json=_event("dead-1", 5))

    p = client.get("/v1/sources/shop/progress").json()
    assert p["source"] == "shop"
    assert p["last_seq"] == 3          # RETRYING/死信事件不推进位点
    assert p["applied"] == 2
    assert p["records"] == 2
    assert p["quarantined"] == 1
    assert p["dead_letter"] == 1
    assert p["pending"] == 1

    # 与既有接口的账本/落库视图完全对得上
    assert p["records"] == client.get("/v1/sources/shop/records").json()["count"]
    events = client.get("/v1/sources/shop/events").json()["events"]
    assert p["applied"] == sum(1 for e in events if e["status"] == "APPLIED")
    assert p["pending"] == sum(1 for e in events if e["status"] == "RETRYING")
    assert p["quarantined"] == client.get("/v1/quarantine?source=shop").json()["count"]
    assert p["dead_letter"] == client.get("/v1/dead-letters?source=shop").json()["count"]
    assert p["last_seq"] == client.get("/v1/sources/shop/checkpoint").json()["last_seq"]


def test_progress_is_per_source(client):
    client.post("/v1/sources/shop/events", json=_event("a", 5))
    client.post("/v1/sources/crm/events", json=_event("b", 2, "crm"))
    client.post("/v1/sources/crm/events", json={"event_id": "bad", "source": "crm", "seq": 3})

    shop = client.get("/v1/sources/shop/progress").json()
    crm = client.get("/v1/sources/crm/progress").json()
    assert (shop["last_seq"], shop["applied"], shop["quarantined"]) == (5, 1, 0)
    assert (crm["last_seq"], crm["applied"], crm["quarantined"]) == (3, 1, 1)


def test_progress_snapshot_consistent_under_concurrent_writes(client):
    # 并发写入期间反复读取快照：任一时刻都必须满足内部恒等式，不自相矛盾
    events = [_event(f"c-{i}", i + 1) for i in range(60)]

    def deliver(chunk):
        client.post("/v1/sources/shop/events/batch", json=chunk)

    def observe():
        snapshots = []
        for _ in range(15):
            snapshots.append(client.get("/v1/sources/shop/progress").json())
        return snapshots

    mid = len(events) // 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        f1 = pool.submit(deliver, events[:mid])
        f2 = pool.submit(deliver, events[mid:])
        f3 = pool.submit(observe)
        f1.result(); f2.result()
        snapshots = f3.result()

    for p in snapshots:
        # 已生效条数必须等于目标库落库条数（同一临界区快照）
        assert p["applied"] == p["records"], p
        # 位点不回退、不越过已投递的最大 seq
        assert 0 <= p["last_seq"] <= len(events), p

    final = client.get("/v1/sources/shop/progress").json()
    assert final["applied"] == final["records"] == len(events)
    assert final["last_seq"] == len(events)
    assert final["pending"] == 0
