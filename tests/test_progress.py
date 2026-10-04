"""来源级进度对账：数字与账本/落库对得上，快照自洽。"""

from __future__ import annotations


BATCH = "/v1/sources/shop/events/batch"


def _ev(event_id: str, seq: int) -> dict:
    return {"event_id": event_id, "source": "shop", "seq": seq, "payload": {"v": seq}}


def test_progress_counts_match_ledger_and_records(client):
    # 2 条生效 + 1 条隔离 + 1 条死信 + 1 条未完成
    client.post(BATCH, json={"events": [
        _ev("a1", 1),
        _ev("a2", 2),
        {"event_id": "bad", "source": "shop", "seq": 3},          # 隔离
    ]})
    client.post("/v1/admin/faults/shop/wait?fail_times=99")
    client.post("/v1/sources/shop/events", json=_ev("wait", 4))  # RETRYING
    client.post("/v1/admin/faults/shop/gone?fail_times=99")
    for _ in range(5):
        client.post("/v1/sources/shop/events", json=_ev("gone", 5))  # DEAD_LETTER

    p = client.get("/v1/sources/shop/progress").json()
    assert p["source"] == "shop"
    # 位点只由 APPLIED / QUARANTINED 推进（既有规则）：max(seq2, seq3)=3；
    # RETRYING(seq4) 与 DEAD_LETTER(seq5) 不推进位点，因此停在 3 且绝不回退
    assert p["last_seq"] == 3
    assert p["applied_count"] == 2
    assert p["quarantined_count"] == 1
    assert p["dead_letter_count"] == 1
    assert p["pending_count"] == 1
    # 账本总数 = 生效 + 隔离 + 死信 + 未完成
    assert p["ledger_total"] == 2 + 1 + 1 + 1

    # 与真实落库/隔离区/死信队列逐一对账
    assert p["applied_count"] == client.get("/v1/sources/shop/records").json()["count"]
    assert p["quarantined_count"] == client.get("/v1/quarantine?source=shop").json()["count"]
    assert p["dead_letter_count"] == client.get("/v1/dead-letters?source=shop").json()["count"]
    retrying = [
        e for e in client.get("/v1/sources/shop/events").json()["events"]
        if e["status"] == "RETRYING"
    ]
    assert p["pending_count"] == len(retrying) == 1
    assert "checked_at" in p and p["checked_at"]


def test_progress_duplicate_count_tracks_repeat_requests(client):
    client.post("/v1/sources/shop/events", json=_ev("d1", 1))
    for _ in range(3):
        client.post("/v1/sources/shop/events", json=_ev("d1", 1))
    # 批量里再重复 2 次
    client.post(BATCH, json={"events": [_ev("d1", 1), _ev("d1", 1)]})
    p = client.get("/v1/sources/shop/progress").json()
    # 生效仍只有 1 条，重复请求被单独计数 5 次
    assert p["applied_count"] == 1
    assert p["duplicate_count"] == 5
    assert client.get("/v1/sources/shop/records").json()["count"] == 1


def test_progress_per_source_isolated(client):
    client.post(BATCH, json={"events": [_ev("s1", 1), _ev("s2", 2)]})
    client.post(
        "/v1/sources/crm/events/batch",
        json={"events": [
            {"event_id": "c1", "source": "crm", "seq": 1, "payload": {}}
        ]},
    )
    shop = client.get("/v1/sources/shop/progress").json()
    crm = client.get("/v1/sources/crm/progress").json()
    assert shop["applied_count"] == 2 and shop["last_seq"] == 2
    assert crm["applied_count"] == 1 and crm["last_seq"] == 1
    # 互不串账
    assert shop["ledger_total"] == 2 and crm["ledger_total"] == 1


def test_progress_unknown_source_is_zero(client):
    p = client.get("/v1/sources/brand-new/progress").json()
    assert p["last_seq"] == 0
    assert p["applied_count"] == 0
    assert p["quarantined_count"] == 0
    assert p["dead_letter_count"] == 0
    assert p["pending_count"] == 0
    assert p["ledger_total"] == 0


def test_progress_resume_clears_pending_and_lands_record(client):
    client.post("/v1/admin/faults/shop/p1?fail_times=2")
    client.post("/v1/sources/shop/events", json=_ev("p1", 1))
    before = client.get("/v1/sources/shop/progress").json()
    assert before["pending_count"] == 1 and before["applied_count"] == 0
    client.post("/v1/sources/shop/resume")
    after = client.get("/v1/sources/shop/progress").json()
    assert after["pending_count"] == 0
    assert after["applied_count"] == 1
    assert after["applied_count"] == client.get("/v1/sources/shop/records").json()["count"]


def test_progress_snapshot_always_self_consistent_under_concurrent_writes(client):
    """并发写入的同时反复读进度：任何快照都必须内部自洽。

    自洽条件：applied_count == 落库记录数；quarantined/dead_letter 与各自队列一致；
    ledger_total == applied + quarantined + dead_letter + pending。
    """
    import threading

    from app.store import LocalStore

    # 直接用底层 store：source_snapshot 在同一把排他锁内读取，
    # 因而“快照内的落库记录数”与“账本 APPLIED 数”即使在并发写入时也必须相等。
    snap_store = LocalStore(client.headers["x-test-data-dir"])
    stop = threading.Event()
    violations: list[str] = []

    def writer() -> None:
        for i in range(1, 121):
            client.post(
                "/v1/sources/shop/events/batch",
                json={"events": [
                    {"event_id": f"w{i}", "source": "shop", "seq": i, "payload": {"i": i}},
                ]},
            )

    def reader() -> None:
        prev_applied = -1
        prev_seq = -1
        while not stop.is_set():
            # 单次请求内、服务端同一把锁读到的快照：各计数分解必须彼此对得上
            p = client.get("/v1/sources/shop/progress").json()
            # 同一把锁内的原始快照：生效账本条数与实际落库记录数必须完全相等
            snap = snap_store.source_snapshot("shop")
            ledger_applied = sum(
                1 for d in snap["events"].values() if d["status"] == "APPLIED"
            )
            if ledger_applied != len(snap["records"]):
                violations.append(
                    f"same-snapshot applied {ledger_applied} != records {len(snap['records'])}"
                )
            if p["ledger_total"] != (
                p["applied_count"]
                + p["quarantined_count"]
                + p["dead_letter_count"]
                + p["pending_count"]
            ):
                violations.append("ledger_total decomposition mismatch")
            # applied_count 由同一快照的落库记录数得出，不得超过账本总量
            if p["applied_count"] > p["ledger_total"]:
                violations.append("applied exceeds ledger_total")
            # 连续快照必须单调，不得回退
            if p["applied_count"] < prev_applied:
                violations.append(
                    f"applied went backwards {prev_applied} -> {p['applied_count']}"
                )
            if p["last_seq"] < prev_seq:
                violations.append(f"checkpoint went backwards {prev_seq} -> {p['last_seq']}")
            prev_applied = p["applied_count"]
            prev_seq = p["last_seq"]

    t = threading.Thread(target=writer)
    t.start()
    rt = threading.Thread(target=reader)
    rt.start()
    t.join()
    stop.set()
    rt.join()

    assert violations == []
    final = client.get("/v1/sources/shop/progress").json()
    assert final["applied_count"] == 120
    assert final["ledger_total"] == 120
    assert final["last_seq"] == 120
