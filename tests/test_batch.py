"""批量投递：部分坏事件/部分重复不影响其他事件，逐条结论可区分。"""

from __future__ import annotations


def _event(event_id: str, seq: int, source: str = "shop") -> dict:
    return {"event_id": event_id, "source": source, "seq": seq, "payload": {"v": seq}}


def test_batch_partial_bad_and_duplicate_do_not_affect_others(client):
    # 先让 evt-dup 生效，制造“批内重复”
    assert client.post("/v1/sources/shop/events", json=_event("evt-dup", 1)).json()[
        "status"
    ] == "APPLIED"

    batch = [
        _event("evt-ok-1", 2),                      # 正常：APPLIED
        _event("evt-dup", 1),                       # 历史重复：DUPLICATE
        {"event_id": "evt-bad", "source": "shop", "seq": 3},  # 缺 payload：QUARANTINED
        {"source": "shop", "seq": 4, "payload": {}},          # 缺 event_id：REJECTED
        "not-an-object",                            # 非对象：REJECTED
        _event("evt-ok-2", 5),                      # 正常：APPLIED（不被前面坏事件连累）
    ]
    r = client.post("/v1/sources/shop/events/batch", json={"events": batch})
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "shop"

    results = body["results"]
    assert [it["index"] for it in results] == list(range(len(batch)))
    statuses = [it["status"] for it in results]
    assert statuses == [
        "APPLIED",
        "DUPLICATE",
        "QUARANTINED",
        "REJECTED",
        "REJECTED",
        "APPLIED",
    ]
    # 每条结论的原因可区分
    assert results[1]["duplicate_of"] == "evt-dup"
    assert results[2]["reason"] == "MISSING_FIELD"
    assert results[3]["reason"] == "MISSING_EVENT_ID"
    assert results[4]["reason"] == "MALFORMED_STRUCTURE"

    summary = body["summary"]
    assert summary["total"] == 6
    assert summary["applied"] == 2
    assert summary["duplicate"] == 1
    assert summary["quarantined"] == 1
    assert summary["rejected"] == 2

    # 落库条数 = 去重后真正生效的事件数（evt-dup 之前已生效 + 本批 2 条）
    assert client.get("/v1/sources/shop/records").json()["count"] == 3
    # 坏事件也推进位点：最后一条正常事件 seq=5
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 5


def test_batch_duplicate_event_id_within_same_batch_applies_once(client):
    ev = _event("evt-same", 7)
    r = client.post(
        "/v1/sources/shop/events/batch",
        json=[ev, dict(ev), dict(ev, payload={"v": "tampered"})],
    )
    assert r.status_code == 200
    statuses = [it["status"] for it in r.json()["results"]]
    # 同一批里重复的事件标识，只有一条真正生效
    assert statuses == ["APPLIED", "DUPLICATE", "DUPLICATE"]
    records = client.get("/v1/sources/shop/records").json()
    assert records["count"] == 1
    assert records["records"][0]["payload"] == {"v": 7}


def test_batch_envelope_must_be_list_or_events_object(client):
    r = client.post("/v1/sources/shop/events/batch", json={"event_id": "x"})
    assert r.status_code == 400
    assert r.json()["reason"] == "MALFORMED_STRUCTURE"
    r2 = client.post(
        "/v1/sources/shop/events/batch",
        content=b"not-json",
        headers={"Content-Type": "application/json"},
    )
    assert r2.status_code == 400


def test_batch_cross_source_item_rejected_per_item(client):
    batch = [
        _event("ok-1", 1),
        {"event_id": "alien", "source": "crm", "seq": 2, "payload": {}},
        _event("ok-2", 3),
    ]
    r = client.post("/v1/sources/shop/events/batch", json=batch)
    assert r.status_code == 200
    results = r.json()["results"]
    assert results[0]["status"] == "APPLIED"
    assert results[1]["status"] == "REJECTED"
    assert results[1]["reason"] == "SOURCE_MISMATCH"
    assert results[2]["status"] == "APPLIED"
    # 跨来源条目两侧均不落库
    assert client.get("/v1/sources/shop/records").json()["count"] == 2
    assert client.get("/v1/sources/crm/records").json()["count"] == 0


def test_batch_with_transient_failure_item(client):
    client.post("/v1/admin/faults/shop/flaky?fail_times=1")
    batch = [_event("flaky", 1), _event("stable", 2)]
    r = client.post("/v1/sources/shop/events/batch", json=batch)
    assert r.status_code == 200
    results = r.json()["results"]
    # 暂时失败只影响自己：RETRYING；同批其他事件正常生效
    assert results[0]["status"] == "RETRYING"
    assert results[0]["attempts"] == 1
    assert results[1]["status"] == "APPLIED"
    # 故障已消耗，重投该事件即成功
    r2 = client.post("/v1/sources/shop/events", json=_event("flaky", 1))
    assert r2.json()["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 2


def test_batch_does_not_block_other_sources(client):
    # 大批次投递到 shop 后，crm 的位点/落库完全独立
    big_batch = [_event(f"bulk-{i}", i + 1) for i in range(200)]
    r = client.post("/v1/sources/shop/events/batch", json=big_batch)
    assert r.status_code == 200
    assert r.json()["summary"]["applied"] == 200

    assert client.post("/v1/sources/crm/events", json=_event("c1", 1, "crm")).json()[
        "status"
    ] == "APPLIED"
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 200
    assert client.get("/v1/sources/crm/checkpoint").json()["last_seq"] == 1
    assert client.get("/v1/sources/shop/records").json()["count"] == 200
    assert client.get("/v1/sources/crm/records").json()["count"] == 1
