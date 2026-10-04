"""批量投递：逐条判定、部分坏事件/部分重复不连累其他事件、整批不整体失败。"""

from __future__ import annotations


BATCH_PATH = "/v1/sources/shop/events/batch"


def _ev(event_id: str, seq: int, **over) -> dict:
    ev = {"event_id": event_id, "source": "shop", "seq": seq, "payload": {"v": seq}}
    ev.update(over)
    return ev


def _by_id(resp: dict) -> dict[str, dict]:
    return {item["event_id"]: item for item in resp["results"] if item["event_id"]}


def test_batch_mixed_good_bad_duplicate_each_judged_independently(client):
    body = {"events": [
        _ev("b1", 1),                                   # 0 APPLIED
        {"event_id": "b2", "source": "shop", "seq": 2},  # 1 QUARANTINED 缺 payload
        _ev("b1", 1),                                   # 2 批内重复 -> DUPLICATE
        {"source": "shop", "seq": 4, "payload": {}},    # 3 REJECTED 缺 event_id
        "not-an-object",                                # 4 REJECTED 非对象
        _ev("b3", 5),                                   # 5 APPLIED（不被前面连累）
        _ev("b4", 6, source="crm"),                     # 6 REJECTED 跨来源
    ]}
    r = client.post(BATCH_PATH, json=body)

    # 信封合法 => 整批恒 200，绝不整体失败
    assert r.status_code == 200
    data = r.json()
    results = data["results"]
    assert [item["index"] for item in results] == list(range(7))

    assert results[0]["status"] == "APPLIED"
    assert results[1]["status"] == "QUARANTINED"
    assert results[1]["reason"] == "MISSING_FIELD"
    assert results[2]["status"] == "DUPLICATE"
    assert results[2]["duplicate_of"] == "b1"
    assert results[3]["status"] == "REJECTED"
    assert results[3]["reason"] == "MISSING_EVENT_ID"
    assert results[4]["status"] == "REJECTED"
    assert results[4]["reason"] == "MALFORMED_STRUCTURE"
    assert results[5]["status"] == "APPLIED"
    assert results[6]["status"] == "REJECTED"
    assert results[6]["reason"] == "SOURCE_MISMATCH"

    summary = data["summary"]
    assert summary == {
        "received": 7,
        "applied": 2,
        "duplicate": 1,
        "quarantined": 1,
        "retrying": 0,
        "dead_letter": 0,
        "rejected": 3,
    }

    # 好事件照常落库，去重后生效条数 = 2（b1、b3），坏事件不连累
    records = client.get("/v1/sources/shop/records").json()
    assert records["count"] == 2
    assert {r["event_id"] for r in records["records"]} == {"b1", "b3"}
    # 位点按最大 seq 单调推进到 5（跨来源的 6 不算）
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 5
    # 隔离区仅 1 条坏事件
    assert client.get("/v1/quarantine?source=shop").json()["count"] == 1
    # 跨来源事件两侧都不落库
    assert client.get("/v1/sources/crm/records").json()["count"] == 0


def test_batch_duplicate_event_id_applies_only_once(client):
    # 同一批里同一个 event_id 出现 3 次，最终只有一条真正生效
    body = {"events": [_ev("dup", 3), _ev("dup", 3), _ev("dup", 3)]}
    r = client.post(BATCH_PATH, json=body).json()
    statuses = [i["status"] for i in r["results"]]
    assert statuses[0] == "APPLIED"
    assert statuses[1:] == ["DUPLICATE", "DUPLICATE"]
    assert r["summary"]["applied"] == 1
    assert r["summary"]["duplicate"] == 2
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
    assert client.get("/v1/sources/shop/events").json()["count"] == 1


def test_batch_overlapping_with_prior_single_delivery(client):
    # 先单条投递生效，再用包含该事件的批量投递：旧事件判 DUPLICATE，新事件照常生效
    client.post("/v1/sources/shop/events", json=_ev("old", 1))
    body = {"events": [_ev("old", 1), _ev("new", 2)]}
    r = client.post(BATCH_PATH, json=body).json()
    by = _by_id(r)
    assert by["old"]["status"] == "DUPLICATE"
    assert by["old"]["duplicate_of"] == "old"
    assert by["new"]["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 2


def test_batch_retrying_item_does_not_block_others(client):
    # 给其中一条注入永久故障：该条 RETRYING，其余事件仍全部 APPLIED
    client.post("/v1/admin/faults/shop/flaky?fail_times=99")
    body = {"events": [
        _ev("ok1", 1),
        _ev("flaky", 2),
        _ev("ok2", 3),
    ]}
    r = client.post(BATCH_PATH, json=body).json()
    by = _by_id(r)
    assert by["ok1"]["status"] == "APPLIED"
    assert by["flaky"]["status"] == "RETRYING"
    assert by["flaky"]["attempts"] == 1
    assert by["ok2"]["status"] == "APPLIED"
    assert r["summary"]["retrying"] == 1
    assert r["summary"]["applied"] == 2
    # 两条好事件落库，失败事件不落库
    assert client.get("/v1/sources/shop/records").json()["count"] == 2
    # resume 后续跑：故障仍在会继续重试计数，但不影响其它事件；清故障后可成功
    client.delete("/v1/admin/faults")
    resumed = client.post("/v1/sources/shop/resume").json()
    assert resumed["results"][-1]["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 3


def test_batch_dead_letter_item_is_isolated_in_result(client):
    client.post("/v1/admin/faults/shop/dl?fail_times=99")
    ev = _ev("dl", 1)
    # 先用单条把它打进死信
    for _ in range(5):
        client.post("/v1/sources/shop/events", json=ev)
    # 再随一批新事件一起投递：死信条维持终止态，新事件不受影响
    body = {"events": [_ev("dl", 1), _ev("fresh", 2)]}
    r = client.post(BATCH_PATH, json=body).json()
    by = _by_id(r)
    assert by["dl"]["status"] == "DEAD_LETTER"
    assert by["dl"]["reason"] == "RETRY_EXHAUSTED"
    assert by["fresh"]["status"] == "APPLIED"
    assert r["summary"]["dead_letter"] == 1
    assert client.get("/v1/dead-letters?source=shop").json()["count"] == 1


def test_batch_out_of_order_does_not_rollback_checkpoint(client):
    # 乱序到达：seq 大的先来，小的后来；位点不倒退，小 seq 事件首次仍生效但位点不回退
    client.post(BATCH_PATH, json={"events": [_ev("hi", 10)]})
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 10
    client.post(BATCH_PATH, json={"events": [_ev("lo", 3)]})
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 10
    # 小 seq 事件确实生效了，且没有重写大 seq 事件
    records = client.get("/v1/sources/shop/records").json()
    assert records["count"] == 2
    by = {r["event_id"]: r for r in records["records"]}
    assert by["hi"]["seq"] == 10 and by["lo"]["seq"] == 3


def test_batch_malformed_envelope_rejected_not_per_item(client):
    # events 缺失 / 非数组：这是请求信封错误，整体 400
    r1 = client.post(BATCH_PATH, json={"nope": []})
    assert r1.status_code == 400 and r1.json()["reason"] == "BATCH_MALFORMED"
    r2 = client.post(BATCH_PATH, json={"events": {"a": 1}})
    assert r2.status_code == 400 and r2.json()["reason"] == "BATCH_MALFORMED"
    r3 = client.post(BATCH_PATH, content=b"{bad",
                     headers={"content-type": "application/json"})
    assert r3.status_code == 400 and r3.json()["reason"] == "BATCH_MALFORMED"


def test_empty_batch_is_ok_with_zero_results(client):
    r = client.post(BATCH_PATH, json={"events": []})
    assert r.status_code == 200
    body = r.json()
    assert body["results"] == []
    assert body["summary"]["received"] == 0


def test_batch_does_not_block_other_sources(client):
    # 大批 shop 事件处理期间，crm 来源可以独立投递成功（不同来源各自独立）
    shop_events = [_ev(f"s{i}", i) for i in range(1, 51)]
    r = client.post(BATCH_PATH, json={"events": shop_events})
    assert r.status_code == 200 and r.json()["summary"]["applied"] == 50
    other = client.post(
        "/v1/sources/crm/events/batch",
        json={"events": [
            {"event_id": "c1", "source": "crm", "seq": 1, "payload": {}}
        ]},
    )
    assert other.status_code == 200
    assert other.json()["results"][0]["status"] == "APPLIED"
    assert client.get("/v1/sources/crm/checkpoint").json()["last_seq"] == 1
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 50
