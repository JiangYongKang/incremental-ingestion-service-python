"""异常链路：可区分原因拒绝/隔离，坏事件不阻塞其他来源。"""

from __future__ import annotations


def test_missing_event_id_rejected_not_silently_dropped(client):
    r = client.post(
        "/v1/sources/shop/events",
        json={"source": "shop", "seq": 1, "payload": {}},
    )
    assert r.status_code == 400
    assert r.json()["reason"] == "MISSING_EVENT_ID"


def test_body_not_object_rejected(client):
    r = client.post("/v1/sources/shop/events", json=[1, 2, 3])
    assert r.status_code == 400
    assert r.json()["reason"] == "MALFORMED_STRUCTURE"


def test_invalid_json_rejected(client):
    r = client.post(
        "/v1/sources/shop/events",
        content=b"{not-json",
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 400
    assert r.json()["reason"] == "MALFORMED_STRUCTURE"


def test_missing_field_is_quarantined_with_reason(client):
    r = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "bad-1", "source": "shop", "seq": 1},
    )
    assert r.status_code == 422
    body = r.json()
    assert body["status"] == "QUARANTINED"
    assert body["reason"] == "MISSING_FIELD"
    q = client.get("/v1/quarantine?source=shop").json()
    assert q["count"] == 1 and q["items"][0]["reason"] == "MISSING_FIELD"


def test_type_mismatch_is_quarantined_with_reason(client):
    r = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "bad-2", "source": "shop", "seq": "1", "payload": {}},
    )
    assert r.status_code == 422
    assert r.json()["reason"] == "TYPE_MISMATCH"


def test_payload_wrong_type_is_quarantined(client):
    r = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "bad-3", "source": "shop", "seq": 2, "payload": "oops"},
    )
    assert r.status_code == 422
    assert r.json()["reason"] == "TYPE_MISMATCH"


def test_quarantined_bad_event_does_not_block_same_source(client):
    # seq=1 坏事件被隔离
    r = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "bad", "source": "shop", "seq": 1},
    )
    assert r.json()["status"] == "QUARANTINED"
    # seq=2 好事件照常生效；坏事件已推进位点，不阻塞
    r2 = client.post(
        "/v1/sources/shop/events",
        json={"event_id": "good", "source": "shop", "seq": 2, "payload": {"ok": True}},
    )
    assert r2.json()["status"] == "APPLIED"
    assert client.get("/v1/sources/shop/records").json()["count"] == 1
    assert client.get("/v1/sources/shop/checkpoint").json()["last_seq"] == 2


def test_bad_event_in_one_source_does_not_block_other(client):
    r = client.post(
        "/v1/sources/a/events",
        json={"event_id": "x", "source": "a", "seq": 1},
    )
    assert r.json()["status"] == "QUARANTINED"
    r2 = client.post(
        "/v1/sources/b/events",
        json={"event_id": "y", "source": "b", "seq": 1, "payload": {"v": 1}},
    )
    assert r2.json()["status"] == "APPLIED"
    assert client.get("/v1/quarantine?source=a").json()["count"] == 1
    assert client.get("/v1/quarantine?source=b").json()["count"] == 0


def test_quarantined_event_redelivered_keeps_verdict(client):
    bad = {"event_id": "bad-dup", "source": "shop", "seq": 1}
    assert client.post("/v1/sources/shop/events", json=bad).json()["status"] == "QUARANTINED"
    r = client.post("/v1/sources/shop/events", json=bad)
    assert r.json()["status"] == "DUPLICATE"
    # 隔离区仍只有一条，不重复隔离
    assert client.get("/v1/quarantine?source=shop").json()["count"] == 1
