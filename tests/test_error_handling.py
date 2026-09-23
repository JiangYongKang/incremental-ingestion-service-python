"""场景 4：畸形事件按可区分原因拒绝隔离；瞬时失败可重试并可终止；
单条坏事件不阻塞同来源后续事件和其他来源。"""
from __future__ import annotations

from conftest import log_case, make_event

from app.models import ErrorClass, Verdict


def test_malformed_events_are_classified_and_quarantined(service):
    cases = [
        ("字段缺失", {"event_id": "m1", "seq": 1, "op": "upsert"},
         ErrorClass.MISSING_FIELD),
        ("类型不匹配-seq", {"event_id": "m2", "seq": "1", "op": "upsert",
                          "payload": {"key": "k", "value": "v"}},
         ErrorClass.TYPE_MISMATCH),
        ("类型不匹配-payload", {"event_id": "m3", "seq": 1, "op": "upsert",
                               "payload": "not-an-object"},
         ErrorClass.TYPE_MISMATCH),
        ("结构非法-op", {"event_id": "m4", "seq": 1, "op": "drop",
                        "payload": {"key": "k"}},
         ErrorClass.INVALID_STRUCTURE),
        ("upsert缺value", {"event_id": "m5", "seq": 1, "op": "upsert",
                           "payload": {"key": "k"}},
         ErrorClass.MISSING_FIELD),
        ("顶层非对象", ["not", "a", "dict"], ErrorClass.INVALID_STRUCTURE),
    ]
    for title, raw, expected_class in cases:
        r = service.ingest("src-bad", raw)
        log_case(title, event_id=r.event_id, verdict=r.verdict.value,
                 error_class=r.error_class.value, checkpoint=r.checkpoint,
                 reason=r.reason)
        assert r.verdict is Verdict.REJECTED
        assert r.error_class is expected_class

    rejected = service.status("src-bad")["rejected"]
    assert len(rejected) == 6
    # 畸形事件绝不推进检查点、绝不落库
    assert service.status("src-bad")["checkpoint"] == 0
    assert service.target_snapshot("src-bad") == {}
    classes = sorted(x["error_class"] for x in rejected)
    log_case("隔离区结论", rejected_count=6, classes=classes)


def test_transient_failure_retries_then_succeeds(service):
    event = make_event(seq=1, event_id="t-1", _fail_next=True)
    r1 = service.ingest("src-retry", event)
    assert r1.verdict is Verdict.RETRYING and r1.attempts == 1
    log_case("首次失败登记重试", event_id="t-1", attempts=1,
             verdict=r1.verdict.value, reason=r1.reason)

    # 重启后续跑（新实例同目录）：去掉强制失败后成功，不重复落库
    from app.retry import RetryPolicy
    from app.service import IngestionService
    from app.storage import LocalStorage
    import tempfile
    # service 的目录通过 storage 拿不到，直接用 retry_pending 模拟续跑
    results = service.retry_pending("src-retry")
    assert len(results) == 1
    r2 = results[0]
    log_case("续跑成功", event_id="t-1", attempts=r2.attempts,
             verdict=r2.verdict.value, checkpoint=r2.checkpoint)
    assert r2.verdict is Verdict.ACCEPTED and r2.attempts == 2
    assert r2.checkpoint == 1
    assert service.target_snapshot("src-retry") == {
        "k1": {"key": "k1", "value": "v1"}}
    # 再次续跑无事件可跑
    assert service.retry_pending("src-retry") == []


def test_retry_exhaustion_dead_letters_and_does_not_block(service):
    event = make_event(seq=1, event_id="dl-1", _fail_next=True)
    r = service.ingest("src-dl", event)
    assert r.verdict is Verdict.RETRYING
    r2 = service.retry_pending("src-dl", force_fail=True)[0]
    assert r2.verdict is Verdict.RETRYING and r2.attempts == 2
    r3 = service.retry_pending("src-dl", force_fail=True)[0]
    log_case("重试耗尽终止", event_id="dl-1", attempts=r3.attempts,
             verdict=r3.verdict.value, checkpoint=r3.checkpoint,
             reason=r3.reason)
    assert r3.verdict is Verdict.DEAD_LETTER and r3.attempts == 3
    dead = service.status("src-dl")["dead_letters"]
    assert [d["event_id"] for d in dead] == ["dl-1"]

    # 后续事件不被毒消息阻塞
    r4 = service.ingest("src-dl", make_event(seq=2, event_id="dl-2"))
    log_case("毒消息不阻塞后续", event_id="dl-2",
             verdict=r4.verdict.value, checkpoint=r4.checkpoint)
    assert r4.verdict is Verdict.ACCEPTED and r4.checkpoint == 2

    # 其他来源完全不受影响
    r5 = service.ingest("src-other", make_event(seq=1, event_id="o-1"))
    assert r5.verdict is Verdict.ACCEPTED


def test_bad_event_in_one_source_does_not_block_other_source(service):
    service.ingest("src-x", ["bad"])
    service.ingest("src-x", {"event_id": "x", "seq": 1})
    assert service.status("src-x")["checkpoint"] == 0
    r = service.ingest("src-y", make_event(seq=1, event_id="y-1"))
    log_case("坏事件不跨来源", source="src-y",
             verdict=r.verdict.value, checkpoint=r.checkpoint)
    assert r.verdict is Verdict.ACCEPTED and r.checkpoint == 1
