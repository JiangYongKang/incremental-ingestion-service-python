"""场景 1：重复投递——同一事件多次投递只生效一次，响应可区分，不二次写入。"""
from __future__ import annotations

from conftest import log_case, make_event

from app.models import Verdict


def test_same_event_delivered_many_times_applies_once(service):
    event = make_event(seq=1, event_id="evt-1")

    first = service.ingest("src-orders", event)
    log_case("首次投递", event_id=first.event_id, seq=first.seq,
             checkpoint=first.checkpoint, verdict=first.verdict.value,
             reason=first.reason)
    assert first.verdict is Verdict.ACCEPTED
    assert first.checkpoint == 1

    verdicts = []
    for i in range(5):
        r = service.ingest("src-orders", dict(event))
        verdicts.append(r.verdict)
        log_case(f"重复投递#{i+1}", event_id=r.event_id, seq=r.seq,
                 checkpoint=r.checkpoint, verdict=r.verdict.value,
                 http_meaning="幂等返回既有结果", reason=r.reason)

    assert all(v is Verdict.DUPLICATE for v in verdicts)
    target = service.target_snapshot("src-orders")
    assert target == {"k1": {"key": "k1", "value": "v1"}}
    status = service.status("src-orders")
    assert status["checkpoint"] == 1
    assert status["applied"] == ["1"]
    assert status["buffered"] == [] and status["retrying"] == []
    log_case("一致性结论", target_rows=len(target),
             applied=status["applied"], checkpoint=status["checkpoint"])


def test_duplicate_after_restart_still_idempotent(service, tmp_data_dir):
    """服务重启（新实例、同目录）后重复投递仍判 duplicate。"""
    from app.retry import RetryPolicy
    from app.service import IngestionService
    from app.storage import LocalStorage

    event = make_event(seq=1, event_id="evt-1")
    assert service.ingest("src-r", event).verdict is Verdict.ACCEPTED

    restarted = IngestionService(
        LocalStorage(tmp_data_dir), RetryPolicy(3, 0), transient_rate=0.0)
    r = restarted.ingest("src-r", event)
    log_case("重启后重复投递", event_id=r.event_id, checkpoint=r.checkpoint,
             verdict=r.verdict.value, reason=r.reason)
    assert r.verdict is Verdict.DUPLICATE
    assert r.checkpoint == 1
    assert len(restarted.target_snapshot("src-r")) == 1
