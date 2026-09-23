"""场景 3：检查点只能单调推进；回退、跳跃、跨来源串用必须被明确拒绝。"""
from __future__ import annotations

import pytest

from conftest import log_case, make_event

from app.errors import CheckpointError
from app.models import ErrorClass, Verdict


def test_checkpoint_regression_rejected(service):
    service.ingest("src-cp", make_event(seq=1, event_id="c-1"))
    service.ingest("src-cp", make_event(seq=2, event_id="c-2"))

    with pytest.raises(CheckpointError) as exc:
        service.advance_checkpoint("src-cp", 1)
    log_case("检查点回退拒绝", source="src-cp",
             requested_seq=1, checkpoint=2,
             error_class=exc.value.error_class.value, reason=exc.value.message)
    assert exc.value.error_class is ErrorClass.CHECKPOINT_REGRESSION

    # 状态未改变
    status = service.status("src-cp")
    assert status["checkpoint"] == 2
    # 旧位点重复投递：duplicate（已生效），不回退
    r = service.ingest("src-cp", make_event(seq=1, event_id="c-1"))
    assert r.verdict is Verdict.DUPLICATE
    assert service.status("src-cp")["checkpoint"] == 2
    log_case("回退后状态保持", checkpoint=2, verdict=r.verdict.value)


def test_checkpoint_cannot_skip_unprocessed(service):
    service.ingest("src-skip", make_event(seq=1, event_id="s-1"))
    with pytest.raises(CheckpointError) as exc:
        service.advance_checkpoint("src-skip", 99)
    log_case("跳跃推进拒绝", source="src-skip", requested_seq=99,
             checkpoint=1, reason=exc.value.message)
    assert service.status("src-skip")["checkpoint"] == 1


def test_checkpoints_are_isolated_per_source(service):
    """跨来源串用：来源 A 的位点不能影响来源 B。"""
    service.ingest("src-a", make_event(seq=1, event_id="a-1"))
    service.ingest("src-a", make_event(seq=2, event_id="a-2"))
    service.ingest("src-b", make_event(seq=1, event_id="b-1"))

    assert service.status("src-a")["checkpoint"] == 2
    assert service.status("src-b")["checkpoint"] == 1

    # 在 B 上用 A 的位点序列重复投递——B 从未见过，但 seq=1 已生效：
    r = service.ingest("src-b", make_event(seq=1, event_id="b-1"))
    log_case("跨来源重复投递", source="src-b", event_id="b-1",
             verdict=r.verdict.value,
             rule="各来源独立账本：B 上判 duplicate")
    assert r.verdict is Verdict.DUPLICATE

    # B 投递 seq=2 正常推进，目标数据与 A 完全隔离
    r = service.ingest("src-b", make_event(seq=2, event_id="b-2"))
    assert r.verdict is Verdict.ACCEPTED and r.checkpoint == 2
    assert service.target_snapshot("src-a") == {
        "k1": {"key": "k1", "value": "v1"},
        "k2": {"key": "k2", "value": "v2"}}
    assert service.target_snapshot("src-b") == {
        "k1": {"key": "k1", "value": "v1"},
        "k2": {"key": "k2", "value": "v2"}}


def test_out_of_order_buffers_and_checkpoint_advances_without_gap(service):
    """超前事件缓冲，缺口补齐时检查点随连续事件一起推进。"""
    r3 = service.ingest("src-gap", make_event(seq=3, event_id="g-3"))
    assert r3.verdict is Verdict.BUFFERED
    r2 = service.ingest("src-gap", make_event(seq=2, event_id="g-2"))
    assert r2.verdict is Verdict.BUFFERED
    log_case("乱序事件缓冲", source="src-gap", checkpoint=0,
             buffered=[2, 3])

    r1 = service.ingest("src-gap", make_event(seq=1, event_id="g-1"))
    log_case("缺口补齐级联生效", event_id="g-1",
             verdict=r1.verdict.value, checkpoint=r1.checkpoint,
             rule="1 生效后按序级联应用 2、3")
    assert r1.verdict is Verdict.ACCEPTED
    assert r1.checkpoint == 3
    status = service.status("src-gap")
    assert status["buffered"] == []
    assert set(status["applied"]) == {"1", "2", "3"}
