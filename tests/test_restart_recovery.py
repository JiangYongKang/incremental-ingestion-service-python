"""场景 5：服务重启后不丢事件、不重复落库，未完成事件可续跑。"""
from __future__ import annotations

from conftest import log_case, make_event

from app.models import Verdict
from app.retry import RetryPolicy
from app.service import IngestionService
from app.storage import LocalStorage


def _new_service(data_dir) -> IngestionService:
    return IngestionService(LocalStorage(data_dir),
                            RetryPolicy(max_attempts=3, backoff_seconds=0),
                            transient_rate=0.0)


def test_restart_resumes_retrying_event_without_duplicate_write(tmp_path):
    data_dir = tmp_path / "data"
    svc = _new_service(data_dir)
    r = svc.ingest("src-restart",
                   make_event(seq=1, event_id="r-1", _fail_next=True))
    assert r.verdict is Verdict.RETRYING
    log_case("崩溃前事件未完成", event_id="r-1", attempts=r.attempts,
             verdict=r.verdict.value, checkpoint=0,
             target_rows=len(svc.target_snapshot("src-restart")))

    # 模拟重启：全新服务实例从同一目录恢复
    svc2 = _new_service(data_dir)
    status = svc2.status("src-restart")
    log_case("重启后状态恢复", checkpoint=status["checkpoint"],
             retrying=[x["seq"] for x in status["retrying"]],
             target_rows=len(svc2.target_snapshot("src-restart")))
    assert status["checkpoint"] == 0
    assert [x["seq"] for x in status["retrying"]] == [1]

    results = svc2.retry_pending("src-restart")
    assert [r.verdict for r in results] == [Verdict.ACCEPTED]
    assert results[0].attempts == 2  # 尝试次数跨重启保留
    log_case("重启后续跑成功", event_id="r-1",
             verdict=results[0].verdict.value,
             attempts=results[0].attempts,
             checkpoint=results[0].checkpoint)

    # 不重复落库：恰好一行
    assert svc2.target_snapshot("src-restart") == {
        "k1": {"key": "k1", "value": "v1"}}


def test_restart_preserves_buffered_gap(tmp_path):
    data_dir = tmp_path / "data"
    svc = _new_service(data_dir)
    svc.ingest("src-gap", make_event(seq=3, event_id="p-3"))
    svc.ingest("src-gap", make_event(seq=2, event_id="p-2"))

    svc2 = _new_service(data_dir)
    status = svc2.status("src-gap")
    log_case("重启后缓冲恢复", checkpoint=status["checkpoint"],
             buffered=status["buffered"])
    assert sorted(status["buffered"], key=int) == ["2", "3"]

    r = svc2.ingest("src-gap", make_event(seq=1, event_id="p-1"))
    assert r.verdict is Verdict.ACCEPTED and r.checkpoint == 3
    assert len(svc2.target_snapshot("src-gap")) == 3
    log_case("重启后补齐缺口", checkpoint=r.checkpoint,
             target_rows=3, rule="2、3 从持久化缓冲级联生效")


def test_restart_then_repeated_consumption_does_not_rewrite(tmp_path):
    data_dir = tmp_path / "data"
    svc = _new_service(data_dir)
    svc.ingest("src-idem", make_event(seq=1, event_id="i-1"))
    svc.ingest("src-idem", make_event(seq=2, event_id="i-2"))

    svc2 = _new_service(data_dir)
    for seq, eid in [(1, "i-1"), (2, "i-2"), (1, "i-1")]:
        r = svc2.ingest("src-idem", make_event(seq=seq, event_id=eid))
        assert r.verdict is Verdict.DUPLICATE
    log_case("重启后重复消费", verdicts=["duplicate"] * 3,
             target_rows=len(svc2.target_snapshot("src-idem")),
             checkpoint=svc2.status("src-idem")["checkpoint"])
    assert len(svc2.target_snapshot("src-idem")) == 2
