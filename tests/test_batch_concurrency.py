"""真实多进程（非线程）验证：重叠批次并发投递，每条事件仍只生效一次。"""

from __future__ import annotations

import multiprocessing as mp


def _event(event_id: str, seq: int, source: str) -> dict:
    return {"event_id": event_id, "source": source, "seq": seq, "payload": {"v": seq}}


def _worker_deliver(args: tuple[str, str, list[dict]]) -> dict:
    """子进程入口：独立构造服务实例，对共享数据目录投递一个批次。"""
    data_dir, source, events = args
    from app.service import IngestionService
    from app.store import LocalStore

    service = IngestionService(LocalStore(data_dir))
    response = service.ingest_batch(events, source)
    summary = response.summary.model_dump()
    summary["statuses"] = [r.status.value for r in response.results]
    return summary


def _run_pool(data_dir: str, tasks: list[tuple[str, str, list[dict]]]) -> list[dict]:
    # spawn：真正的独立进程，不用线程模拟
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=len(tasks)) as pool:
        return pool.map(_worker_deliver, tasks)


def test_overlapping_batches_across_processes_apply_exactly_once(tmp_path):
    data_dir = str(tmp_path / "data")
    source = "shop"
    n = 100
    # 4 个进程投递同一批完全相同的事件（100% 重叠）
    batch = [_event(f"evt-{i}", i + 1, source) for i in range(n)]
    tasks = [(data_dir, source, list(batch)) for _ in range(4)]

    summaries = _run_pool(data_dir, tasks)

    total_applied = sum(s["applied"] for s in summaries)
    total_duplicate = sum(s["duplicate"] for s in summaries)
    # 每条事件恰好生效一次：4 个进程合计只有 n 个 APPLIED，其余全是 DUPLICATE
    assert total_applied == n, summaries
    assert total_duplicate == 4 * n - n, summaries
    assert all(s["rejected"] == 0 and s["quarantined"] == 0 for s in summaries)

    # 落库条数与去重后事件数一致，位点推进到最大 seq
    from app.store import LocalStore

    store = LocalStore(data_dir)
    assert len(store.list_records(source)) == n
    assert store.get_checkpoint(source) == n
    assert len(store.list_events(source)) == n


def test_out_of_order_overlapping_batches_keep_checkpoint_monotonic(tmp_path):
    data_dir = str(tmp_path / "data")
    source = "shop"
    n = 120
    # 三个进程分别投递：正序后半、倒序、乱序全量，彼此重叠且乱序到达
    ascending = [_event(f"e-{i}", i + 1, source) for i in range(n)]
    tail_first = list(reversed(ascending[n // 2:])) + ascending[: n // 2]
    shuffled = [ascending[i] for i in sorted(range(n), key=lambda i: (i * 37) % n)]
    tasks = [
        (data_dir, source, tail_first),
        (data_dir, source, list(reversed(ascending))),
        (data_dir, source, shuffled),
    ]

    summaries = _run_pool(data_dir, tasks)

    assert sum(s["applied"] for s in summaries) == n
    from app.store import LocalStore

    store = LocalStore(data_dir)
    # 乱序批次不导致位点倒退：最终位点等于最大 seq
    assert store.get_checkpoint(source) == n
    # 已生效事件不被重写：落库条数 == 去重后事件数
    assert len(store.list_records(source)) == n


def test_big_batch_does_not_block_other_source(tmp_path):
    data_dir = str(tmp_path / "data")
    big = [_event(f"bulk-{i}", i + 1, "shop") for i in range(300)]
    small = [_event(f"c-{i}", i + 1, "crm") for i in range(20)]

    summaries = _run_pool(
        data_dir, [(data_dir, "shop", big), (data_dir, "crm", small)]
    )

    shop, crm = summaries
    # 两个来源各自全部生效，互不干扰
    assert shop["applied"] == 300 and shop["rejected"] == 0
    assert crm["applied"] == 20 and crm["rejected"] == 0

    from app.store import LocalStore

    store = LocalStore(data_dir)
    assert store.get_checkpoint("shop") == 300
    assert store.get_checkpoint("crm") == 20
    assert len(store.list_records("shop")) == 300
    assert len(store.list_records("crm")) == 20
