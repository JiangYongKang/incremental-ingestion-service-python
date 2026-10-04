"""真实多进程并发投递重叠批次：每条事件跨进程只能生效一次。

刻意使用 multiprocessing（独立 OS 进程，各有自己的内存锁/故障表），
而不是线程：跨进程去重只能依赖文件锁 + 条件写盘，这正是本服务多进程部署的形态。
"""

from __future__ import annotations

import multiprocessing as mp
import os
import tempfile

import pytest

from app.models import EventStatus
from app.service import IngestionService
from app.store import LocalStore


def _worker_ingest_batch(data_dir: str, source: str, events: list[dict]) -> list[str]:
    """子进程入口：独立构建 service，投递一整批，返回每条判定状态（可 pickle）。"""
    service = IngestionService(LocalStore(data_dir))
    resp = service.ingest_batch({"events": events}, source)
    return [item.status.value for item in resp.results]


def _worker_ingest_single(data_dir: str, source: str, events: list[dict]) -> list[str]:
    """子进程入口：逐条走单条 ingest 路径。"""
    service = IngestionService(LocalStore(data_dir))
    return [service.ingest(ev, source).status.value for ev in events]


def _make_events(source: str, n: int) -> list[dict]:
    return [
        {"event_id": f"{source}-evt-{i:03d}", "source": source, "seq": i + 1,
         "payload": {"i": i + 1}}
        for i in range(n)
    ]


@pytest.fixture
def data_dir() -> str:
    return tempfile.mkdtemp(prefix="ingest-multiproc-")


def test_overlapping_batches_across_processes_apply_once(data_dir):
    n_events = 30
    n_processes = 6
    source = "shop"
    events = _make_events(source, n_events)

    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=n_processes) as pool:
        # 每个进程都投递“同一批”事件 => 高度重叠的并发批次
        async_results = [
            pool.apply_async(_worker_ingest_batch, (data_dir, source, events))
            for _ in range(n_processes)
        ]
        all_statuses: list[str] = []
        for ar in async_results:
            all_statuses.extend(ar.get(timeout=60))

    # 每个事件恰有一个 APPLIED，其余全部 DUPLICATE（可区分，且没有整批失败/拒绝）
    assert all_statuses.count(EventStatus.APPLIED.value) == n_events
    assert all_statuses.count(EventStatus.DUPLICATE.value) == n_events * (
        n_processes - 1
    )
    assert all_statuses.count(EventStatus.REJECTED.value) == 0
    assert all_statuses.count(EventStatus.QUARANTINED.value) == 0

    # 用全新进程视角核对磁盘真相：落库条数 == 去重后事件数，账本条数一致
    verifier = IngestionService(LocalStore(data_dir))
    progress = verifier.source_progress(source)
    assert progress.applied_count == n_events
    assert progress.ledger_total == n_events
    assert len(verifier.store.list_records(source)) == n_events
    # 位点单调推进到最大 seq，不回退
    assert verifier.store.get_checkpoint(source) == n_events
    assert progress.last_seq == n_events

    # 每个 event_id 在目标库里恰好一条且内容正确（未被重复写/覆盖）
    records = {r["event_id"]: r for r in verifier.store.list_records(source)}
    assert set(records) == {f"{source}-evt-{i:03d}" for i in range(n_events)}
    for i in range(n_events):
        assert records[f"{source}-evt-{i:03d}"]["payload"] == {"i": i + 1}


def test_multiprocess_batches_do_not_cross_sources(data_dir):
    # 多个进程同时对不同来源投递：来源之间位点/账本互不串用
    ctx = mp.get_context("spawn")
    sources = {"shop": 20, "crm": 15, "biz": 10}
    jobs = []
    with ctx.Pool(processes=6) as pool:
        for src, n in sources.items():
            evs = _make_events(src, n)
            for _ in range(3):  # 每来源 3 个进程重复投递
                jobs.append(
                    (src, pool.apply_async(_worker_ingest_batch, (data_dir, src, evs)))
                )
        statuses = {src: [] for src in sources}
        for src, ar in jobs:
            statuses[src].extend(ar.get(timeout=60))

    verifier = IngestionService(LocalStore(data_dir))
    for src, n in sources.items():
        assert statuses[src].count(EventStatus.APPLIED.value) == n
        assert statuses[src].count(EventStatus.DUPLICATE.value) == n * 2
        progress = verifier.source_progress(src)
        assert progress.applied_count == n
        assert progress.last_seq == n
        # event_id 虽按来源命名，但不同来源目录独立
        assert len(verifier.store.list_records(src)) == n


def test_concurrent_single_and_batch_paths_agree(data_dir):
    # 混合路径：一部分走单条 ingest，一部分走批量，全部跨进程并发，仍然只生效一次
    n_events = 24
    source = "shop"
    events = _make_events(source, n_events)

    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=4) as pool:
        r1 = pool.apply_async(_worker_ingest_batch, (data_dir, source, events))
        r2 = pool.apply_async(_worker_ingest_batch, (data_dir, source, events))
        r3 = pool.apply_async(_worker_ingest_single, (data_dir, source, events))
        got = r1.get(timeout=60) + r2.get(timeout=60) + r3.get(timeout=60)

    assert got.count(EventStatus.APPLIED.value) == n_events
    assert got.count(EventStatus.DUPLICATE.value) == n_events * 2
    verifier = IngestionService(LocalStore(data_dir))
    assert len(verifier.store.list_records(source)) == n_events
    assert verifier.store.get_checkpoint(source) == n_events
