"""场景 2：并发重复投递——同一事件被多线程/多进程同时投递，只生效一次。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor

from conftest import log_case, make_event

from app.models import Verdict


def test_concurrent_threads_same_event_applies_once(service):
    event = make_event(seq=1, event_id="evt-concurrent-1")
    n = 20

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(
            lambda _: service.ingest("src-concurrent", dict(event)),
            range(n)))

    verdicts = [r.verdict for r in results]
    accepted = [v for v in verdicts if v is Verdict.ACCEPTED]
    duplicates = [v for v in verdicts if v is Verdict.DUPLICATE]
    log_case("线程并发重复投递", event_id="evt-concurrent-1",
             total=n, accepted=len(accepted), duplicate=len(duplicates),
             rule="恰好 1 次 accepted，其余 duplicate",
             checkpoint=results[0].checkpoint)

    assert len(accepted) == 1, f"只允许一次 accepted，实际 {len(accepted)}"
    assert len(duplicates) == n - 1
    target = service.target_snapshot("src-concurrent")
    assert target == {"k1": {"key": "k1", "value": "v1"}}
    assert service.status("src-concurrent")["checkpoint"] == 1


def test_concurrent_processes_same_event_applies_once(tmp_path):
    """多进程同时投递同一事件：文件锁保证只生效一次。"""
    worker = textwrap.dedent(f"""
        import sys, os, json
        sys.path.insert(0, {os.getcwd()!r})
        from app.retry import RetryPolicy
        from app.service import IngestionService
        from app.storage import LocalStorage

        data_dir = sys.argv[1]
        worker_id = sys.argv[2]
        svc = IngestionService(LocalStorage(data_dir),
                               RetryPolicy(3, 0), transient_rate=0.0)
        event = {{"event_id": "evt-proc-1", "seq": 1, "op": "upsert",
                 "payload": {{"key": "k1", "value": "v1"}}}}
        r = svc.ingest("src-proc", event)
        print(json.dumps({{"worker": worker_id, "verdict": r.verdict.value}}))
    """)
    data_dir = tmp_path / "data"
    procs = [
        subprocess.run(
            [sys.executable, "-c", worker, str(data_dir), str(i)],
            capture_output=True, text=True, timeout=60,
            cwd=os.getcwd())
        for i in range(8)]
    outputs = []
    for i, p in enumerate(procs):
        assert p.returncode == 0, f"worker {i} 失败: {p.stderr[-500:]}"
        outputs.append(json.loads(p.stdout.strip()))
    verdicts = sorted(o["verdict"] for o in outputs)
    log_case("进程并发重复投递", event_id="evt-proc-1",
             total=len(outputs), verdicts=verdicts,
             rule="恰好 1 次 accepted，其余 duplicate")
    assert verdicts.count("accepted") == 1
    assert verdicts.count("duplicate") == len(outputs) - 1

    from app.storage import LocalStorage
    rows = LocalStorage(data_dir).snapshot_target("src-proc")
    assert rows == {"k1": {"key": "k1", "value": "v1"}}
