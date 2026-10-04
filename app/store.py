"""本地文件存储：模拟目标库落库、幂等记录、位点与隔离区/死信队列。

存储布局（data_dir 下）：

    <source>/checkpoint.json   {"last_seq": int}
    <source>/events.json       {event_id: 事件账本文档}   —— 幂等账本
    <source>/records.json      {event_id: 落库记录}       —— 模拟目标库
    <source>/_source.lock      来源级进程间文件锁
    quarantine.json            [隔离事件...]
    dead_letters.json          [死信事件...]
    _global.lock               全局文件（隔离区/死信）的进程间锁

并发策略：进程内 threading.RLock + 进程间 fcntl.flock 排他锁。
来源级文件（位点/账本/落库）各用各的来源锁，单个来源的大批次处理
不会卡住其他来源；隔离区/死信等全局文件用全局锁。需要同时持两类锁时
固定顺序：先来源锁、后全局锁，避免死锁。
任何“读-改-写”都在锁内以最新磁盘内容完成，写入走临时文件 + os.replace 原子替换。
"""

from __future__ import annotations

import fcntl
import json
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

_SAFE_SOURCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")


class LocalStore:
    def __init__(self, data_dir: str | Path = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._lock_file = self.data_dir / "_global.lock"
        self._lock_file.touch(exist_ok=True)
        # 来源级进程内锁（与来源级 flock 配对）；线程本地计数实现可重入
        self._source_locks: dict[str, threading.RLock] = {}
        self._held = threading.local()

    # ------------------------------------------------------------------ #
    # 内部原语
    # ------------------------------------------------------------------ #
    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        """进程内 + 跨进程排他临界区（全局文件：隔离区/死信）。"""
        with self._lock:
            with open(self._lock_file, "r+") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def _source_rlock(self, source: str) -> threading.RLock:
        with self._lock:
            lock = self._source_locks.get(source)
            if lock is None:
                lock = threading.RLock()
                self._source_locks[source] = lock
            return lock

    @contextmanager
    def _exclusive_source(self, source: str) -> Iterator[None]:
        """来源级排他临界区：不同来源互不阻塞；同线程可重入。

        可重入是必要的：外层事务（如 ingest 的“查账本-应用-记账”）持有
        来源锁时，内层的 get_event/put_record 等原语直接复用外层临界区，
        否则同一线程对同一锁文件二次 flock 会自我死锁。
        """
        held = getattr(self._held, "sources", None)
        if held is None:
            held = self._held.sources = {}
        depth = held.get(source, 0)
        if depth > 0:
            held[source] = depth + 1
            try:
                yield
            finally:
                held[source] = depth
            return
        lock_path = self._source_dir(source) / "_source.lock"
        lock_path.touch(exist_ok=True)
        with self._source_rlock(source):
            with open(lock_path, "r+") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    held[source] = 1
                    yield
                finally:
                    held.pop(source, None)
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def transaction(self, source: str) -> Iterator[None]:
        """来源级事务临界区：跨进程串行化“检查-决定-落盘”的复合操作。"""
        return self._exclusive_source(source)

    @staticmethod
    def _safe(source: str) -> str:
        if not isinstance(source, str) or not _SAFE_SOURCE.match(source):
            raise ValueError(f"非法来源标识: {source!r}")
        return source

    def _source_dir(self, source: str) -> Path:
        d = self.data_dir / self._safe(source)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _read_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write_json(self, path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        try:
            with open(fd, "w", encoding="utf-8") as fh:
                json.dump(value, fh, ensure_ascii=False, indent=2, sort_keys=True)
                fh.flush()
                os_replace_atomic(tmp_name, str(path))
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------ #
    # 目标库（落库结果）
    # ------------------------------------------------------------------ #
    def put_record(self, source: str, event_id: str, record: dict[str, Any]) -> bool:
        """幂等写入目标记录。

        返回 True 表示首次写入；False 表示记录已存在（不覆盖、不二次写入）。
        """
        path = self._source_dir(source) / "records.json"
        with self._exclusive_source(source):
            records = self._read_json(path, {})
            if event_id in records:
                return False
            records[event_id] = record
            self._write_json(path, records)
            return True

    def get_record(self, source: str, event_id: str) -> dict[str, Any] | None:
        path = self._source_dir(source) / "records.json"
        with self._exclusive_source(source):
            return self._read_json(path, {}).get(event_id)

    def list_records(self, source: str) -> list[dict[str, Any]]:
        path = self._source_dir(source) / "records.json"
        with self._exclusive_source(source):
            return list(self._read_json(path, {}).values())

    # ------------------------------------------------------------------ #
    # 幂等账本
    # ------------------------------------------------------------------ #
    def get_event(self, source: str, event_id: str) -> dict[str, Any] | None:
        path = self._source_dir(source) / "events.json"
        with self._exclusive_source(source):
            return self._read_json(path, {}).get(event_id)

    def save_event(self, source: str, event_id: str, doc: dict[str, Any]) -> None:
        path = self._source_dir(source) / "events.json"
        with self._exclusive_source(source):
            events = self._read_json(path, {})
            events[event_id] = doc
            self._write_json(path, events)

    def list_events(self, source: str) -> list[dict[str, Any]]:
        path = self._source_dir(source) / "events.json"
        with self._exclusive_source(source):
            docs = list(self._read_json(path, {}).values())
        return sorted(docs, key=lambda d: (d.get("seq") is None, d.get("seq")))

    # ------------------------------------------------------------------ #
    # 位点
    # ------------------------------------------------------------------ #
    def get_checkpoint(self, source: str) -> int:
        path = self._source_dir(source) / "checkpoint.json"
        with self._exclusive_source(source):
            return int(self._read_json(path, {"last_seq": 0})["last_seq"])

    def advance_checkpoint(self, source: str, seq: int) -> int:
        """仅允许单调推进，返回推进后的位点。"""
        path = self._source_dir(source) / "checkpoint.json"
        with self._exclusive_source(source):
            current = int(self._read_json(path, {"last_seq": 0})["last_seq"])
            if seq <= current:
                return current
            self._write_json(path, {"last_seq": seq})
            return seq

    # ------------------------------------------------------------------ #
    # 隔离区
    # ------------------------------------------------------------------ #
    def add_quarantine(self, source: str, doc: dict[str, Any]) -> None:
        path = self.data_dir / "quarantine.json"
        with self._exclusive():
            items = self._read_json(path, [])
            if not any(
                i.get("source") == source and i.get("event_id") == doc.get("event_id")
                for i in items
            ):
                items.append(doc)
                self._write_json(path, items)

    def list_quarantine(self, source: str | None = None) -> list[dict[str, Any]]:
        path = self.data_dir / "quarantine.json"
        with self._exclusive():
            items = self._read_json(path, [])
        if source is not None:
            items = [i for i in items if i.get("source") == source]
        return items

    # ------------------------------------------------------------------ #
    # 死信队列
    # ------------------------------------------------------------------ #
    def add_dead_letter(self, source: str, doc: dict[str, Any]) -> None:
        path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            items = self._read_json(path, [])
            if not any(
                i.get("source") == source and i.get("event_id") == doc.get("event_id")
                for i in items
            ):
                items.append(doc)
                self._write_json(path, items)

    def list_dead_letters(self, source: str | None = None) -> list[dict[str, Any]]:
        path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            items = self._read_json(path, [])
        if source is not None:
            items = [i for i in items if i.get("source") == source]
        return items

    def remove_dead_letter(self, source: str, event_id: str) -> None:
        path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            items = self._read_json(path, [])
            remaining = [
                i for i in items
                if not (i.get("source") == source and i.get("event_id") == event_id)
            ]
            if len(remaining) != len(items):
                self._write_json(path, remaining)

    # ------------------------------------------------------------------ #
    # 进度对账快照
    # ------------------------------------------------------------------ #
    def progress_snapshot(self, source: str) -> dict[str, int]:
        """在同一临界区内读取来源级与全局文件，返回不自相矛盾的对账快照。

        锁顺序固定为 来源锁 -> 全局锁，与其他路径不形成环。
        """
        src_dir = self._source_dir(source)
        with self._exclusive_source(source):
            checkpoint = int(
                self._read_json(src_dir / "checkpoint.json", {"last_seq": 0})["last_seq"]
            )
            events = list(self._read_json(src_dir / "events.json", {}).values())
            records = self._read_json(src_dir / "records.json", {})
            with self._exclusive():
                quarantine = self._read_json(self.data_dir / "quarantine.json", [])
                dead_letters = self._read_json(self.data_dir / "dead_letters.json", [])

        def _count(status: str) -> int:
            return sum(1 for e in events if e.get("status") == status)

        return {
            "last_seq": checkpoint,
            "applied": _count("APPLIED"),
            "records": len(records),
            "quarantined": sum(1 for i in quarantine if i.get("source") == source),
            "dead_letter": sum(1 for i in dead_letters if i.get("source") == source),
            "pending": _count("RETRYING"),
        }


def os_replace_atomic(src: str, dst: str) -> None:
    """包装 os.replace，便于单测桩替换。"""
    import os
    os.replace(src, dst)
