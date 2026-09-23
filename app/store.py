"""本地文件存储：模拟目标库落库、幂等记录、位点与隔离区/死信队列。

存储布局（data_dir 下）：

    <source>/checkpoint.json   {"last_seq": int}
    <source>/events.json       {event_id: 事件账本文档}   —— 幂等账本
    <source>/records.json      {event_id: 落库记录}       —— 模拟目标库
    quarantine.json            [隔离事件...]
    dead_letters.json          [死信事件...]
    _global.lock               进程间文件锁

并发策略：进程内 threading.RLock + 进程间 fcntl.flock 排他锁，
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

    # ------------------------------------------------------------------ #
    # 内部原语
    # ------------------------------------------------------------------ #
    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        """进程内 + 跨进程排他临界区。"""
        with self._lock:
            with open(self._lock_file, "r+") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

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
        with self._exclusive():
            records = self._read_json(path, {})
            if event_id in records:
                return False
            records[event_id] = record
            self._write_json(path, records)
            return True

    def get_record(self, source: str, event_id: str) -> dict[str, Any] | None:
        path = self._source_dir(source) / "records.json"
        with self._exclusive():
            return self._read_json(path, {}).get(event_id)

    def list_records(self, source: str) -> list[dict[str, Any]]:
        path = self._source_dir(source) / "records.json"
        with self._exclusive():
            return list(self._read_json(path, {}).values())

    # ------------------------------------------------------------------ #
    # 幂等账本
    # ------------------------------------------------------------------ #
    def get_event(self, source: str, event_id: str) -> dict[str, Any] | None:
        path = self._source_dir(source) / "events.json"
        with self._exclusive():
            return self._read_json(path, {}).get(event_id)

    def save_event(self, source: str, event_id: str, doc: dict[str, Any]) -> None:
        path = self._source_dir(source) / "events.json"
        with self._exclusive():
            events = self._read_json(path, {})
            events[event_id] = doc
            self._write_json(path, events)

    def list_events(self, source: str) -> list[dict[str, Any]]:
        path = self._source_dir(source) / "events.json"
        with self._exclusive():
            docs = list(self._read_json(path, {}).values())
        return sorted(docs, key=lambda d: (d.get("seq") is None, d.get("seq")))

    # ------------------------------------------------------------------ #
    # 位点
    # ------------------------------------------------------------------ #
    def get_checkpoint(self, source: str) -> int:
        path = self._source_dir(source) / "checkpoint.json"
        with self._exclusive():
            return int(self._read_json(path, {"last_seq": 0})["last_seq"])

    def advance_checkpoint(self, source: str, seq: int) -> int:
        """仅允许单调推进，返回推进后的位点。"""
        path = self._source_dir(source) / "checkpoint.json"
        with self._exclusive():
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


def os_replace_atomic(src: str, dst: str) -> None:
    """包装 os.replace，便于单测桩替换。"""
    import os
    os.replace(src, dst)
