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
import hashlib
import json
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

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

    @contextmanager
    def event_lock(self, source: str, event_id: str) -> Iterator[None]:
        """跨进程的单事件排他锁。

        多进程同时投递重叠批次时，对同一 (source, event_id) 的“查账本→判定→写入”
        必须整体互斥，才能保证只有一个进程得到 APPLIED，其余得到可区分的 DUPLICATE。
        锁文件按来源分目录（<source>/_locks/<hash>.lock），来源之间互不阻塞；
        不使用 event_id 直接做文件名，避免异常字符与超长标识问题。
        """
        safe_source = self._safe(source)
        locks_dir = self.data_dir / safe_source / "_locks"
        locks_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        lock_path = locks_dir / f"{digest}.lock"
        lock_path.touch(exist_ok=True)
        # 不与 _exclusive 嵌套争用同一把 flock：本锁只协调同一事件的跨进程临界区，
        # 临界区内对 JSON 文件的读改写仍各自经过 _exclusive，锁序恒为 event_lock -> global。
        with open(lock_path, "r+") as fh:
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
    # 复合原子提交：账本 + 落库 + 位点 / 隔离 / 死信，同一把锁内完成
    # ------------------------------------------------------------------ #
    def commit_apply(
        self,
        source: str,
        event_id: str,
        record: dict[str, Any],
        ledger_doc: dict[str, Any] | Callable[[bool], dict[str, Any]],
        seq: int,
        *,
        remove_dead_letter: bool = False,
    ) -> bool:
        """应用成功路径的原子复合提交。

        在同一个排他临界区内：条件写目标记录（已存在则不覆盖）→ 写账本 → 推进位点。
        因此任何并发/崩溃窗口下，外部读到的快照要么“没生效”，要么“记录、账本、位点
        全部已生效”，不会出现账本说 APPLIED 却查不到记录这类自相矛盾。

        返回 True 表示本次为首次写入；False 表示目标记录此前已存在（崩溃恢复确认路径）。
        """
        sdir = self._source_dir(source)
        records_path = sdir / "records.json"
        events_path = sdir / "events.json"
        cp_path = sdir / "checkpoint.json"
        dl_path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            records = self._read_json(records_path, {})
            first_write = event_id not in records
            if first_write:
                records[event_id] = record
                self._write_json(records_path, records)
            doc = ledger_doc(first_write) if callable(ledger_doc) else ledger_doc
            events = self._read_json(events_path, {})
            events[event_id] = doc
            self._write_json(events_path, events)
            current = int(self._read_json(cp_path, {"last_seq": 0})["last_seq"])
            if seq > current:
                self._write_json(cp_path, {"last_seq": seq})
            if remove_dead_letter:
                self._remove_dead_letter_locked(source, event_id, dl_path)
            return first_write

    def commit_ledger_only(
        self,
        source: str,
        event_id: str,
        ledger_doc: dict[str, Any],
        *,
        remove_dead_letter: bool = False,
    ) -> None:
        """只更新账本（RETRYING 中间态）；重放场景下同步从死信队列移除。"""
        events_path = self._source_dir(source) / "events.json"
        dl_path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            events = self._read_json(events_path, {})
            events[event_id] = ledger_doc
            self._write_json(events_path, events)
            if remove_dead_letter:
                self._remove_dead_letter_locked(source, event_id, dl_path)

    def _remove_dead_letter_locked(
        self, source: str, event_id: str, dl_path: Path
    ) -> bool:
        """调用方必须已持有 _exclusive。返回是否实际移除。"""
        items = self._read_json(dl_path, [])
        remaining = [
            i
            for i in items
            if not (i.get("source") == source and i.get("event_id") == event_id)
        ]
        if len(remaining) != len(items):
            self._write_json(dl_path, remaining)
            return True
        return False

    def commit_quarantine(
        self,
        source: str,
        event_id: str,
        ledger_doc: dict[str, Any],
        seq: int | None,
    ) -> None:
        """隔离路径原子复合提交：账本 + 隔离区 +（有合法 seq 时）位点。"""
        sdir = self._source_dir(source)
        events_path = sdir / "events.json"
        cp_path = sdir / "checkpoint.json"
        q_path = self.data_dir / "quarantine.json"
        with self._exclusive():
            events = self._read_json(events_path, {})
            events[event_id] = ledger_doc
            self._write_json(events_path, events)

            items = self._read_json(q_path, [])
            if not any(
                i.get("source") == source and i.get("event_id") == event_id
                for i in items
            ):
                items.append(ledger_doc)
                self._write_json(q_path, items)

            if isinstance(seq, int) and not isinstance(seq, bool):
                current = int(self._read_json(cp_path, {"last_seq": 0})["last_seq"])
                if seq > current:
                    self._write_json(cp_path, {"last_seq": seq})

    def commit_dead_letter(
        self, source: str, event_id: str, ledger_doc: dict[str, Any]
    ) -> None:
        """死信路径原子复合提交：账本 + 死信队列。"""
        events_path = self._source_dir(source) / "events.json"
        dl_path = self.data_dir / "dead_letters.json"
        with self._exclusive():
            events = self._read_json(events_path, {})
            events[event_id] = ledger_doc
            self._write_json(events_path, events)

            items = self._read_json(dl_path, [])
            if not any(
                i.get("source") == source and i.get("event_id") == event_id
                for i in items
            ):
                items.append(ledger_doc)
                self._write_json(dl_path, items)

    # ------------------------------------------------------------------ #
    # 来源级进度快照：同一把锁内读取全部相关文件
    # ------------------------------------------------------------------ #
    def source_snapshot(self, source: str) -> dict[str, Any]:
        """在单一排他临界区内读取对账所需的全部数据。

        返回同一时刻磁盘上一致的：checkpoint、events 账本、records 目标记录、
        隔离区（本来源）、死信（本来源）。调用方在此基础上做纯内存计数，
        因而在并发写入期间读取也不会得到互相矛盾的数字。
        """
        sdir = self._source_dir(source)
        with self._exclusive():
            last_seq = int(
                self._read_json(sdir / "checkpoint.json", {"last_seq": 0})["last_seq"]
            )
            events = dict(self._read_json(sdir / "events.json", {}))
            records = dict(self._read_json(sdir / "records.json", {}))
            quarantine = [
                i
                for i in self._read_json(self.data_dir / "quarantine.json", [])
                if i.get("source") == source
            ]
            dead_letters = [
                i
                for i in self._read_json(self.data_dir / "dead_letters.json", [])
                if i.get("source") == source
            ]
        return {
            "last_seq": last_seq,
            "events": events,
            "records": records,
            "quarantine": quarantine,
            "dead_letters": dead_letters,
        }

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
