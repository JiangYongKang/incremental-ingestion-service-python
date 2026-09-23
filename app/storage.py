"""本地文件存储：用 JSON 文件模拟源端状态与目标端落库。

并发安全：
- 进程内：每来源一把 threading.RLock（service 的"校验->写状态->落库"临界区使用）；
- 进程间：msvcrt(fcntl) 文件锁保护状态文件的"读改写"，保证多进程并发重复投递也只生效一次。

数据目录结构：
  data/state/<source>.json   来源状态（检查点、已生效索引、缓冲、重试中、死信、拒绝）
  data/target/<source>.json  目标端表数据（模拟落库）
"""
from __future__ import annotations

import fcntl
import json
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class SourceState:
    """单个来源的全部可持久化状态。"""

    def __init__(self, checkpoint: int = 0,
                 applied: dict[str, dict[str, Any]] | None = None,
                 buffered: dict[str, dict[str, Any]] | None = None,
                 retrying: dict[str, dict[str, Any]] | None = None,
                 dead_letters: list[dict[str, Any]] | None = None,
                 rejected: list[dict[str, Any]] | None = None):
        self.checkpoint = checkpoint
        # seq -> {"event_id", "op", "payload"}：已生效事件索引（幂等账本）
        self.applied = applied if applied is not None else {}
        # seq -> 同上：位点超前、等待缺口补齐的事件
        self.buffered = buffered if buffered is not None else {}
        # seq -> 同上 + {"attempts"}：上游暂时失败、待续跑的事件
        self.retrying = retrying if retrying is not None else {}
        # [{"seq","event_id","op","payload","attempts","reason"}]：重试耗尽的毒消息
        self.dead_letters = dead_letters if dead_letters is not None else []
        # [{"seq","event_id","error_class","reason","raw"}]：被拒绝隔离的非法事件
        self.rejected = rejected if rejected is not None else []

    def to_dict(self) -> dict[str, Any]:
        return {
            "checkpoint": self.checkpoint,
            "applied": self.applied,
            "buffered": self.buffered,
            "retrying": self.retrying,
            "dead_letters": self.dead_letters,
            "rejected": self.rejected,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceState":
        return cls(
            checkpoint=int(data.get("checkpoint", 0)),
            applied={str(k): v for k, v in data.get("applied", {}).items()},
            buffered={str(k): v for k, v in data.get("buffered", {}).items()},
            retrying={str(k): v for k, v in data.get("retrying", {}).items()},
            dead_letters=list(data.get("dead_letters", [])),
            rejected=list(data.get("rejected", [])),
        )


def _safe_source(source: str) -> str:
    """来源名仅允许安全字符，防止路径穿越。"""
    if not source or not all(c.isalnum() or c in "._-" for c in source):
        raise ValueError(f"非法来源名: {source!r}")
    return source


class LocalStorage:
    def __init__(self, data_dir: str | Path = "data"):
        self.data_dir = Path(data_dir)
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    def _ensure_dirs(self) -> None:
        """惰性创建目录：仅在真正读写时落盘，避免仅导入模块即产生文件。"""
        (self.data_dir / "state").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "target").mkdir(parents=True, exist_ok=True)

    # ---------- 锁 ----------

    def _thread_lock(self, source: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(source)
            if lock is None:
                lock = threading.RLock()
                self._locks[source] = lock
            return lock

    @contextmanager
    def source_lock(self, source: str) -> Iterator[None]:
        """获取来源的进程内可重入锁（同一线程可嵌套）。"""
        lock = self._thread_lock(source)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    @contextmanager
    def file_lock(self, path: Path) -> Iterator[None]:
        """对文件加进程间排他锁（锁文件，非状态文件本身）。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # ---------- 状态 ----------

    def _state_path(self, source: str) -> Path:
        return self.data_dir / "state" / f"{_safe_source(source)}.json"

    def _state_lock_path(self, source: str) -> Path:
        return self.data_dir / "state" / f"{_safe_source(source)}.lock"

    def load_state(self, source: str) -> SourceState:
        path = self._state_path(source)
        if not path.exists():
            return SourceState()
        with open(path, encoding="utf-8") as f:
            return SourceState.from_dict(json.load(f))

    def save_state(self, source: str, state: SourceState) -> None:
        """原子写：临时文件 + fsync + rename，避免重启读到半截状态。"""
        self._ensure_dirs()
        path = self._state_path(source)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    @contextmanager
    def transaction(self, source: str) -> Iterator[SourceState]:
        """线程锁 + 进程文件锁保护下读入状态，临界区内修改，退出时原子持久化。"""
        with self.source_lock(source):
            with self.file_lock(self._state_lock_path(source)):
                state = self.load_state(source)
                yield state
                self.save_state(source, state)

    # ---------- 目标端 ----------

    def _target_path(self, source: str) -> Path:
        return self.data_dir / "target" / f"{_safe_source(source)}.json"

    def _load_target(self, source: str) -> dict[str, dict[str, Any]]:
        path = self._target_path(source)
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def _save_target(self, source: str, rows: dict[str, dict[str, Any]]) -> None:
        self._ensure_dirs()
        path = self._target_path(source)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def apply_to_target(self, source: str, op: str, record: dict[str, Any]) -> None:
        """对目标端应用一条 upsert/delete（在来源锁临界区内调用）。"""
        rows = self._load_target(source)
        key = str(record["key"])
        if op == "upsert":
            rows[key] = {"key": key, "value": record.get("value")}
        elif op == "delete":
            rows.pop(key, None)
        else:
            raise ValueError(f"未知操作类型: {op!r}")
        self._save_target(source, rows)

    def snapshot_target(self, source: str) -> dict[str, Any]:
        with self.source_lock(source):
            return self._load_target(source)

    def list_sources(self) -> list[str]:
        self._ensure_dirs()
        return sorted(p.stem for p in (self.data_dir / "state").glob("*.json"))
