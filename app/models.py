"""数据模型与判定类型定义。"""
from __future__ import annotations

from enum import Enum
from typing import Any


class Verdict(str, Enum):
    """一次投递/处理的判定结论（响应可区分）。"""

    ACCEPTED = "accepted"            # 首次成功应用
    DUPLICATE = "duplicate"          # 幂等命中：已生效事件重复投递
    STALE = "stale"                  # 位点落后于检查点（重复/回退投递）
    BUFFERED = "buffered"            # 位点超前，已缓存等待缺口补齐
    RETRYING = "retrying"            # 上游暂时失败，已登记可重试
    DEAD_LETTER = "dead_letter"      # 达到终止条件，进入隔离区
    REJECTED = "rejected"            # 非法事件被拒绝并隔离


class ErrorClass(str, Enum):
    """错误分类（可区分原因）。"""

    MISSING_FIELD = "missing_field"          # 必填字段缺失
    TYPE_MISMATCH = "type_mismatch"          # 字段类型不匹配
    INVALID_STRUCTURE = "invalid_structure"  # 结构非法（非对象/未知操作等）
    CHECKPOINT_REGRESSION = "checkpoint_regression"  # 检查点回退
    CROSS_SOURCE = "cross_source"            # 跨来源串用
    TRANSIENT = "transient"                  # 上游暂时失败（可重试）
    POISON = "poison"                        # 重试耗尽后的毒消息


class Event:
    """归一化后的增量事件。"""

    __slots__ = ("source", "seq", "event_id", "op", "payload")

    def __init__(self, source: str, seq: int, event_id: str, op: str, payload: dict):
        self.source = source
        self.seq = seq
        self.event_id = event_id
        self.op = op
        self.payload = payload

    def __repr__(self) -> str:
        return (f"Event(source={self.source!r}, seq={self.seq}, "
                f"event_id={self.event_id!r}, op={self.op!r})")


class IngestResult:
    """投递/续跑接口的返回结果，携带判定依据，便于日志与断言。"""

    def __init__(self, verdict: Verdict | str, source: str,
                 seq: int | None, event_id: str | None,
                 reason: str | None = None, attempts: int = 0,
                 checkpoint: int | None = None,
                 error_class: ErrorClass | str | None = None):
        self.verdict = Verdict(verdict)
        self.source = source
        self.seq = seq
        self.event_id = event_id
        self.reason = reason
        self.attempts = attempts
        self.checkpoint = checkpoint
        self.error_class = (
            ErrorClass(error_class) if error_class is not None else None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "source": self.source,
            "seq": self.seq,
            "event_id": self.event_id,
            "reason": self.reason,
            "attempts": self.attempts,
            "checkpoint": self.checkpoint,
            "error_class": self.error_class.value if self.error_class else None,
        }
