"""异常与错误分类。"""
from __future__ import annotations

from app.models import ErrorClass


class IngestError(Exception):
    """摄取链路业务异常基类。"""

    def __init__(self, error_class: ErrorClass, message: str,
                 source: str | None = None, seq: int | None = None,
                 event_id: str | None = None):
        super().__init__(message)
        self.error_class = error_class
        self.message = message
        self.source = source
        self.seq = seq
        self.event_id = event_id


class InvalidEventError(IngestError):
    """字段缺失/类型不匹配/结构非法：不可重试，直接拒绝隔离。"""


class TransientFailureError(IngestError):
    """上游暂时失败：可按退避策略重试。"""


class CheckpointError(IngestError):
    """检查点回退或跨来源串用。"""
