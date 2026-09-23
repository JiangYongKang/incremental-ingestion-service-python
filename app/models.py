"""数据契约：入站事件、投递响应、位点状态。"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    """单条事件的最终判定。"""

    APPLIED = "APPLIED"            # 首次应用成功
    DUPLICATE = "DUPLICATE"        # 重复投递，未二次写入
    QUARANTINED = "QUARANTINED"    # 坏事件被隔离
    RETRYING = "RETRYING"          # 上游暂时失败，重试中
    DEAD_LETTER = "DEAD_LETTER"    # 重试耗尽，进入死信


class RejectReason(str, Enum):
    """可区分的拒绝/隔离原因。"""

    MISSING_FIELD = "MISSING_FIELD"            # 字段缺失
    TYPE_MISMATCH = "TYPE_MISMATCH"            # 类型不匹配
    MALFORMED_STRUCTURE = "MALFORMED_STRUCTURE"  # 结构非法
    MISSING_EVENT_ID = "MISSING_EVENT_ID"      # 缺少事件标识
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"        # 重试耗尽


class CheckpointErrorType(str, Enum):
    ROLLBACK = "CHECKPOINT_ROLLBACK"
    SOURCE_MISMATCH = "SOURCE_MISMATCH"
    GAP = "CHECKPOINT_GAP"


class IncomingEvent(BaseModel):
    """来源端推送的增量事件（请求体）。"""

    event_id: str | None = None
    source: str | None = None
    seq: int | None = None
    payload: dict[str, Any] | None = None


class EventResult(BaseModel):
    event_id: str
    source: str
    seq: int | None = None
    status: EventStatus
    reason: RejectReason | None = None
    attempts: int = 0
    detail: str | None = None
    duplicate_of: str | None = None


class CheckpointCommit(BaseModel):
    source: str
    seq: int


class CheckpointView(BaseModel):
    source: str
    last_seq: int


class DeadLetterView(BaseModel):
    event_id: str
    source: str
    seq: int | None = None
    reason: RejectReason
    attempts: int
    detail: str | None = None


class QuarantineView(BaseModel):
    event_id: str
    source: str
    seq: int | None = None
    reason: RejectReason
    detail: str | None = None
