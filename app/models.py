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
    REJECTED = "REJECTED"          # 仅批量投递：该位置事件无法标识/路由，被逐条拒绝（不连累整批）


class RejectReason(str, Enum):
    """可区分的拒绝/隔离原因。"""

    MISSING_FIELD = "MISSING_FIELD"            # 字段缺失
    TYPE_MISMATCH = "TYPE_MISMATCH"            # 类型不匹配
    MALFORMED_STRUCTURE = "MALFORMED_STRUCTURE"  # 结构非法
    MISSING_EVENT_ID = "MISSING_EVENT_ID"      # 缺少事件标识
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"        # 重试耗尽
    SOURCE_MISMATCH = "SOURCE_MISMATCH"        # 事件体来源与投递来源不一致（跨来源串用）
    BATCH_MALFORMED = "BATCH_MALFORMED"        # 批量请求信封非法（events 缺失/不是数组）
    PROCESSING_ERROR = "PROCESSING_ERROR"      # 防御性兜底：单条处理时发生意外错误


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


class BatchItemResult(BaseModel):
    """批量投递中“每一个位置”的独立判定，与请求数组下标一一对应。"""

    index: int
    event_id: str | None = None
    source: str
    seq: int | None = None
    status: EventStatus
    reason: RejectReason | None = None
    attempts: int = 0
    detail: str | None = None
    duplicate_of: str | None = None


class BatchSummary(BaseModel):
    """批次汇总计数：各状态条数；received 为请求中的事件条数。"""

    received: int = 0
    applied: int = 0
    duplicate: int = 0
    quarantined: int = 0
    retrying: int = 0
    dead_letter: int = 0
    rejected: int = 0


class BatchIngestResponse(BaseModel):
    source: str
    summary: BatchSummary
    results: list[BatchItemResult]


class SourceProgress(BaseModel):
    """来源级进度对账快照。

    各字段与同一把排他锁内读到的账本/落库文件相互对得上：
    applied_count == 目标库记录条数 == 账本中 APPLIED 条数；
    quarantined_count / dead_letter_count 与隔离区、死信队列一致；
    pending_count 为账本中仍是 RETRYING（未到终态）的条数；
    账本总条数等于 applied + quarantined + dead_letter + pending。
    """

    source: str
    last_seq: int
    applied_count: int
    duplicate_count: int
    quarantined_count: int
    dead_letter_count: int
    pending_count: int
    ledger_total: int
    checked_at: str
