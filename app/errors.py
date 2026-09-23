"""领域错误类型与 HTTP 异常映射。"""

from __future__ import annotations

from app.models import CheckpointErrorType, RejectReason


class IngestionError(Exception):
    """所有摄取领域错误的基类。"""

    http_status: int = 400
    code: str = "INGESTION_ERROR"

    def __init__(self, message: str, *, reason: RejectReason | str | None = None):
        super().__init__(message)
        self.message = message
        self.reason = reason


class ValidationFailure(IngestionError):
    """事件字段缺失/类型不匹配/结构非法。"""

    http_status = 422

    def __init__(self, message: str, reason: RejectReason, *, event_id: str | None = None):
        super().__init__(message, reason=reason)
        self.event_id = event_id


class CheckpointError(IngestionError):
    """位点回退、跨来源串用、跳号。"""

    http_status = 409

    def __init__(self, message: str, error_type: CheckpointErrorType):
        super().__init__(message, reason=error_type.value)
        self.error_type = error_type


class UpstreamFailure(IngestionError):
    """模拟的上游暂时性失败（可重试）。"""

    http_status = 503
    code = "UPSTREAM_TEMPORARY_FAILURE"


class UnroutableEvent(IngestionError):
    """无法标识或路由的事件：非对象、缺少 event_id 等（不隔离，直接拒绝）。"""

    http_status = 400

    def __init__(self, message: str, reason: RejectReason):
        super().__init__(message, reason=reason)
        self.code = reason.value
