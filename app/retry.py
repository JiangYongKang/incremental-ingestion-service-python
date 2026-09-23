"""重试与退避策略：固定退避 + 最大尝试次数终止。

策略本身不 sleep、不执行重试，只回答两个问题：
1. 已尝试 attempts 次后是否还应重试；
2. 下一次重试前应等待多久（供调用方解释/调度，测试中可不实际等待）。
"""
from __future__ import annotations

from app.models import Event

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 0.05  # 本地验证用很短的退避


class RetryPolicy:
    def __init__(self, max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 backoff_seconds: float = DEFAULT_BACKOFF_SECONDS):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    def should_retry(self, attempts: int) -> bool:
        """attempts 为已经发生过的尝试次数（含首次）。"""
        return attempts < self.max_attempts

    def wait_seconds(self, attempts: int) -> float:
        """第 attempts 次失败后，下一次重试前的等待时长。"""
        return self.backoff_seconds

    def describe(self, event: Event, attempts: int) -> str:
        """给出人类可解释的重试/终止判定依据。"""
        if self.should_retry(attempts):
            return (f"event={event.event_id} seq={event.seq} 上游暂时失败，"
                    f"已尝试 {attempts}/{self.max_attempts} 次，"
                    f"{self.wait_seconds(attempts)}s 后可重试")
        return (f"event={event.event_id} seq={event.seq} 已达最大尝试次数 "
                f"{self.max_attempts}，终止重试并转入死信隔离区")
