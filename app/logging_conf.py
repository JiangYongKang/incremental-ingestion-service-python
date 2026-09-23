"""结构化判定日志：事件标识、位点、判定依据。"""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("ingest")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | event=%(event_id)s "
                "source=%(source)s seq=%(seq)s checkpoint=%(checkpoint)s "
                "verdict=%(verdict)s reason=%(reason)s attempts=%(attempts)s | %(message)s"
            )
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    # 保持向上传播：pytest 的 live log 与 uvicorn 日志都能接管输出
    logger.propagate = True
    return logger


def log_verdict(
    logger: logging.Logger,
    *,
    event_id: str,
    source: str,
    seq: int | None,
    checkpoint: int | None,
    verdict: str,
    reason: str | None,
    attempts: int,
    message: str = "",
) -> None:
    logger.info(
        message,
        extra={
            "event_id": event_id,
            "source": source,
            "seq": seq,
            "checkpoint": checkpoint,
            "verdict": verdict,
            "reason": reason or "-",
            "attempts": attempts,
        },
    )
