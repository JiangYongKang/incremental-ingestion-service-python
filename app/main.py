"""FastAPI 入口：本地增量摄取服务。

接口（source 为路径参数，天然隔离不同来源）：
  POST   /sources/{source}/events             投递增量事件
  POST   /sources/{source}/retry              续跑未完成事件
  POST   /sources/{source}/checkpoint         显式检查点断言/推进
  POST   /sources/{source}/dead-letters/{seq}/replay  重放死信
  GET    /sources/{source}/status             来源状态
  GET    /sources/{source}/target             目标端落库快照
  GET    /sources                             所有来源
"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.errors import CheckpointError, InvalidEventError
from app.models import IngestResult, Verdict
from app.retry import RetryPolicy
from app.service import IngestionService
from app.storage import LocalStorage

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s")

DATA_DIR = os.environ.get("INGEST_DATA_DIR", "data")
MAX_ATTEMPTS = int(os.environ.get("INGEST_MAX_ATTEMPTS", "5"))

storage = LocalStorage(DATA_DIR)
service = IngestionService(storage, RetryPolicy(max_attempts=MAX_ATTEMPTS),
                           transient_rate=0.0)
app = FastAPI(title="incremental-ingestion-service", version="0.1.0")


def _http_status(verdict: Verdict) -> int:
    return {
        Verdict.ACCEPTED: 200,
        Verdict.DUPLICATE: 200,   # 幂等成功，但响应体 verdict 可区分
        Verdict.STALE: 409,       # 位点回退/重复消费
        Verdict.BUFFERED: 202,    # 已接收，等待缺口
        Verdict.RETRYING: 202,    # 已登记，待续跑
        Verdict.DEAD_LETTER: 422, # 终止隔离
        Verdict.REJECTED: 400,    # 非法事件
    }[verdict]


def _respond(result: IngestResult) -> JSONResponse:
    return JSONResponse(status_code=_http_status(result.verdict),
                        content=result.to_dict())


@app.exception_handler(CheckpointError)
def _checkpoint_handler(_request: Request, exc: CheckpointError) -> JSONResponse:
    logging.getLogger("ingestion").warning(
        "source=%s seq=%s verdict=checkpoint_rejected reason=%s",
        exc.source, exc.seq, exc.message)
    return JSONResponse(
        status_code=409,
        content={"verdict": "rejected", "source": exc.source, "seq": exc.seq,
                 "error_class": exc.error_class.value, "reason": exc.message})


@app.exception_handler(InvalidEventError)
def _invalid_handler(_request: Request, exc: InvalidEventError) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"verdict": "rejected", "source": exc.source, "seq": exc.seq,
                 "event_id": exc.event_id,
                 "error_class": exc.error_class.value, "reason": exc.message})


@app.get("/")
def read_root():
    return {"service": "incremental-ingestion-service",
            "data_dir": DATA_DIR, "max_attempts": MAX_ATTEMPTS}


@app.get("/sources")
def list_sources():
    return {"sources": storage.list_sources()}


@app.post("/sources/{source}/events")
async def post_event(source: str, request: Request):
    try:
        raw = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"verdict": "rejected", "source": source,
                     "error_class": "invalid_structure",
                     "reason": "请求体不是合法 JSON"})
    return _respond(service.ingest(source, raw))


@app.post("/sources/{source}/retry")
async def post_retry(source: str, request: Request):
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    force_fail = bool((body or {}).get("force_fail", False))
    results = service.retry_pending(source, force_fail=force_fail)
    payload = {"resumed": [r.to_dict() for r in results], "count": len(results)}
    return JSONResponse(status_code=200, content=payload)


@app.post("/sources/{source}/checkpoint")
async def post_checkpoint(source: str, body: dict):
    return _respond(service.advance_checkpoint(source, body.get("seq")))


@app.post("/sources/{source}/dead-letters/{seq}/replay")
def post_replay(source: str, seq: int):
    return _respond(service.replay_dead_letter(source, seq))


@app.get("/sources/{source}/status")
def get_status(source: str):
    return service.status(source)


@app.get("/sources/{source}/target")
def get_target(source: str):
    return {"source": source, "rows": service.target_snapshot(source)}
