"""HTTP 接口层。

状态码约定：
200 APPLIED / DUPLICATE / replay 成功；批量投递恒为 200（逐条结果看响应体）
202 RETRYING（暂时失败，已记录尝试次数，可重投或 /resume）
400 跨来源串用、请求体非 JSON 对象、缺少 event_id 等无法标识/路由的错误；
    批量请求信封非法（events 缺失/不是数组）
409 位点回退
422 可标识的坏事件（已隔离）
503 重试耗尽进入死信
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from app.errors import (
    CheckpointError,
    IngestionError,
    UnroutableEvent,
    UpstreamFailure,
    ValidationFailure,
)
from app.logging_conf import configure_logging
from app.models import (
    CheckpointCommit,
    CheckpointView,
    DeadLetterView,
    EventResult,
    EventStatus,
    QuarantineView,
    SourceProgress,
)
from app.service import CrossSourceError, IngestionService
from app.store import LocalStore

router = APIRouter()
_service: IngestionService | None = None


def get_service() -> IngestionService:
    assert _service is not None, "service 未初始化"
    return _service


def create_app(data_dir: str | None = None) -> FastAPI:
    global _service
    data_dir = data_dir or os.environ.get("INGEST_DATA_DIR", "data")
    _service = IngestionService(LocalStore(data_dir))
    configure_logging()

    app = FastAPI(
        title="增量数据摄取服务",
        version="0.1.0",
        description=(
            "本地存储模拟源端推送与目标落库：幂等应用、单调位点、"
            "坏事件隔离、有界重试/死信与续跑。"
        ),
    )
    app.include_router(router)
    return app


def _reason_value(reason: Any) -> str | None:
    if reason is None:
        return None
    return getattr(reason, "value", str(reason))


def _error_response(exc: IngestionError) -> JSONResponse:
    body = {
        "error": exc.code,
        "message": exc.message,
        "reason": _reason_value(exc.reason),
    }
    return JSONResponse(status_code=exc.http_status, content=body)


def _result_response(result: EventResult) -> JSONResponse:
    status_map = {
        EventStatus.APPLIED: 200,
        EventStatus.DUPLICATE: 200,
        EventStatus.RETRYING: 202,
        EventStatus.QUARANTINED: 422,
        EventStatus.DEAD_LETTER: 503,
    }
    return JSONResponse(
        status_code=status_map[result.status],
        content=result.model_dump(mode="json"),
    )


@router.post("/v1/sources/{source}/events")
async def post_event(source: str, request: Request) -> JSONResponse:
    try:
        raw = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={
                "error": "MALFORMED_STRUCTURE",
                "message": "请求体不是合法 JSON",
                "reason": "MALFORMED_STRUCTURE",
            },
        )
    try:
        result = get_service().ingest(raw, source)
    except UnroutableEvent as exc:
        return _error_response(exc)
    except CrossSourceError as exc:
        return _error_response(exc)
    except ValidationFailure as exc:
        return _error_response(exc)
    except IngestionError as exc:
        return _error_response(exc)
    return _result_response(result)


@router.post("/v1/sources/{source}/events/batch")
async def post_event_batch(source: str, request: Request) -> JSONResponse:
    """批量投递同一来源的一批事件。

    与单条接口共存：只要请求信封合法（{"events": [...]}），HTTP 恒为 200，
    每条事件的判定/原因在 results 中按下标逐条给出；个别坏事件/重复/暂时失败
    不会导致整批失败。信封本身非法（非 JSON、无 events、events 非数组）才返回 400。
    """
    try:
        raw = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={
                "error": "BATCH_MALFORMED",
                "message": "请求体不是合法 JSON",
                "reason": "BATCH_MALFORMED",
            },
        )
    try:
        batch = get_service().ingest_batch(raw, source)
    except UnroutableEvent as exc:
        return _error_response(exc)
    except IngestionError as exc:
        return _error_response(exc)
    # 批量整体不因单条结果而改变 HTTP 状态：逐条结论看 results[i].status
    return JSONResponse(status_code=200, content=batch.model_dump(mode="json"))


@router.post("/v1/checkpoints")
async def post_checkpoint(body: CheckpointCommit) -> JSONResponse:
    try:
        new_seq = get_service().commit_checkpoint(body.source, body.seq)
    except CheckpointError as exc:
        return _error_response(exc)
    except ValidationFailure as exc:
        return _error_response(exc)
    return JSONResponse(
        status_code=200,
        content=CheckpointView(source=body.source, last_seq=new_seq).model_dump(),
    )


@router.get("/v1/sources/{source}/checkpoint")
async def get_checkpoint(source: str) -> dict[str, Any]:
    return CheckpointView(
        source=source, last_seq=get_service().store.get_checkpoint(source)
    ).model_dump()


@router.get("/v1/sources/{source}/progress")
async def get_source_progress(source: str) -> dict[str, Any]:
    """来源级进度对账：当前位点、已生效/隔离/死信/未完成条数，彼此对得上。"""
    progress: SourceProgress = get_service().source_progress(source)
    return progress.model_dump(mode="json")


@router.post("/v1/sources/{source}/resume")
async def post_resume(source: str) -> dict[str, Any]:
    results = get_service().resume(source)
    return {
        "source": source,
        "resumed": len(results),
        "results": [r.model_dump(mode="json") for r in results],
    }


@router.get("/v1/sources/{source}/records")
async def get_records(source: str) -> dict[str, Any]:
    records = get_service().store.list_records(source)
    return {"source": source, "count": len(records), "records": records}


@router.get("/v1/sources/{source}/events")
async def get_events(source: str) -> dict[str, Any]:
    docs = get_service().store.list_events(source)
    return {"source": source, "count": len(docs), "events": docs}


@router.get("/v1/quarantine")
async def get_quarantine(source: str | None = None) -> dict[str, Any]:
    items = get_service().store.list_quarantine(source)
    return {"count": len(items), "items": [QuarantineView(**i).model_dump() for i in items]}


@router.get("/v1/dead-letters")
async def get_dead_letters(source: str | None = None) -> dict[str, Any]:
    items = get_service().store.list_dead_letters(source)
    return {"count": len(items), "items": [DeadLetterView(**i).model_dump() for i in items]}


@router.post("/v1/dead-letters/{source}/{event_id}/replay")
async def post_replay(source: str, event_id: str) -> JSONResponse:
    try:
        result = get_service().replay_dead_letter(source, event_id)
    except ValidationFailure as exc:
        return _error_response(exc)
    return _result_response(result)


@router.post("/v1/admin/faults/{source}/{event_id}")
async def post_fault(
    source: str, event_id: str, fail_times: int = 3
) -> dict[str, Any]:
    get_service().inject_fault(source, event_id, fail_times)
    return {
        "injected": True,
        "source": source,
        "event_id": event_id,
        "fail_times": fail_times,
    }


@router.delete("/v1/admin/faults")
async def delete_faults() -> dict[str, Any]:
    get_service().clear_faults()
    return {"cleared": True}
