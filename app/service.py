"""摄取服务：校验、幂等应用、位点协议、重试/死信、故障注入。

关键不变量：
* 目标写入 (LocalStore.put_record) 以 event_id 为键、在排他锁内做存在性检查，
  任何重复/并发路径都不可能产生第二条目标记录。
* 同一 (source, event_id) 在进程内由专用锁串行化；并发重复请求中第一个负责应用，
  其余得到可区分的 DUPLICATE 响应。
* 事件账本先行落盘（RETRYING / APPLIED / QUARANTINED / DEAD_LETTER），
  服务重启后未完成事件可由 /resume 或重复投递续跑，且不重复落库。
* 上游暂时失败采用“每次投递/续跑只计一次尝试”的有界重试，达到上限进死信队列，
  显式 replay 才能再次获得处理机会。
"""

from __future__ import annotations

import threading
from typing import Any

from app.errors import (
    CheckpointError,
    UnroutableEvent,
    UpstreamFailure,
    ValidationFailure,
)
from app.logging_conf import configure_logging, log_verdict
from app.models import (
    BatchItemResult,
    BatchResponse,
    BatchSummary,
    CheckpointErrorType,
    EventResult,
    EventStatus,
    ProgressView,
    RejectReason,
)
from app.store import LocalStore

MAX_ATTEMPTS = 5


def _is_plain_int(value: Any) -> bool:
    """bool 不是合法的业务整数。"""
    return isinstance(value, int) and not isinstance(value, bool)


class IngestionService:
    def __init__(self, store: LocalStore, max_attempts: int = MAX_ATTEMPTS):
        self.store = store
        self.max_attempts = max_attempts
        self._inflight: dict[tuple[str, str], threading.Lock] = {}
        self._inflight_guard = threading.Lock()
        self._faults: dict[tuple[str, str], int] = {}
        self._fault_guard = threading.Lock()
        self.log = configure_logging()

    # ------------------------------------------------------------------ #
    # 并发控制与故障注入
    # ------------------------------------------------------------------ #
    def _get_inflight_lock(self, source: str, event_id: str) -> threading.Lock:
        key = (source, event_id)
        with self._inflight_guard:
            lock = self._inflight.get(key)
            if lock is None:
                lock = threading.Lock()
                self._inflight[key] = lock
            return lock

    def inject_fault(self, source: str, event_id: str, fail_times: int) -> None:
        with self._fault_guard:
            self._faults[(source, event_id)] = max(0, int(fail_times))

    def clear_faults(self) -> None:
        with self._fault_guard:
            self._faults.clear()

    # ------------------------------------------------------------------ #
    # 入站校验（可区分原因）
    # ------------------------------------------------------------------ #
    def _validate(self, raw: Any, url_source: str) -> tuple[str, str, int, dict[str, Any]]:
        """校验原始 JSON，返回 (event_id, source, seq, payload)。

        无法标识事件的错误（非对象 / 无 event_id）直接抛出，由接口拒绝；
        可标识的坏事件抛出 ValidationFailure，由 ingest 隔离。
        """
        if not isinstance(raw, dict):
            raise UnroutableEvent(
                "事件体必须是 JSON 对象",
                RejectReason.MALFORMED_STRUCTURE,
            )

        event_id = raw.get("event_id")
        if event_id is None or (isinstance(event_id, str) and not event_id.strip()):
            raise UnroutableEvent(
                "缺少必填字段 event_id",
                RejectReason.MISSING_EVENT_ID,
            )
        if not isinstance(event_id, str):
            raise UnroutableEvent(
                "event_id 必须是字符串",
                RejectReason.TYPE_MISMATCH,
            )

        if "source" not in raw or raw.get("source") is None:
            raise ValidationFailure(
                "缺少必填字段 source",
                RejectReason.MISSING_FIELD,
                event_id=event_id,
            )
        source = raw["source"]
        if not isinstance(source, str) or not source.strip():
            raise ValidationFailure(
                "source 必须是非空字符串",
                RejectReason.TYPE_MISMATCH,
                event_id=event_id,
            )
        if source != url_source:
            # 跨来源串用：拒绝，且不落入当前来源
            raise CrossSourceError(source, url_source, event_id)

        if "seq" not in raw or raw.get("seq") is None:
            raise ValidationFailure(
                "缺少必填字段 seq",
                RejectReason.MISSING_FIELD,
                event_id=event_id,
            )
        seq = raw["seq"]
        if not _is_plain_int(seq):
            raise ValidationFailure(
                "seq 必须是整数",
                RejectReason.TYPE_MISMATCH,
                event_id=event_id,
            )
        if seq <= 0:
            raise ValidationFailure(
                "seq 必须是正整数",
                RejectReason.MALFORMED_STRUCTURE,
                event_id=event_id,
            )

        if "payload" not in raw or raw.get("payload") is None:
            raise ValidationFailure(
                "缺少必填字段 payload",
                RejectReason.MISSING_FIELD,
                event_id=event_id,
            )
        payload = raw["payload"]
        if not isinstance(payload, dict):
            raise ValidationFailure(
                "payload 必须是 JSON 对象",
                RejectReason.TYPE_MISMATCH,
                event_id=event_id,
            )
        if not all(isinstance(k, str) for k in payload):
            raise ValidationFailure(
                "payload 的键必须是字符串",
                RejectReason.MALFORMED_STRUCTURE,
                event_id=event_id,
            )

        return event_id, source, seq, payload

    # ------------------------------------------------------------------ #
    # 模拟上游目标端写入（故障注入点）
    # ------------------------------------------------------------------ #
    def _upstream_apply(
        self, source: str, event_id: str, seq: int, payload: dict[str, Any]
    ) -> bool:
        key = (source, event_id)
        with self._fault_guard:
            remaining = self._faults.get(key, 0)
            if remaining > 0:
                self._faults[key] = remaining - 1
                raise UpstreamFailure(
                    f"目标端暂时不可用（注入故障剩余 {remaining - 1} 次）"
                )
        record = {
            "event_id": event_id,
            "source": source,
            "seq": seq,
            "payload": payload,
        }
        # put_record 自身幂等：已存在则不覆盖，返回 False（崩溃后半程自愈场景）
        return self.store.put_record(source, event_id, record)

    # ------------------------------------------------------------------ #
    # 账本辅助
    # ------------------------------------------------------------------ #
    def _ledger(
        self,
        *,
        event_id: str,
        source: str,
        seq: int | None,
        status: EventStatus,
        attempts: int,
        reason: RejectReason | None = None,
        detail: str | None = None,
        payload_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        doc = {
            "event_id": event_id,
            "source": source,
            "seq": seq,
            "status": status.value,
            "attempts": attempts,
            "reason": reason.value if reason else None,
            "detail": detail,
        }
        if payload_snapshot is not None:
            doc["payload_snapshot"] = payload_snapshot
        return doc

    def _result(
        self,
        *,
        event_id: str,
        source: str,
        seq: int | None,
        status: EventStatus,
        attempts: int = 0,
        reason: RejectReason | None = None,
        detail: str | None = None,
        duplicate_of: str | None = None,
    ) -> EventResult:
        return EventResult(
            event_id=event_id,
            source=source,
            seq=seq,
            status=status,
            reason=reason,
            attempts=attempts,
            detail=detail,
            duplicate_of=duplicate_of,
        )

    def _log(self, result: EventResult) -> None:
        log_verdict(
            self.log,
            event_id=result.event_id,
            source=result.source,
            seq=result.seq,
            checkpoint=self.store.get_checkpoint(result.source),
            verdict=result.status.value,
            reason=result.reason.value if result.reason else None,
            attempts=result.attempts,
            message=result.detail or "",
        )

    # ------------------------------------------------------------------ #
    # 隔离
    # ------------------------------------------------------------------ #
    def _quarantine(
        self,
        exc: ValidationFailure,
        source: str,
        seq: int | None,
    ) -> EventResult:
        event_id = exc.event_id or "unknown"
        doc = self._ledger(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.QUARANTINED,
            attempts=0,
            reason=exc.reason,
            detail=exc.message,
        )
        self.store.save_event(source, event_id, doc)
        self.store.add_quarantine(source, doc)
        if _is_plain_int(seq):
            # 坏事件也推进位点，避免同来源后续事件被卡住
            self.store.advance_checkpoint(source, seq)
        return self._result(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.QUARANTINED,
            reason=exc.reason,
            detail=exc.message,
        )

    # ------------------------------------------------------------------ #
    # 核心入口
    # ------------------------------------------------------------------ #
    def ingest(self, raw_event: Any, url_source: str) -> EventResult:
        # 1) 先校验。可标识的坏事件进隔离区；不可标识的直接抛出由接口拒绝。
        try:
            event_id, source, seq, payload = self._validate(raw_event, url_source)
        except CrossSourceError:
            # 跨来源串用必须直接拒绝，不隔离、不写账本
            raise
        except ValidationFailure as exc:
            if exc.event_id is None:
                raise
            body_source = url_source
            if isinstance(raw_event, dict) and isinstance(raw_event.get("source"), str):
                body_source = raw_event["source"]
            # 畸形重投的“查账本-决定-隔离”与正常路径在同一把来源锁内串行，
            # 避免跨进程并发时同一事件一边被应用、一边被隔离
            with self.store.transaction(body_source):
                # 已被处理过的事件即使再次以畸形形态投递，也维持原判定，不重复隔离
                if isinstance(raw_event, dict) and isinstance(exc.event_id, str):
                    existing = self.store.get_event(body_source, exc.event_id)
                    if existing is not None:
                        known = EventStatus(existing["status"])
                        if known in (
                            EventStatus.APPLIED,
                            EventStatus.QUARANTINED,
                            EventStatus.DEAD_LETTER,
                        ):
                            return self._handle_existing(existing, {})
                        if known is EventStatus.RETRYING:
                            # 未完成事件的畸形重投不影响其续跑：用首次快照继续
                            snapshot = existing.get("payload_snapshot") or {}
                            return self._handle_existing(existing, snapshot)
                seq_value = None
                if isinstance(raw_event, dict) and _is_plain_int(raw_event.get("seq")):
                    seq_value = raw_event["seq"]
                result = self._quarantine(exc, body_source, seq_value)
            self._log(result)
            return result

        # 2) 同一事件的重复/并发投递在此串行（进程内每键锁 + 跨进程来源事务）
        lock = self._get_inflight_lock(source, event_id)
        with lock:
            with self.store.transaction(source):
                existing = self.store.get_event(source, event_id)
                if existing is not None:
                    return self._handle_existing(existing, payload)
                return self._apply_once(event_id, source, seq, payload, attempts=0, new=True)

    def _handle_existing(
        self, existing: dict[str, Any], payload: dict[str, Any]
    ) -> EventResult:
        status = EventStatus(existing["status"])
        seq = existing.get("seq")
        source = existing["source"]
        event_id = existing["event_id"]
        attempts = int(existing.get("attempts", 0))

        if status is EventStatus.APPLIED:
            result = self._result(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.DUPLICATE,
                attempts=attempts,
                detail="重复投递：事件此前已应用，未二次写入，结果保持不变",
                duplicate_of=event_id,
            )
            self._log(result)
            return result

        if status is EventStatus.QUARANTINED:
            # 坏事件重复投递：维持原判定，不重复隔离
            reason = RejectReason(existing["reason"]) if existing.get("reason") else None
            result = self._result(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.DUPLICATE,
                reason=reason,
                detail=f"重复投递的坏事件，维持隔离判定：{existing.get('detail')}",
                duplicate_of=event_id,
            )
            self._log(result)
            return result

        if status is EventStatus.DEAD_LETTER:
            reason = RejectReason(existing["reason"]) if existing.get("reason") else None
            result = self._result(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.DEAD_LETTER,
                attempts=attempts,
                reason=reason,
                detail="重试已终止，事件在死信队列中，需显式 replay",
            )
            self._log(result)
            return result

        # RETRYING：未完成事件的重复投递/重启后续跑，继续计数尝试
        return self._apply_once(event_id, source, seq, payload, attempts=attempts, new=False)

    def _apply_once(
        self,
        event_id: str,
        source: str,
        seq: int,
        payload: dict[str, Any],
        *,
        attempts: int,
        new: bool,
    ) -> EventResult:
        attempts += 1
        try:
            first_write = self._upstream_apply(source, event_id, seq, payload)
        except UpstreamFailure as exc:
            if attempts >= self.max_attempts:
                doc = self._ledger(
                    event_id=event_id,
                    source=source,
                    seq=seq,
                    status=EventStatus.DEAD_LETTER,
                    attempts=attempts,
                    reason=RejectReason.RETRY_EXHAUSTED,
                    payload_snapshot=payload,
                    detail=f"已重试 {attempts} 次仍失败，终止并进入死信队列：{exc.message}",
                )
                self.store.save_event(source, event_id, doc)
                self.store.add_dead_letter(source, doc)
                result = self._result(
                    event_id=event_id,
                    source=source,
                    seq=seq,
                    status=EventStatus.DEAD_LETTER,
                    attempts=attempts,
                    reason=RejectReason.RETRY_EXHAUSTED,
                    detail=doc["detail"],
                )
            else:
                doc = self._ledger(
                    event_id=event_id,
                    source=source,
                    seq=seq,
                    status=EventStatus.RETRYING,
                    attempts=attempts,
                    payload_snapshot=payload,
                    detail=(
                        f"第 {attempts}/{self.max_attempts} 次尝试暂时失败：{exc.message}"
                    ),
                )
                self.store.save_event(source, event_id, doc)
                result = self._result(
                    event_id=event_id,
                    source=source,
                    seq=seq,
                    status=EventStatus.RETRYING,
                    attempts=attempts,
                    detail=f"{doc['detail']}；可重新投递或调用 /resume 续跑",
                )
            self._log(result)
            return result

        doc = self._ledger(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.APPLIED,
            attempts=attempts,
            detail=(
                "首次应用成功"
                if first_write and new
                else "应用成功（目标记录已存在，按崩溃恢复路径确认，未二次写入）"
                if not first_write
                else "续跑后应用成功"
            ),
        )
        self.store.save_event(source, event_id, doc)
        self.store.advance_checkpoint(source, seq)
        result = self._result(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.APPLIED,
            attempts=attempts,
            detail=doc["detail"],
        )
        self._log(result)
        return result

    # ------------------------------------------------------------------ #
    # 批量投递
    # ------------------------------------------------------------------ #
    def ingest_batch(self, raw_batch: Any, url_source: str) -> BatchResponse:
        """按批处理同一来源的事件：逐条走与单条投递完全相同的判定路径。

        个别事件重复/畸形/暂时失败不影响同批其他事件，整批请求不因此失败；
        只有批次信封本身非法（不是 JSON 数组）才整体拒绝。
        """
        if isinstance(raw_batch, dict) and isinstance(raw_batch.get("events"), list):
            items = raw_batch["events"]
        elif isinstance(raw_batch, list):
            items = raw_batch
        else:
            raise UnroutableEvent(
                "批量投递的请求体必须是 JSON 数组，或形如 {\"events\": [...]} 的对象",
                RejectReason.MALFORMED_STRUCTURE,
            )

        results: list[BatchItemResult] = []
        for index, item in enumerate(items):
            results.append(self._ingest_batch_item(index, item, url_source))

        summary = BatchSummary(total=len(results))
        for r in results:
            key = r.status.value.lower()
            setattr(summary, key, getattr(summary, key) + 1)
        self.log.info(
            "batch completed",
            extra={
                "event_id": "-",
                "source": url_source,
                "seq": "-",
                "checkpoint": self.store.get_checkpoint(url_source),
                "verdict": "BATCH_DONE",
                "reason": "-",
                "attempts": 0,
            },
        )
        return BatchResponse(source=url_source, summary=summary, results=results)

    def _ingest_batch_item(
        self, index: int, item: Any, url_source: str
    ) -> BatchItemResult:
        """单条目处理：复用 ingest 的全部语义，把异常折叠成条目级 REJECTED。"""
        try:
            result = self.ingest(item, url_source)
            return BatchItemResult(
                index=index,
                event_id=result.event_id,
                source=result.source,
                seq=result.seq,
                status=result.status,
                reason=result.reason,
                attempts=result.attempts,
                detail=result.detail,
                duplicate_of=result.duplicate_of,
            )
        except CrossSourceError as exc:
            return self._rejected_item(
                index, item, url_source, "SOURCE_MISMATCH", exc.message
            )
        except UnroutableEvent as exc:
            reason = exc.reason.value if isinstance(exc.reason, RejectReason) else str(exc.reason)
            return self._rejected_item(index, item, url_source, reason, exc.message)
        except ValidationFailure as exc:
            reason = exc.reason.value if isinstance(exc.reason, RejectReason) else str(exc.reason)
            return self._rejected_item(index, item, url_source, reason, exc.message)

    def _rejected_item(
        self,
        index: int,
        item: Any,
        url_source: str,
        reason: str,
        message: str,
    ) -> BatchItemResult:
        event_id = item.get("event_id") if isinstance(item, dict) else None
        if not isinstance(event_id, str):
            event_id = None
        seq = item.get("seq") if isinstance(item, dict) else None
        if not _is_plain_int(seq):
            seq = None
        log_verdict(
            self.log,
            event_id=event_id or "-",
            source=url_source,
            seq=seq,
            checkpoint=self.store.get_checkpoint(url_source),
            verdict=EventStatus.REJECTED.value,
            reason=reason,
            attempts=0,
            message=message,
        )
        return BatchItemResult(
            index=index,
            event_id=event_id,
            source=url_source,
            seq=seq,
            status=EventStatus.REJECTED,
            reason=reason,
            detail=message,
        )

    # ------------------------------------------------------------------ #
    # 进度对账
    # ------------------------------------------------------------------ #
    def progress(self, source: str) -> ProgressView:
        snap = self.store.progress_snapshot(source)
        view = ProgressView(source=source, **snap)
        self.log.info(
            "progress snapshot",
            extra={
                "event_id": "-",
                "source": source,
                "seq": "-",
                "checkpoint": view.last_seq,
                "verdict": "PROGRESS",
                "reason": "-",
                "attempts": 0,
            },
        )
        return view

    # ------------------------------------------------------------------ #
    # 位点协议
    # ------------------------------------------------------------------ #
    def commit_checkpoint(self, source: str, seq: int) -> int:
        if not _is_plain_int(seq) or seq < 0:
            raise ValidationFailure(
                "seq 必须是非负整数",
                RejectReason.TYPE_MISMATCH,
            )
        current = self.store.get_checkpoint(source)
        if seq < current:
            raise CheckpointError(
                f"拒绝位点回退：来源 {source} 当前位点 {current}，收到 {seq}",
                CheckpointErrorType.ROLLBACK,
            )
        new_value = self.store.advance_checkpoint(source, seq)
        self.log.info(
            "checkpoint committed",
            extra={
                "event_id": "-",
                "source": source,
                "seq": "-",
                "checkpoint": new_value,
                "verdict": "CHECKPOINT_ADVANCE",
                "reason": "-",
                "attempts": 0,
            },
        )
        return new_value

    # ------------------------------------------------------------------ #
    # 续跑
    # ------------------------------------------------------------------ #
    def resume(self, source: str) -> list[EventResult]:
        results: list[EventResult] = []
        for doc in self.store.list_events(source):
            if doc.get("status") != EventStatus.RETRYING.value:
                continue
            event_id = doc["event_id"]
            lock = self._get_inflight_lock(source, event_id)
            with lock:
                with self.store.transaction(source):
                    fresh = self.store.get_event(source, event_id)
                    if fresh is None or fresh.get("status") != EventStatus.RETRYING.value:
                        continue
                    # 续跑内循环直到终态（成功或死信）
                    while True:
                        result = self._apply_once(
                            event_id,
                            source,
                            int(fresh["seq"]),
                            self._payload_of(source, event_id),
                            attempts=int(fresh.get("attempts", 0)),
                            new=False,
                        )
                        results.append(result)
                        if result.status in (
                            EventStatus.APPLIED,
                            EventStatus.DEAD_LETTER,
                        ):
                            break
                        fresh = self.store.get_event(source, event_id)
        return results

    def _payload_of(self, source: str, event_id: str) -> dict[str, Any]:
        # RETRYING 事件尚未落库，其 payload 在首次失败时已快照进账本
        record = self.store.get_record(source, event_id)
        if record is not None:
            return record["payload"]
        doc = self.store.get_event(source, event_id) or {}
        return doc.get("payload_snapshot") or {}

    # ------------------------------------------------------------------ #
    # 死信重放
    # ------------------------------------------------------------------ #
    def replay_dead_letter(self, source: str, event_id: str) -> EventResult:
        existing = self.store.get_event(source, event_id)
        if existing is None or existing.get("status") != EventStatus.DEAD_LETTER.value:
            raise ValidationFailure(
                f"事件 {event_id} 不在死信队列",
                RejectReason.MALFORMED_STRUCTURE,
                event_id=event_id,
            )
        with self._get_inflight_lock(source, event_id):
            with self.store.transaction(source):
                self.store.remove_dead_letter(source, event_id)
                seq = int(existing["seq"])
                payload = existing.get("payload_snapshot") or {}
                # 显式重放给予一轮全新尝试机会
                result = self._apply_once(
                    event_id, source, seq, payload, attempts=0, new=False
                )
                return result


class CrossSourceError(ValidationFailure):
    """事件体内来源与投递来源不一致（跨来源串用）。"""

    http_status = 400
    code = "SOURCE_MISMATCH"

    def __init__(self, body_source: str, url_source: str, event_id: str):
        super().__init__(
            f"事件来源 {body_source!r} 与投递来源 {url_source!r} 不一致，拒绝跨来源串用",
            RejectReason.MALFORMED_STRUCTURE,
            event_id=event_id,
        )
        self.body_source = body_source
        self.url_source = url_source
