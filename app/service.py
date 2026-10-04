"""摄取服务：校验、幂等应用、位点协议、重试/死信、故障注入，以及批量投递与进度对账。

关键不变量：
* 目标写入 (LocalStore.commit_apply/put_record) 以 event_id 为键、在排他锁内做存在性
  检查，任何重复/并发路径都不可能产生第二条目标记录。
* 同一 (source, event_id) 在进程内由专用锁、在进程间由文件锁（store.event_lock）双重
  串行化：多进程同时投递重叠批次时，只有一个负责应用，其余得到可区分的 DUPLICATE。
* “落库 + 账本 + 位点/隔离/死信”由 store 在同一把排他锁内作为一次复合原子提交落盘，
  并发读取进度快照不会看到自相矛盾的中间态。
* 事件账本先行落盘（RETRYING / APPLIED / QUARANTINED / DEAD_LETTER），服务重启后
  未完成事件可由 /resume 或重复投递续跑，且不重复落库。
* 上游暂时失败采用“每次投递/续跑只计一次尝试”的有界重试，达到上限进死信队列，
  显式 replay 才能再次获得处理机会。
* 批量入口对每个位置独立判定：单条重复/畸形/暂时失败不连累同批其他事件，整批请求
  不整体失败；同一批内重复的事件标识最终也只有一条生效。
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.errors import (
    CheckpointError,
    UnroutableEvent,
    UpstreamFailure,
    ValidationFailure,
)
from app.logging_conf import configure_logging, log_verdict
from app.models import (
    BatchIngestResponse,
    BatchItemResult,
    BatchSummary,
    CheckpointErrorType,
    EventResult,
    EventStatus,
    RejectReason,
    SourceProgress,
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

        无法标识事件的错误（非对象 / 无 event_id）直接抛出，由接口拒绝（批量中表现为
        该位置 REJECTED）；可标识的坏事件抛出 ValidationFailure，由 ingest 隔离。
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
    # 模拟上游目标端写入（故障注入点：只负责决定本次是否抛暂时失败）
    # ------------------------------------------------------------------ #
    def _consume_fault(self, source: str, event_id: str) -> None:
        key = (source, event_id)
        with self._fault_guard:
            remaining = self._faults.get(key, 0)
            if remaining > 0:
                self._faults[key] = remaining - 1
                raise UpstreamFailure(
                    f"目标端暂时不可用（注入故障剩余 {remaining - 1} 次）"
                )

    # ------------------------------------------------------------------ #
    # 账本/结果辅助
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
        duplicate_hits: int = 0,
    ) -> dict[str, Any]:
        doc = {
            "event_id": event_id,
            "source": source,
            "seq": seq,
            "status": status.value,
            "attempts": attempts,
            "reason": reason.value if reason else None,
            "detail": detail,
            "duplicate_hits": duplicate_hits,
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

    def _bump_duplicate_hits(self, source: str, event_id: str) -> int:
        """对已终态事件的每一次重复投递计数 +1，返回新计数（调用方已持事件锁）。"""
        doc = self.store.get_event(source, event_id)
        if doc is None:
            return 0
        hits = int(doc.get("duplicate_hits", 0)) + 1
        doc["duplicate_hits"] = hits
        self.store.commit_ledger_only(source, event_id, doc)
        return hits

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
        # 账本 + 隔离区 + 位点 同一把锁内提交，坏事件也推进位点，避免卡住后续事件
        self.store.commit_quarantine(source, event_id, doc, seq)
        return self._result(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.QUARANTINED,
            reason=exc.reason,
            detail=exc.message,
        )

    # ------------------------------------------------------------------ #
    # 核心入口（单条，旧接口行为保持不变）
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
            seq_value = None
            if isinstance(raw_event, dict) and _is_plain_int(raw_event.get("seq")):
                seq_value = raw_event["seq"]
            # 同一事件的并发/重复畸形投递在跨进程事件锁内串行，维持原判定
            with self.store.event_lock(body_source, exc.event_id):
                with self._get_inflight_lock(body_source, exc.event_id):
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
                    result = self._quarantine(exc, body_source, seq_value)
            self._log(result)
            return result

        # 2) 同一事件的重复/并发投递在此串行（进程内锁 + 跨进程文件锁）
        with self.store.event_lock(source, event_id):
            with self._get_inflight_lock(source, event_id):
                existing = self.store.get_event(source, event_id)
                if existing is not None:
                    return self._handle_existing(existing, payload)
                return self._apply_once(
                    event_id, source, seq, payload, attempts=0, new=True
                )

    def _handle_existing(
        self, existing: dict[str, Any], payload: dict[str, Any]
    ) -> EventResult:
        status = EventStatus(existing["status"])
        seq = existing.get("seq")
        source = existing["source"]
        event_id = existing["event_id"]
        attempts = int(existing.get("attempts", 0))

        if status is EventStatus.APPLIED:
            hits = self._bump_duplicate_hits(source, event_id)
            result = self._result(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.DUPLICATE,
                attempts=attempts,
                detail=(
                    "重复投递：事件此前已应用，未二次写入，结果保持不变"
                    f"（累计第 {hits} 次重复）"
                ),
                duplicate_of=event_id,
            )
            self._log(result)
            return result

        if status is EventStatus.QUARANTINED:
            # 坏事件重复投递：维持原判定，不重复隔离
            hits = self._bump_duplicate_hits(source, event_id)
            reason = RejectReason(existing["reason"]) if existing.get("reason") else None
            result = self._result(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.DUPLICATE,
                reason=reason,
                detail=(
                    f"重复投递的坏事件，维持隔离判定：{existing.get('detail')}"
                    f"（累计第 {hits} 次重复）"
                ),
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
        remove_dead_letter: bool = False,
    ) -> EventResult:
        attempts += 1
        try:
            self._consume_fault(source, event_id)
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
                self.store.commit_dead_letter(source, event_id, doc)
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
                self.store.commit_ledger_only(
                    source, event_id, doc, remove_dead_letter=remove_dead_letter
                )
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

        record = {
            "event_id": event_id,
            "source": source,
            "seq": seq,
            "payload": payload,
        }

        def _doc(first_write: bool) -> dict[str, Any]:
            if first_write and new:
                detail = "首次应用成功"
            elif not first_write:
                detail = "应用成功（目标记录已存在，按崩溃恢复路径确认，未二次写入）"
            else:
                detail = "续跑后应用成功"
            return self._ledger(
                event_id=event_id,
                source=source,
                seq=seq,
                status=EventStatus.APPLIED,
                attempts=attempts,
                detail=detail,
            )

        # 落库 + 账本 + 位点（+ 重放时移死信）一次原子复合提交
        first_write = self.store.commit_apply(
            source,
            event_id,
            record,
            _doc,
            seq,
            remove_dead_letter=remove_dead_letter,
        )
        if first_write and new:
            detail = "首次应用成功"
        elif not first_write:
            detail = "应用成功（目标记录已存在，按崩溃恢复路径确认，未二次写入）"
        else:
            detail = "续跑后应用成功"
        result = self._result(
            event_id=event_id,
            source=source,
            seq=seq,
            status=EventStatus.APPLIED,
            attempts=attempts,
            detail=detail,
        )
        self._log(result)
        return result

    # ------------------------------------------------------------------ #
    # 批量投递
    # ------------------------------------------------------------------ #
    def ingest_batch(self, raw_batch: Any, url_source: str) -> BatchIngestResponse:
        """把同一来源的一批事件逐条交给单条摄取逻辑。

        每条事件独立判定、互不连累：某条重复/畸形/暂时失败都不会让整批失败。
        返回结果与请求数组下标一一对应；同批内重复 event_id 只有第一条真正生效，
        其余得到 DUPLICATE。
        """
        if not isinstance(raw_batch, dict) or "events" not in raw_batch:
            raise UnroutableEvent(
                "批量请求体必须是包含 events 数组的 JSON 对象",
                RejectReason.BATCH_MALFORMED,
            )
        items = raw_batch.get("events")
        if not isinstance(items, list):
            raise UnroutableEvent(
                "events 必须是 JSON 数组",
                RejectReason.BATCH_MALFORMED,
            )

        results: list[BatchItemResult] = []
        counters = {
            EventStatus.APPLIED: 0,
            EventStatus.DUPLICATE: 0,
            EventStatus.QUARANTINED: 0,
            EventStatus.RETRYING: 0,
            EventStatus.DEAD_LETTER: 0,
            EventStatus.REJECTED: 0,
        }

        for index, raw in enumerate(items):
            item = self._ingest_one_for_batch(raw, url_source, index)
            results.append(item)
            counters[item.status] += 1

        summary = BatchSummary(
            received=len(items),
            applied=counters[EventStatus.APPLIED],
            duplicate=counters[EventStatus.DUPLICATE],
            quarantined=counters[EventStatus.QUARANTINED],
            retrying=counters[EventStatus.RETRYING],
            dead_letter=counters[EventStatus.DEAD_LETTER],
            rejected=counters[EventStatus.REJECTED],
        )
        return BatchIngestResponse(source=url_source, summary=summary, results=results)

    def _ingest_one_for_batch(
        self, raw: Any, url_source: str, index: int
    ) -> BatchItemResult:
        """批量中单条处理：任何异常都收敛为该位置的 REJECTED，绝不抛出整批。"""
        try:
            result = self.ingest(raw, url_source)
            return self._to_batch_item(index, result)
        except CrossSourceError as exc:
            return self._rejected_item(
                index,
                url_source,
                event_id=exc.event_id if isinstance(exc, ValidationFailure) else None,
                reason=RejectReason.SOURCE_MISMATCH,
                detail=exc.message,
                raw=raw,
            )
        except (UnroutableEvent, ValidationFailure) as exc:
            event_id = None
            if isinstance(exc, ValidationFailure):
                event_id = exc.event_id
            elif isinstance(raw, dict) and isinstance(raw.get("event_id"), str):
                event_id = raw["event_id"]
            return self._rejected_item(
                index,
                url_source,
                event_id=event_id,
                reason=exc.reason,
                detail=exc.message,
                raw=raw,
            )
        except Exception as exc:  # 防御性兜底：保证整批绝不因单条意外而失败
            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            item = self._rejected_item(
                index,
                url_source,
                event_id=event_id if isinstance(event_id, str) else None,
                reason=RejectReason.PROCESSING_ERROR,
                detail=f"单条处理发生意外错误，已隔离出该条：{exc!r}",
                raw=raw,
            )
            self.log.error(
                "batch item processing error",
                extra={
                    "event_id": item.event_id or "-",
                    "source": url_source,
                    "seq": item.seq if item.seq is not None else "-",
                    "checkpoint": self.store.get_checkpoint(url_source),
                    "verdict": EventStatus.REJECTED.value,
                    "reason": RejectReason.PROCESSING_ERROR.value,
                    "attempts": 0,
                },
            )
            return item

    def _to_batch_item(self, index: int, r: EventResult) -> BatchItemResult:
        return BatchItemResult(
            index=index,
            event_id=r.event_id,
            source=r.source,
            seq=r.seq,
            status=r.status,
            reason=r.reason,
            attempts=r.attempts,
            detail=r.detail,
            duplicate_of=r.duplicate_of,
        )

    def _rejected_item(
        self,
        index: int,
        url_source: str,
        *,
        event_id: str | None,
        reason: RejectReason | None,
        detail: str | None,
        raw: Any,
    ) -> BatchItemResult:
        seq = None
        if isinstance(raw, dict) and _is_plain_int(raw.get("seq")):
            seq = raw["seq"]
        return BatchItemResult(
            index=index,
            event_id=event_id,
            source=url_source,
            seq=seq,
            status=EventStatus.REJECTED,
            reason=reason,
            attempts=0,
            detail=detail,
        )

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
            with self.store.event_lock(source, event_id):
                with self._get_inflight_lock(source, event_id):
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
        with self.store.event_lock(source, event_id):
            with self._get_inflight_lock(source, event_id):
                existing = self.store.get_event(source, event_id)
                if (
                    existing is None
                    or existing.get("status") != EventStatus.DEAD_LETTER.value
                ):
                    raise ValidationFailure(
                        f"事件 {event_id} 不在死信队列",
                        RejectReason.MALFORMED_STRUCTURE,
                        event_id=event_id,
                    )
                seq = int(existing["seq"])
                payload = existing.get("payload_snapshot") or {}
                # 显式重放给予一轮全新尝试机会；死信移除并入复合提交，保持快照一致
                result = self._apply_once(
                    event_id,
                    source,
                    seq,
                    payload,
                    attempts=0,
                    new=False,
                    remove_dead_letter=True,
                )
                return result

    # ------------------------------------------------------------------ #
    # 来源级进度对账
    # ------------------------------------------------------------------ #
    def source_progress(self, source: str) -> SourceProgress:
        """读取来源级一致性快照并计数。

        snapshot 在 store 的同一把排他锁内读取全部相关文件，因此计数彼此对得上：
        applied_count == 目标库记录数；quarantined/dead_letter 与隔离区、死信队列一致；
        ledger_total == applied + quarantined + dead_letter + pending。
        """
        snap = self.store.source_snapshot(source)
        events: dict[str, dict[str, Any]] = snap["events"]
        applied = quarantined = dead_letter = pending = 0
        duplicate_hits = 0
        for doc in events.values():
            status = doc.get("status")
            duplicate_hits += int(doc.get("duplicate_hits", 0))
            if status == EventStatus.APPLIED.value:
                applied += 1
            elif status == EventStatus.QUARANTINED.value:
                quarantined += 1
            elif status == EventStatus.DEAD_LETTER.value:
                dead_letter += 1
            elif status == EventStatus.RETRYING.value:
                pending += 1

        # 以落库记录数为准对账；正常运行下二者必然相等（复合原子提交保证）
        record_count = len(snap["records"])
        if record_count != applied:
            # 仅可能出现在“落库后、账本更新前”的极端崩溃窗口，如实暴露不自洽
            self.log.warning(
                "progress snapshot: applied ledger/records mismatch",
                extra={
                    "event_id": "-",
                    "source": source,
                    "seq": "-",
                    "checkpoint": snap["last_seq"],
                    "verdict": "PROGRESS_MISMATCH",
                    "reason": f"ledger={applied} records={record_count}",
                    "attempts": 0,
                },
            )
            applied = record_count

        return SourceProgress(
            source=source,
            last_seq=snap["last_seq"],
            applied_count=record_count,
            duplicate_count=duplicate_hits,
            quarantined_count=len(snap["quarantine"]),
            dead_letter_count=len(snap["dead_letters"]),
            pending_count=pending,
            ledger_total=len(events),
            checked_at=datetime.now(timezone.utc).isoformat(),
        )


class CrossSourceError(ValidationFailure):
    """事件体内来源与投递来源不一致（跨来源串用）。"""

    http_status = 400
    code = "SOURCE_MISMATCH"

    def __init__(self, body_source: str, url_source: str, event_id: str):
        super().__init__(
            f"事件来源 {body_source!r} 与投递来源 {url_source!r} 不一致，拒绝跨来源串用",
            RejectReason.SOURCE_MISMATCH,
            event_id=event_id,
        )
        self.body_source = body_source
        self.url_source = url_source
