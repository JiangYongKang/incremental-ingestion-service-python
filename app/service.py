"""增量摄取核心服务。

每个来源在独立临界区内完成"读状态 -> 判定 -> 落库 -> 改状态 -> 持久化"，
从而保证：
- 同一事件重复/并发投递只生效一次（幂等账本 applied）；
- 检查点随连续生效事件单调推进，回退判定为 stale 并拒绝；
- 位点超前的事件缓冲，缺口补齐后续跑；
- 上游暂时失败可重试，超过最大次数转入死信隔离区，不阻塞后续；
- 非法事件按错误分类拒绝并隔离，不影响其他来源。
"""
from __future__ import annotations

import logging
from typing import Any

from app.errors import (
    CheckpointError,
    InvalidEventError,
    TransientFailureError,
)
from app.models import ErrorClass, Event, IngestResult, Verdict
from app.retry import RetryPolicy
from app.storage import LocalStorage, SourceState

logger = logging.getLogger("ingestion")

VALID_OPS = ("upsert", "delete")
REQUIRED_FIELDS = ("event_id", "seq", "op", "payload")


class IngestionService:
    def __init__(self, storage: LocalStorage,
                 retry_policy: RetryPolicy | None = None,
                 transient_rate: float = 0.3):
        self.storage = storage
        self.retry_policy = retry_policy or RetryPolicy()
        # 模拟上游写入的瞬时失败概率（可用 payload._fail_next 强制下一次失败）
        self.transient_rate = transient_rate

    # ------------------------------------------------------------------
    # 校验
    # ------------------------------------------------------------------

    def _validate(self, source: str, raw: Any) -> Event:
        """把原始 JSON 归一化为 Event；非法时抛出带分类的 InvalidEventError。"""
        if not isinstance(raw, dict):
            raise InvalidEventError(
                ErrorClass.INVALID_STRUCTURE, "事件体必须是 JSON 对象",
                source=source)
        for field in REQUIRED_FIELDS:
            if field not in raw or raw[field] is None:
                raise InvalidEventError(
                    ErrorClass.MISSING_FIELD, f"缺少必填字段: {field}",
                    source=source, event_id=raw.get("event_id"))
        event_id = raw["event_id"]
        seq = raw["seq"]
        op = raw["op"]
        payload = raw["payload"]
        if not isinstance(event_id, str) or not event_id:
            raise InvalidEventError(
                ErrorClass.TYPE_MISMATCH, "event_id 必须是非空字符串",
                source=source)
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise InvalidEventError(
                ErrorClass.TYPE_MISMATCH, "seq 必须是 >= 1 的整数",
                source=source, event_id=event_id)
        if not isinstance(op, str):
            raise InvalidEventError(
                ErrorClass.TYPE_MISMATCH, "op 必须是字符串",
                source=source, event_id=event_id, seq=seq)
        if op not in VALID_OPS:
            raise InvalidEventError(
                ErrorClass.INVALID_STRUCTURE,
                f"op 只能是 {VALID_OPS} 之一，收到: {op!r}",
                source=source, event_id=event_id, seq=seq)
        if not isinstance(payload, dict):
            raise InvalidEventError(
                ErrorClass.TYPE_MISMATCH, "payload 必须是对象",
                source=source, event_id=event_id, seq=seq)
        key = payload.get("key")
        if key is None:
            raise InvalidEventError(
                ErrorClass.MISSING_FIELD, "payload.key 为必填项",
                source=source, event_id=event_id, seq=seq)
        if not isinstance(key, (str, int)) or isinstance(key, bool):
            raise InvalidEventError(
                ErrorClass.TYPE_MISMATCH, "payload.key 必须是字符串或整数",
                source=source, event_id=event_id, seq=seq)
        if op == "upsert" and "value" not in payload:
            raise InvalidEventError(
                ErrorClass.MISSING_FIELD, "upsert 事件 payload.value 为必填项",
                source=source, event_id=event_id, seq=seq)
        return Event(source, seq, event_id, op, payload)

    # ------------------------------------------------------------------
    # 模拟上游：可能瞬时失败
    # ------------------------------------------------------------------

    def _upstream_apply(self, event: Event, force_fail: bool = False) -> None:
        """模拟把事件写入目标端，可能抛出可重试的瞬时异常。"""
        import random

        if force_fail or random.random() < self.transient_rate:
            raise TransientFailureError(
                ErrorClass.TRANSIENT,
                "模拟上游暂时失败（连接超时），可重试",
                source=event.source, seq=event.seq, event_id=event.event_id)
        self.storage.apply_to_target(
            event.source, event.op,
            {"key": str(event.payload["key"]),
             "value": event.payload.get("value")})

    # ------------------------------------------------------------------
    # 日志
    # ------------------------------------------------------------------

    @staticmethod
    def _log(level: int, event_id: str | None, source: str,
             seq: int | None, checkpoint: int, verdict: str, reason: str) -> None:
        logger.log(
            level,
            "event_id=%s source=%s seq=%s checkpoint=%d verdict=%s reason=%s",
            event_id, source, seq, checkpoint, verdict, reason)

    # ------------------------------------------------------------------
    # 核心投递
    # ------------------------------------------------------------------

    def ingest(self, source: str, raw: dict[str, Any]) -> IngestResult:
        try:
            event = self._validate(source, raw)
        except InvalidEventError as exc:
            return self._quarantine_invalid(source, raw, exc)

        with self.storage.transaction(source) as state:
            try:
                result = self._route(
                    state, event, bool(event.payload.get("_fail_next")))
            except InvalidEventError as exc:
                # 路由期发现位点冲突等：登记拒绝隔离后返回
                state.rejected.append({
                    "seq": exc.seq, "event_id": exc.event_id,
                    "error_class": exc.error_class.value,
                    "reason": exc.message, "raw": raw})
                self._log(logging.WARNING, event.event_id, source,
                          event.seq, state.checkpoint,
                          Verdict.REJECTED.value,
                          f"{exc.error_class.value}: {exc.message}")
                return IngestResult(
                    Verdict.REJECTED, source, event.seq, event.event_id,
                    reason=f"[{exc.error_class.value}] {exc.message}",
                    checkpoint=state.checkpoint,
                    error_class=exc.error_class)
            self._log(logging.INFO, event.event_id, source, event.seq,
                      state.checkpoint, result.verdict.value,
                      result.reason or "-")
            return result

    def _route(self, state: SourceState, event: Event,
               force_fail: bool = False) -> IngestResult:
        key = str(event.seq)

        # 1) 位点已在幂等账本中：
        #    - 同一事件重复投递 -> duplicate（幂等返回，HTTP 200，不二次写入）
        #    - 不同事件占用同一位点 -> 结构冲突，拒绝隔离
        applied_rec = state.applied.get(key)
        if applied_rec is not None:
            if applied_rec["event_id"] == event.event_id:
                return IngestResult(
                    Verdict.DUPLICATE, event.source, event.seq, event.event_id,
                    reason=(f"seq={event.seq} 已被本事件生效，幂等返回既有结果"
                            f"（checkpoint={state.checkpoint}），不二次写入"),
                    checkpoint=state.checkpoint)
            raise InvalidEventError(
                ErrorClass.INVALID_STRUCTURE,
                f"seq={event.seq} 已被事件 {applied_rec['event_id']} 生效占用，"
                f"拒绝事件 {event.event_id}（位点串用/冲突）",
                source=event.source, seq=event.seq, event_id=event.event_id)

        # 2) 位点落在检查点之前但账本无记录：检查点回退/跨来源串用，明确拒绝
        if event.seq <= state.checkpoint:
            return IngestResult(
                Verdict.STALE, event.source, event.seq, event.event_id,
                reason=(f"seq={event.seq} <= checkpoint={state.checkpoint}，"
                        f"检查点回退或跨来源串用，拒绝处理"),
                checkpoint=state.checkpoint)

        # 3) 已在缓冲或重试中：同事件重复投递，幂等返回当前状态
        bucket = state.buffered.get(key) or state.retrying.get(key)
        if bucket is not None:
            if bucket["event_id"] == event.event_id:
                verdict = (Verdict.RETRYING if key in state.retrying
                           else Verdict.BUFFERED)
                return IngestResult(
                    verdict, event.source, event.seq, event.event_id,
                    reason=f"事件已在{'重试' if verdict is Verdict.RETRYING else '缓冲'}队列，重复投递不重复登记",
                    attempts=bucket.get("attempts", 0),
                    checkpoint=state.checkpoint)
            raise InvalidEventError(
                ErrorClass.INVALID_STRUCTURE,
                f"seq={event.seq} 已被事件 {bucket['event_id']} 占用",
                source=event.source, seq=event.seq, event_id=event.event_id)

        # 4) 新事件：按位点决定立即尝试还是缓冲
        if event.seq == state.checkpoint + 1:
            return self._try_apply_head(state, event, 0, force_fail)

        # 位点超前：缓冲，等待缺口补齐
        state.buffered[key] = self._event_record(event)
        return IngestResult(
            Verdict.BUFFERED, event.source, event.seq, event.event_id,
            reason=(f"seq={event.seq} 超前于 checkpoint+1="
                    f"{state.checkpoint + 1}，缓冲等待缺口补齐"),
            checkpoint=state.checkpoint)

    def _event_record(self, event: Event, attempts: int = 0) -> dict[str, Any]:
        clean_payload = {k: v for k, v in event.payload.items()
                         if k != "_fail_next"}
        rec = {"event_id": event.event_id, "op": event.op,
               "payload": clean_payload}
        if attempts:
            rec["attempts"] = attempts
        return rec

    def _try_apply_head(self, state: SourceState, event: Event,
                        attempts_in: int = 0,
                        force_fail: bool = False) -> IngestResult:
        """尝试应用队头事件；成功则推进检查点并级联处理后续；失败则登记重试/死信。"""
        attempts = attempts_in + 1
        try:
            self._upstream_apply(event, force_fail=force_fail)
        except TransientFailureError as exc:
            return self._handle_failure(state, event, attempts, exc.message)

        # 成功：写入幂等账本、推进检查点、清掉强制失败标记
        clean_payload = {k: v for k, v in event.payload.items()
                         if k != "_fail_next"}
        state.applied[str(event.seq)] = {
            "event_id": event.event_id, "op": event.op, "payload": clean_payload}
        state.checkpoint = event.seq
        state.buffered.pop(str(event.seq), None)
        state.retrying.pop(str(event.seq), None)
        self._drain_next(state, event.source)
        return IngestResult(
            Verdict.ACCEPTED, event.source, event.seq, event.event_id,
            reason=f"首次成功应用并落库，checkpoint 推进至 {state.checkpoint}",
            attempts=attempts, checkpoint=state.checkpoint)

    def _handle_failure(self, state: SourceState, event: Event,
                        attempts: int, message: str) -> IngestResult:
        if self.retry_policy.should_retry(attempts):
            state.retrying[str(event.seq)] = self._event_record(event, attempts)
            state.buffered.pop(str(event.seq), None)
            reason = self.retry_policy.describe(event, attempts)
            return IngestResult(
                Verdict.RETRYING, event.source, event.seq, event.event_id,
                reason=f"{message}；{reason}",
                attempts=attempts, checkpoint=state.checkpoint)
        # 终止：转入死信隔离区；毒消息视为已"越过"，避免永久阻塞队列
        state.dead_letters.append({
            "seq": event.seq, "event_id": event.event_id, "op": event.op,
            "payload": event.payload, "attempts": attempts,
            "reason": f"{message}；{self.retry_policy.describe(event, attempts)}"})
        state.retrying.pop(str(event.seq), None)
        state.buffered.pop(str(event.seq), None)
        state.checkpoint = event.seq
        self._drain_next(state, event.source)
        return IngestResult(
            Verdict.DEAD_LETTER, event.source, event.seq, event.event_id,
            reason=f"重试 {attempts} 次仍失败，终止并隔离（不阻塞后续事件）",
            attempts=attempts, checkpoint=state.checkpoint)

    def _drain_next(self, state: SourceState, source: str) -> None:
        """队头生效后，按序级联应用已缓冲/重试中的后续事件。"""
        while True:
            nxt = state.checkpoint + 1
            key = str(nxt)
            rec = state.retrying.pop(key, None) or state.buffered.pop(key, None)
            if rec is None:
                return
            event = Event(source, nxt, rec["event_id"], rec["op"],
                          rec["payload"])
            result = self._try_apply_head(
                state, event, rec.get("attempts", 0))
            if result.verdict in (Verdict.RETRYING, Verdict.DEAD_LETTER):
                # 后续事件仍须保持顺序，停止级联（死信已越过，理论上不会停在这）
                if result.verdict is Verdict.RETRYING:
                    return

    # ------------------------------------------------------------------
    # 非法事件隔离
    # ------------------------------------------------------------------

    def _quarantine_invalid(self, source: str, raw: Any,
                            exc: InvalidEventError) -> IngestResult:
        with self.storage.transaction(source) as state:
            state.rejected.append({
                "seq": exc.seq, "event_id": exc.event_id,
                "error_class": exc.error_class.value,
                "reason": exc.message, "raw": raw})
            self._log(logging.WARNING, exc.event_id, source, exc.seq,
                      state.checkpoint, Verdict.REJECTED.value,
                      f"{exc.error_class.value}: {exc.message}")
            return IngestResult(
                Verdict.REJECTED, source, exc.seq, exc.event_id,
                reason=f"[{exc.error_class.value}] {exc.message}",
                checkpoint=state.checkpoint, error_class=exc.error_class)

    # ------------------------------------------------------------------
    # 检查点协议
    # ------------------------------------------------------------------

    def advance_checkpoint(self, source: str, seq: int) -> IngestResult:
        """显式检查点断言/推进：仅允许单调前进到已连续生效的位置。"""
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise InvalidEventError(ErrorClass.TYPE_MISMATCH,
                                    "seq 必须是整数", source=source)
        with self.storage.transaction(source) as state:
            if seq < state.checkpoint:
                raise CheckpointError(
                    ErrorClass.CHECKPOINT_REGRESSION,
                    f"拒绝检查点回退: {seq} < {state.checkpoint}",
                    source=source, seq=seq)
            # 只允许推进到连续已生效（或死信越过）的位点，防止跳过未处理事件
            max_ok = state.checkpoint
            probe = state.checkpoint + 1
            while str(probe) in state.applied:
                max_ok = probe
                probe += 1
            if seq > max_ok:
                raise CheckpointError(
                    ErrorClass.INVALID_STRUCTURE,
                    f"拒绝跳到 {seq}：检查点只能随事件生效推进，"
                    f"当前最远连续位点为 {max_ok}",
                    source=source, seq=seq)
            state.checkpoint = seq
            self._log(logging.INFO, None, source, seq, state.checkpoint,
                      "checkpoint_ok", "显式检查点断言通过")
            return IngestResult(
                Verdict.ACCEPTED, source, seq, None,
                reason=f"检查点确认为 {seq}", checkpoint=state.checkpoint)

    # ------------------------------------------------------------------
    # 续跑 / 状态
    # ------------------------------------------------------------------

    def retry_pending(self, source: str, force_fail: bool = False) -> list[IngestResult]:
        """续跑：重新尝试该来源所有未完成（重试中 + 缓冲）的事件。

        force_fail=True 仅用于本地演示/测试，强制本轮所有事件命中上游瞬时失败。
        """
        results: list[IngestResult] = []
        with self.storage.transaction(source) as state:
            # 从 checkpoint 之后开始，按 seq 顺序处理
            while True:
                nxt = state.checkpoint + 1
                key = str(nxt)
                rec = state.retrying.pop(key, None) or state.buffered.pop(key, None)
                if rec is None:
                    break
                event = Event(source, nxt, rec["event_id"], rec["op"],
                              rec["payload"])
                attempts = rec.get("attempts", 0)
                result = self._try_apply_head(
                    state, event, attempts, force_fail=force_fail)
                results.append(result)
                # 仍在重试：队头未越过，本轮不能继续（等待下一次续跑）；
                # 成功或死信（均已推进检查点）才继续级联后续事件。
                if result.verdict is Verdict.RETRYING:
                    break
            for result in results:
                self._log(logging.INFO, result.event_id, source, result.seq,
                          state.checkpoint, "resume_" + result.verdict.value,
                          result.reason or "-")
            return results

    def replay_dead_letter(self, source: str, seq: int) -> IngestResult:
        """显式重放一条死信事件（运维介入）：成功则补登账本。"""
        with self.storage.transaction(source) as state:
            idx = next((i for i, d in enumerate(state.dead_letters)
                        if d["seq"] == seq), None)
            if idx is None:
                raise CheckpointError(
                    ErrorClass.INVALID_STRUCTURE,
                    f"seq={seq} 不在死信隔离区", source=source, seq=seq)
            rec = state.dead_letters[idx]
            event = Event(source, rec["seq"], rec["event_id"], rec["op"],
                          rec["payload"])
            payload = dict(event.payload)
            payload.pop("_fail_next", None)
            event.payload = payload
            self.storage.apply_to_target(
                source, event.op,
                {"key": str(event.payload["key"]),
                 "value": event.payload.get("value")})
            state.applied[str(event.seq)] = self._event_record(event)
            del state.dead_letters[idx]
            self._log(logging.INFO, event.event_id, source, event.seq,
                      state.checkpoint, "dead_letter_replayed",
                      "运维重放成功并补登幂等账本")
            return IngestResult(
                Verdict.ACCEPTED, source, event.seq, event.event_id,
                reason="死信重放成功并补登幂等账本",
                checkpoint=state.checkpoint)

    def status(self, source: str) -> dict[str, Any]:
        with self.storage.transaction(source) as state:
            return {
                "source": source,
                "checkpoint": state.checkpoint,
                "applied": sorted(state.applied, key=lambda k: int(k)),
                "buffered": sorted(state.buffered, key=lambda k: int(k)),
                "retrying": sorted(
                    ({"seq": int(k), **v} for k, v in state.retrying.items()),
                    key=lambda x: x["seq"]),
                "dead_letters": state.dead_letters,
                "rejected": state.rejected,
            }

    def target_snapshot(self, source: str) -> dict[str, Any]:
        return self.storage.snapshot_target(source)
