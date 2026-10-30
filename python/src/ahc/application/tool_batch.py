"""Bounded read-only batches for one admitted session; no durable replay claim."""

import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from ahc.domain.capability_values import DecisionKind, EffectClass
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier


class BatchRejected(ContractError):
    """Port guarantees this call was rejected before dispatch."""


@dataclass(frozen=True)
class BatchCall:
    call_id: str
    capability_id: str
    resource_id: str
    arguments_json: str

    def __post_init__(self):
        for value in (self.call_id, self.capability_id, self.resource_id):
            _identifier(value, "batch identifier")
        if type(self.arguments_json) is not str or len(self.arguments_json.encode()) > 16384:
            raise ContractError("batch arguments invalid")
        try:
            value = json.loads(self.arguments_json)
            json.dumps(value, allow_nan=False)
        except (ValueError, TypeError, RecursionError):
            raise ContractError("batch arguments invalid") from None
        if type(value) is not dict:
            raise ContractError("batch arguments must be an object")


@dataclass(frozen=True)
class BatchResult:
    receipts: tuple
    values: tuple

    def summary(self):
        """Model-safe summary; values remain available to local reduction code."""
        return {"receipts": [dict(row) for row in self.receipts]}


class ReadOnlyBatch:
    """Composition must bind view and port to this exact admitted session.

    Port.invoke(session, call, deadline, cancelled) owns typed argument validation
    and cooperative timeout. No hard interruption, process isolation or restart
    replay protection is claimed by this local coordinator.
    Parallel mode requires a concurrent_reads port and thread-safe view, clock
    and cancellation callback. Return waits for all admitted worker calls to end.
    """

    def __init__(self, session, view, port, *, clock=time.monotonic):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("batch session invalid")
        self.session, self.view, self.port, self.clock = session, view, port, clock
        self._used = set()
        self._lock = threading.Lock()

    def run(self, expected, calls, *, deadline, cancelled=lambda: False, concurrency=1):
        if (
            expected != self.session
            or type(calls) is not tuple
            or not 1 <= len(calls) <= 16
            or any(type(call) is not BatchCall for call in calls)
            or len({call.call_id for call in calls}) != len(calls)
            or type(deadline) not in (int, float)
            or not math.isfinite(deadline)
            or type(concurrency) is not int
            or not 1 <= concurrency <= 4
            or (concurrency > 1 and getattr(self.port, "concurrent_reads", False) is not True)
        ):
            raise ContractError("batch admission invalid")
        if not self._lock.acquire(blocking=False):
            raise ContractError("batch already running")
        try:
            if self._used.intersection(call.call_id for call in calls):
                raise ContractError("batch call cannot be replayed")
            if len(self._used) + len(calls) > 256:
                raise ContractError("batch session call budget exhausted")
            self._admit(calls)
            self._used.update(call.call_id for call in calls)
            receipts, values = [], []
            outcomes = self._execute(calls, deadline, cancelled, concurrency)
            for call, (state, reason, value) in zip(calls, outcomes, strict=True):
                receipts.append({"call_id": call.call_id, "state": state, "reason": reason})
                values.append(value)
            return BatchResult(tuple(receipts), tuple(values))
        finally:
            self._lock.release()

    def _admit(self, calls):
        """Optional composition hook; failure prevents every item dispatch."""

    def _effect(self, call):
        return EffectClass.READ_ONLY

    def _execute(self, calls, deadline, cancelled, concurrency):
        if concurrency == 1:
            return [self._invoke(call, deadline, cancelled) for call in calls]
        stopped = threading.Event()

        def is_cancelled():
            return stopped.is_set() or cancelled()

        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="ahc-read") as pool:
            futures = []
            try:
                for call in calls:
                    futures.append(pool.submit(self._invoke, call, deadline, is_cancelled))
                return [future.result() for future in futures]
            except BaseException:
                stopped.set()
                for future in futures:
                    future.cancel()
                raise

    def _invoke(self, call, deadline, cancelled):
        try:
            if cancelled() or self.clock() >= deadline:
                return "rejected", "not-started", None
            decision = self.view.decide(
                call.capability_id, call.resource_id, self._effect(call)
            )
            if decision.kind is not DecisionKind.ALLOW:
                return "rejected", decision.reason.value, None
        except Exception:
            return "rejected", "admission-unavailable", None
        try:
            value = self.port.invoke(self.session, call, deadline, cancelled)
            if cancelled() or self.clock() >= deadline:
                return "unknown", "completion-unverified", None
            encoded = json.dumps(value, allow_nan=False, ensure_ascii=True)
            if len(encoded.encode()) > 16384:
                return "failure", "result-too-large", None
            return "success", None, json.loads(encoded)
        except BatchRejected:
            return "rejected", "invalid-tool-request", None
        except Exception:
            # A raised transport/adapter exception is not proof of no execution.
            return "unknown", "completion-unverified", None
