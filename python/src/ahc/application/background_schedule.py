"""One background owner, independent of foreground model and conversation lifetime."""

import threading

from ahc.domain.sessions import ContractError
from ahc.infrastructure.background_schedule_dispatch import BackgroundScheduleDispatch
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.working_repository import WorkingWriterBusy


class BackgroundSchedule:
    def __init__(self, store, session, runtime_factory, *, tick=None, interval=0.25):
        self.store, self.session, self.runtime_factory = store, session, runtime_factory
        self.dispatch = BackgroundScheduleDispatch(store)
        self.tick = tick or ForegroundSchedule(store)
        self.interval = interval
        self._stop = threading.Event()
        self._admission_lock = threading.Lock()
        self._paused = False
        self._thread = None
        self._runtimes, self._failed = {}, {}
        self._active = None
        self._state, self._failure = "new", None
        self._cursor = ""

    def status(self):
        try:
            return {"state": self._state,
                    **self.dispatch.status(self.session, active_operation=self._active),
                    "unavailable_workbenches": len(self._failed),
                    **({"failure_trace": self._failure} if self._failure else {})}
        except Exception:
            return {"state": "observation-unavailable", "running": int(self._active is not None)}

    def start(self):
        if self._thread is not None:
            raise ContractError("background schedule owner already started")
        self._state = "running"
        self._thread = threading.Thread(target=self._run, name="ahc-background-schedule", daemon=True)
        self._thread.start()

    def prepare_restart(self):
        # Freeze admission before the supervisor decides it is safe to stop.
        with self._admission_lock:
            self._paused = True
        return self.status()

    def _run(self):
        from .failure_trace import failure_trace

        try:
            while not self._stop.is_set():
                try:
                    self.step()
                except WorkingWriterBusy:
                    pass  # No model replay; reobserve durable state on the next timer tick.
                self._stop.wait(self.interval)
        except Exception as error:
            self._state, self._failure = "failed", failure_trace(error)
        finally:
            for runtime in self._runtimes.values():
                try:
                    runtime.close()
                except Exception as error:
                    self._state, self._failure = "close-unverified", failure_trace(error)
            if self._state == "running":
                self._state = "closed"

    def step(self):
        current = self.store.read_current(self.session.ref.virtual_session_id)
        if current.session != self.session or current.status != "active":
            raise ContractError("background schedule origin changed")
        if not self._paused:
            self.tick(current)
            self.dispatch.claim_one(self.session)
        rows = self.dispatch.pending(self.session)
        rows = sorted(rows, key=lambda row: (row["worker_operation_id"] <= self._cursor,
                                             row["worker_operation_id"]))
        for binding in rows:
            if self._stop.is_set():
                return
            key, operation = binding["workspace_id"], binding["worker_operation_id"]
            if key in self._failed:
                continue
            session = binding["worker_session"]
            turn = self.store.read_turn(session, operation)
            if turn is not None and turn.status == "submitted":
                # A prior process may have executed effects. Never restart its model.
                self._failed[key] = "unfinished-turn-requires-reconciliation"
                continue
            with self._admission_lock:
                if self._paused and turn is None:
                    continue
                self._cursor, self._active = operation, operation
            try:
                runtime = self._runtimes.get(key)
                if runtime is None:
                    runtime = self.runtime_factory(binding)
                    self._runtimes[key] = runtime
                    runtime.start()
                if self._paused:
                    if runtime.channel_delivery is not None:
                        runtime.channel_delivery(cancelled=self._stop.is_set)
                else:
                    runtime.step(cancelled=self._stop.is_set)
                if not runtime.admission_status()["ready"]:
                    self._failed[key] = "workbench-unavailable"
            except WorkingWriterBusy:
                # No repeat inside a step. Existing admission/custody protects effects.
                return
            except Exception as error:
                from .failure_trace import failure_trace

                self._failed[key] = failure_trace(error)
            finally:
                self._active = None
            return  # One bounded workbench step per tick, with fair rotation.

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=15)
            if self._thread.is_alive():
                self._state = "close-unverified"
                raise ContractError("background schedule owner shutdown unverified")
        else:
            self._state = "closed"
        if self._state == "close-unverified":
            raise ContractError("background schedule owner shutdown unverified")
