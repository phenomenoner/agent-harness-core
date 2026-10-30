"""Foreground supervisor for configured M0 child services."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from threading import Event

from ahc.domain import AppConfig, ProcessIdentity, RestartPolicy, ServiceSpec
from ahc.infrastructure.persistence import (
    ControlStore,
    JsonStoreInvalid,
    JsonStoreMissing,
    StateStore,
)
from ahc.infrastructure.process import InstanceLease, LeaseUnavailable, current_identity
from ahc.infrastructure.process.managed import ManagedProcess, ProcessCleanupError, spawn_managed

from .operator import LEASE_BUSY, OK, SUPERVISOR_FAILURE
from .supervisor_state import build_state_snapshot


@dataclass
class _Child:
    spec: ServiceSpec
    managed: ManagedProcess | None
    restarts: int = 0
    return_code: int | None = None
    restart_at: float | None = None

    @property
    def process(self) -> subprocess.Popen[bytes] | None:
        return None if self.managed is None else self.managed.process

    @property
    def identity(self) -> ProcessIdentity | None:
        return None if self.managed is None else self.managed.identity


class Supervisor:
    """Own configured children while this Python process remains foreground."""

    def __init__(self, config: AppConfig, *, runtime=None) -> None:
        self.config = config
        self.runtime = runtime
        self.state = StateStore(config.state_dir)
        self.control = ControlStore(config.state_dir)
        self._children: dict[str, _Child] = {}
        self._identity: ProcessIdentity | None = None
        self._started_at = 0.0
        self._stop_event = Event()
        self._old_handlers: dict[int, object] = {}
        self._failure: str | None = None
        self._runtime_failures: list[dict] = []
        self._restart_requested = False
        self._restart_check_failures = 0

    def run(self, max_runtime_seconds: float | None = None) -> int:
        if max_runtime_seconds is not None and max_runtime_seconds < 0:
            raise ValueError("max_runtime_seconds must be non-negative")
        lease = InstanceLease(self.config.lease_path)
        try:
            lease.acquire()
        except LeaseUnavailable:
            return LEASE_BUSY

        self._identity = current_identity()
        self._started_at = time.time()
        self._install_signal_handlers()
        try:
            if self._safe_publish("starting") and self._start_children():
                if self.runtime is not None:
                    try:
                        self.runtime.start()
                    except Exception as error:
                        self._runtime_failure("start", error)
                        raise RuntimeError("foreground runtime start failed") from None
                if self._safe_publish("running"):
                    self._run_loop(max_runtime_seconds)
        except Exception as exc:
            self._record_failure(f"supervisor failure: {exc}")
        finally:
            self._shutdown()
            self._restore_signal_handlers()
            lease.release()
        return SUPERVISOR_FAILURE if self._failure else (75 if self._restart_requested else OK)

    def _run_loop(self, max_runtime_seconds: float | None) -> None:
        deadline = None
        if max_runtime_seconds is not None:
            deadline = time.monotonic() + max_runtime_seconds
        while not self._stop_event.is_set() and (deadline is None or time.monotonic() < deadline):
            if self._stop_requested():
                self._stop_event.set()
                break
            if self.runtime is not None and self._restart_check_failures:
                # Resolve deferred control admission before allowing another work step.
                if self._runtime_restart_requested():
                    self._restart_requested = True
                    self._stop_event.set()
                if self._stop_event.is_set():
                    break
                if self._restart_check_failures:
                    # Keep child supervision and stop/deadline handling alive,
                    # but do not admit another work step past an unresolved check.
                    self._reconcile_children()
                    if not self._safe_publish("degraded" if self._failure else "running"):
                        self._stop_event.set()
                        break
                    delay = self.config.poll_interval_seconds
                    if deadline is not None:
                        delay = min(delay, max(0.0, deadline - time.monotonic()))
                    self._stop_event.wait(delay)
                    continue
            self._reconcile_children()
            if self.runtime is not None and not self._stop_event.is_set():
                try:
                    self.runtime.step(
                        cancelled=lambda: (
                            self._stop_event.is_set()
                            or self._stop_requested()
                            or (deadline is not None and time.monotonic() >= deadline)
                        )
                    )
                except Exception as error:
                    self._runtime_failure("step", error)
                    self._record_failure("foreground runtime step failed")
                    self._stop_event.set()
                if (not self._failure and not self._stop_event.is_set()
                        and self._runtime_restart_requested()):
                    self._restart_requested = True
                    self._stop_event.set()
            if not self._safe_publish("degraded" if self._failure else "running"):
                self._stop_event.set()
                break
            delay = self.config.poll_interval_seconds
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - time.monotonic()))
            self._stop_event.wait(delay)

    def _runtime_restart_requested(self) -> bool:
        from ahc.infrastructure.working_repository import WorkingWriterBusy

        try:
            requested = getattr(self.runtime, "restart_requested", lambda: False)()
        except WorkingWriterBusy:
            # A rejected/rolled-back control transaction has no committed restart.
            # One attempt per normal supervision cycle; no inner retry or model
            # replay. Contention alone is not evidence of a failed runtime.
            self._restart_check_failures = 1
            return False
        except Exception as error:
            self._runtime_failure("restart-check", error)
            self._record_failure("foreground restart check failed")
            self._stop_event.set()
            return False
        self._restart_check_failures = 0
        return requested

    def _start_children(self) -> bool:
        for spec in self.config.services:
            try:
                self._children[spec.name] = self._spawn(spec)
            except Exception as exc:
                self._record_failure(f"failed to start {spec.name}: {exc}")
                self._children[spec.name] = _Child(spec, None)
                return False
        return True

    @staticmethod
    def _spawn(spec: ServiceSpec, restarts: int = 0) -> _Child:
        environment = os.environ.copy()
        environment.update(spec.env)
        command = tuple(sys.executable if item == "{python}" else item for item in spec.command)
        managed = spawn_managed(
            command,
            cwd=None if spec.cwd is None else str(spec.cwd),
            env=environment,
        )
        return _Child(spec, managed, restarts=restarts)

    def _reconcile_children(self) -> None:
        for child in self._children.values():
            process = child.process
            if process is None:
                continue
            if child.return_code is None and process.poll() is not None:
                child.return_code = process.returncode
                if not self._cleanup_generation(child):
                    self._stop_event.set()
                    continue
                if self._should_restart(child):
                    child.restart_at = time.monotonic() + child.spec.restart_delay_seconds
                elif not self._stop_event.is_set():
                    self._record_failure(
                        f"{child.spec.name} stopped unexpectedly with exit code {child.return_code}"
                    )
                    self._stop_event.set()
            if (
                child.restart_at is not None
                and not self._stop_event.is_set()
                and time.monotonic() >= child.restart_at
            ):
                self._restart(child)

    def _restart(self, child: _Child) -> None:
        child.restarts += 1
        try:
            replacement = self._spawn(child.spec, child.restarts)
        except Exception as exc:
            self._record_failure(f"failed to restart {child.spec.name}: {exc}")
            child.managed = None
            child.restart_at = None
            self._stop_event.set()
            return
        child.managed = replacement.managed
        child.return_code = None
        child.restart_at = None

    @staticmethod
    def _should_restart(child: _Child) -> bool:
        if child.return_code is None or child.restarts >= child.spec.max_restarts:
            return False
        if child.spec.restart_policy is RestartPolicy.ALWAYS:
            return True
        return child.spec.restart_policy is RestartPolicy.ON_FAILURE and child.return_code != 0

    def _cleanup_generation(self, child: _Child) -> bool:
        if child.managed is None:
            return True
        try:
            result = child.managed.cleanup(self._stop_timeout(child.spec))
        except ProcessCleanupError as exc:
            self._record_failure(f"cleanup failed for {child.spec.name}: {exc}")
            return False
        if not result.complete:
            self._record_failure(
                f"cleanup incomplete for {child.spec.name}: "
                f"{result.remaining_count} owned process(es) remain"
            )
            return False
        return True

    def _stop_children(self) -> bool:
        complete = True
        for child in self._children.values():
            if not self._cleanup_generation(child):
                complete = False
            process = child.process
            if process is None:
                continue
            try:
                child.return_code = process.wait(timeout=0.2)
            except (subprocess.TimeoutExpired, ChildProcessError):
                if process.poll() is None:
                    complete = False
                    self._record_failure(f"root process still active for {child.spec.name}")
        return complete

    def _shutdown(self) -> None:
        self._safe_publish("stopping")
        cleanup_complete = self._stop_children()
        if self.runtime is not None:
            try:
                self.runtime.close()
            except Exception as error:
                self._runtime_failure("close", error)
                cleanup_complete = False
                self._record_failure("foreground runtime cleanup unverified")
        if not cleanup_complete and self._failure is None:
            self._record_failure("owned process cleanup incomplete")
        terminal = "stopped" if cleanup_complete and self._failure is None else "degraded"
        self._safe_publish(terminal)

    def _safe_publish(self, status: str) -> bool:
        try:
            self._publish(status)
            return True
        except Exception as exc:
            self._record_failure(f"state publication failed for {status}: {exc}")
            return False

    def _record_failure(self, message: str) -> None:
        if self._failure is None:
            self._failure = message
        elif message not in self._failure:
            self._failure = f"{self._failure}; {message}"

    def _runtime_failure(self, phase, error):
        from .failure_trace import failure_trace

        if len(self._runtime_failures) < 3:
            detail = {"phase": phase, **failure_trace(error)}
            receiver = getattr(self.runtime, "channel_receiver", None)
            trace = getattr(receiver, "failure_trace", None)
            if trace is not None:
                detail["channel_receiver"] = trace
            self._runtime_failures.append(detail)
            from ahc.infrastructure.persistence import AtomicJsonStore

            try:
                AtomicJsonStore(self.config.state_dir / "runtime-failure.json").write({
                    "schema_version": 1,
                    "supervisor": self._identity.to_dict(),
                    "runtime_failures": self._runtime_failures,
                })
            except Exception:
                pass  # Optional diagnostics must not prevent the existing failure/cleanup path.

    def _stop_requested(self) -> bool:
        try:
            request = self.control.read()
        except (JsonStoreMissing, JsonStoreInvalid):
            return False
        if request.get("type") != "stop" or self._identity is None:
            return False
        try:
            target = self.control.target(request)
        except JsonStoreInvalid:
            return False
        return target == self._identity

    def _publish(self, status: str) -> None:
        if self._identity is None:
            return
        snapshot = build_state_snapshot(
                status=status,
                identity=self._identity,
                children=self._children,
                started_at=self._started_at,
                failure=self._failure,
            )
        self.state.write(snapshot)
        admission = getattr(self.runtime, "admission_status", None)
        if callable(admission):
            from ahc.infrastructure.persistence import AtomicJsonStore
            try:
                AtomicJsonStore(self.config.state_dir / "runtime-readiness.json").write({
                    "supervisor": self._identity.to_dict(), "updated_at": time.time(),
                    "admission": admission(),
                })
            except Exception:
                pass  # Optional status publication cannot stop the control/diagnostic loop.

    def _install_signal_handlers(self) -> None:
        def stop_handler(_signum: int, _frame: object) -> None:
            self._stop_event.set()

        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                self._old_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, stop_handler)
            except (ValueError, OSError):
                pass

    def _restore_signal_handlers(self) -> None:
        for signum, handler in self._old_handlers.items():
            try:
                signal.signal(signum, handler)  # type: ignore[arg-type]
            except (ValueError, OSError):
                pass
        self._old_handlers.clear()

    def _stop_timeout(self, spec: ServiceSpec) -> float:
        return max(self.config.child_stop_timeout_seconds, spec.stop_timeout_seconds)
