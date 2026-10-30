"""Persistent custody-owned trusted Python; no host containment claim."""

import json
from contextlib import contextmanager
import queue
import threading
import time
from ahc.domain.adaptive_time_budget import deadline_value
from dataclasses import replace
from math import isfinite

from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import (
    CheckpointValueRejected, WorkspaceCheckpoint, WorkspaceIdentity, workspace_failure_projection,
)
from ahc.domain.workspace_display import display_fields
from ahc.infrastructure.backend_custody import BackendCustody

from .docker_workspace import WorkspaceUnavailable
from .workspace_launch import kernel_command as _kernel_command
from .wsl_command import _linux_path
from .wsl_python_checkpoint import validate_checkpoint
from .wsl_python_cleanup import cleanup_workspace

_DISTRIBUTION = "AHC-Codex-Ubuntu-24.04"
_REQUEST_LIMIT = 65536
_OUTPUT_LIMIT = 4096


class WslPythonWorkspace:
    """One persistent, explicitly granted Python interpreter generation."""

    MAX_LIFETIME_SECONDS = 3600

    def __init__(
        self,
        store,
        runner,
        identity,
        *,
        effective_policy,
        environment_digest,
        cwd,
        interpreter="/usr/bin/python3",
        user="ahc",
        timeout=30,
        lifetime_seconds=3600,
        checkpoint=None,
        cancelled=lambda: False,
        batch=None,
        engine="python",
    ):
        if type(identity) is not WorkspaceIdentity or identity.revision != 0:
            raise ContractError("Python workspace requires an initial identity")
        if type(effective_policy) is not EffectivePolicy:
            raise ContractError("Python workspace requires an exact effective policy")
        effective_policy.require_workspace_grant(environment_digest)
        _linux_path(cwd)
        _linux_path(interpreter)
        if interpreter == "/":
            raise ContractError("Python workspace interpreter invalid")
        self._validate_runner(runner, user)
        if (type(lifetime_seconds) is not int
                or not 1 <= lifetime_seconds <= self.MAX_LIFETIME_SECONDS):
            raise ContractError("Python workspace lifetime invalid")
        if (
            type(timeout) not in (int, float)
            or not isfinite(timeout)
            or not 0 < timeout <= lifetime_seconds
        ):
            raise ContractError("Python workspace timeout invalid")
        if not callable(cancelled):
            raise ContractError("Python workspace cancellation predicate invalid")
        self._validate_checkpoint(identity, checkpoint, initial=True)
        self._store, self._runner, self._identity = store, runner, identity
        self._timeout, self._lifetime, self._cancelled = float(timeout), lifetime_seconds, cancelled
        self._cwd, self._interpreter = cwd, interpreter
        self._launch = _kernel_command(
            cwd, engine, startup_timeout=30 if getattr(runner, "owner_lifetime", False) else None)
        self._custody = BackendCustody(store, resource_role="python-workspace")
        self._lease = self._transport = None
        self._closed = self._cleanup_failed = False
        self._lock = threading.RLock()
        self._sequence = 0
        self._turn_deadline = None
        self._opened_at = time.monotonic()
        self._job_lease = None
        self.last_failure = None
        if batch is not None and batch.session != identity.session:
            raise ContractError("workspace batch session mismatch")
        self._batch = batch
        try:
            self._lease = self._custody.acquire(identity.session, identity.workspace_id)
            self._custody.recover(self._lease, runner)
            unit = self._custody.create(
                self._lease,
                identity,
                runner,
                (interpreter, "-I", "-u", "-c", self._launch),
                lifetime_seconds=lifetime_seconds,
            )
            self._unit = unit
            self._transport = runner.attach(unit, self._custody, self._lease)
            self._reply({"operation": "restore", "values": {}})
            self._closed = False
            if checkpoint is not None:
                self._restore_values(checkpoint)
        except BaseException:
            self._abort_open()
            raise

    _validate_checkpoint = staticmethod(validate_checkpoint)

    @staticmethod
    def _validate_runner(runner, user):
        if user != "ahc":
            raise ContractError("Python workspace requires the configured ahc user")
        if getattr(runner, "distribution", None) != _DISTRIBUTION:
            raise ContractError("Python workspace requires the dedicated AHC WSL distribution")

    @property
    def identity(self):
        return self._identity

    @property
    def skill_materialization_root(self):
        """Configured Linux cwd; tool arguments cannot select an output root."""
        return self._cwd

    def _require(self, expected):
        if self._closed or expected != self._identity:
            raise ContractError("workspace owner generation or revision is stale")

    @contextmanager
    def turn_scope(self, deadline):
        """An admitted task owns cell time; startup/direct calls retain their timeout."""
        budget = deadline if callable(getattr(deadline, "current_deadline", None)) else None
        if budget is not None:
            deadline = budget.current_deadline()
        if (type(deadline) not in (int, float) or not isfinite(deadline)
                or deadline <= time.monotonic()):
            raise ContractError("workspace turn deadline expired or invalid")
        with self._lock:
            if self._turn_deadline is not None:
                raise ContractError("workspace already has an admitted turn")
            self._turn_deadline = deadline
            self._turn_budget = budget
        try:
            yield
        finally:
            with self._lock:
                self._turn_deadline = None
                self._turn_budget = None

    def effective_turn_deadline(self):
        budget = getattr(self, "_turn_budget", None)
        return budget.current_deadline() if budget is not None else self._turn_deadline

    def _receive(self, deadline):
        """Wait for one reply while allowing a current cancellation to end ownership."""
        received = queue.Queue(maxsize=1)

        def receive_one():
            try:
                received.put((True, self._transport.receive(deadline=deadline)))
            except BaseException as exc:
                received.put((False, exc))

        threading.Thread(target=receive_one, daemon=True).start()
        while True:
            lease = getattr(self, "_job_lease", None)
            if self._cancelled() or (lease is not None and lease.cancel_requested):
                self.last_failure = {"phase": "receive", "code": "cancelled"}
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python workspace execution was cancelled")
            now = time.monotonic()
            remaining = deadline_value(deadline) - now
            lease = getattr(self, "_job_lease", None)
            if lease is not None:
                remaining = min(remaining, lease.remaining(now))
            if remaining <= 0:
                from .codex_transport import CodexTransportError
                raise CodexTransportError("workspace reply deadline", transport_error_type="deadline")
            try:
                ok, value = received.get(timeout=min(remaining, 0.05))
            except queue.Empty:
                continue  # A silent healthy transport has not yet crossed its deadline.
            if ok:
                if (self._cancelled() or (lease is not None and lease.cancel_requested)
                        or time.monotonic() >= deadline_value(deadline)):
                    self._raise_unavailable("receive", "cancelled-or-expired",
                                            "Python workspace reply arrived after ownership ended")
                return value
            raise value

    def _send(self, payload, deadline):
        """The original worker also owns cancellation of a blocked native write."""
        lease = getattr(self, "_job_lease", None)
        transport = self._transport
        if lease is None:
            return transport.send(payload, deadline=deadline)
        completed = queue.Queue(maxsize=1)

        def send_one():
            try:
                transport.send(payload, deadline=deadline)
                completed.put((True, None))
            except BaseException as error:
                completed.put((False, error))

        threading.Thread(target=send_one, daemon=True).start()
        while True:
            now = time.monotonic()
            remaining = min(deadline_value(deadline) - now, lease.remaining(now))
            if self._cancelled() or lease.cancel_requested or remaining <= 0:
                self._raise_unavailable("send", "cancelled-or-expired",
                                        "Python workspace write ownership ended")
            try:
                ok, error = completed.get(timeout=min(remaining, 0.05))
            except queue.Empty:
                continue
            if not ok:
                raise error
            if self._cancelled() or lease.cancel_requested or time.monotonic() >= deadline_value(deadline):
                self._raise_unavailable("send", "cancelled-or-expired",
                                        "Python workspace write ended after ownership")
            return

    def _reply(self, request):
        self._sequence += 1
        payload = {**request, "id": self._sequence}
        phase = "send"
        self.last_failure = None
        try:
            encoded = json.dumps(payload, allow_nan=False).encode("utf-8")
            if len(encoded) + 1 > _REQUEST_LIMIT:
                raise ValueError()
            budget = getattr(self, "_turn_budget", None)
            deadline = (budget.current_deadline if budget is not None else
                        self._turn_deadline if self._turn_deadline is not None else
                        time.monotonic() + self._timeout)
            lease = getattr(self, "_job_lease", None)
            if lease is not None:
                if budget is None:
                    deadline = min(deadline, lease.hard_deadline, lease.quiet_deadline)
                if lease.cancel_requested or time.monotonic() >= deadline_value(deadline):
                    self._raise_unavailable("send", "cancelled-or-expired",
                                            "Python workspace job ended before send")
            self._send(payload, deadline)
            from .workspace_batch_callback import receive_reply

            phase = "receive"
            reply = receive_reply(self, request, deadline)
            phase = "result-validation"
            if (
                type(reply) is not dict
                or reply.get("id") != self._sequence
                or type(reply.get("id")) is not int
                or type(reply.get("ok")) is not bool
            ):
                raise ValueError()
            return reply
        except WorkspaceUnavailable:
            raise
        except Exception as error:
            from .codex_transport import CodexTransportError
            code = "unknown"
            if isinstance(error, CodexTransportError):
                code = error.transport_error_type
            elif isinstance(error, OSError):
                code = "reader-io" if phase == "receive" else "io"
            elif isinstance(error, (ValueError, TypeError, RecursionError)):
                code = "protocol-frame"
            prior = workspace_failure_projection(self.last_failure)
            self._raise_unavailable(prior.get("phase", phase), prior.get("code", code),
                                    "Python workspace execution is indeterminate")

    def _raise_unavailable(self, phase, code, message):
        self.last_failure = workspace_failure_projection({"phase": phase, "code": code})
        try:
            self._terminate_indeterminate()
            self.last_failure["cleanup"] = "confirmed"
        except Exception:
            self.last_failure["cleanup"] = "unverified"
        raise WorkspaceUnavailable(message)

    def _restore_values(self, checkpoint):
        reply = self._reply({"operation": "restore", "values": json.loads(checkpoint.values_json)})
        if reply.get("ok") is not True:
            self._terminate_indeterminate()
            raise WorkspaceUnavailable("Python workspace restore is unverified")

    def cancel_job(self, lease):
        """Request cleanup by the existing worker without taking its execution lock."""
        lease.request_cancel()

    def execute_job(self, expected, code, lease):
        """Host-admitted optional job; shares this exact interpreter and hard deadline."""
        with self._lock:
            self._job_lease = lease
            try:
                if lease.cancel_requested:
                    self._terminate_indeterminate()
                    raise WorkspaceUnavailable("Python workspace job cancelled before send")
                return self.execute(expected, code)
            finally:
                if self._closed and not self._cleanup_failed:
                    lease.stopped.set()
                self._job_lease = None

    def execute(self, expected, code):
        with self._lock:
            self._require(expected)
            self.validate_execute(code)
            if self._cancelled() or (self._job_lease is not None and self._job_lease.cancel_requested):
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python workspace execution was cancelled")
            self._identity = replace(expected, revision=expected.revision + 1)
            reply = self._reply({"operation": "execute", "code": code,
                                 "batch_enabled": getattr(self, "_batch", None) is not None})
            if self._cancelled() or (self._job_lease is not None and self._job_lease.cancel_requested):
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python workspace execution was cancelled")
            output = reply.get("output")
            if (
                type(output) is not str
                or len(output) > _OUTPUT_LIMIT
                or any(0xD800 <= ord(char) <= 0xDFFF for char in output)
                or type(reply.get("output_truncated")) is not bool
            ):
                self._raise_unavailable("result-validation", "invalid-output",
                                        "Python workspace output is invalid")
            try:
                displays = display_fields(reply)
            except ContractError:
                self._raise_unavailable("result-validation", "invalid-display",
                                        "Python workspace display is invalid")
            return {
                "identity": self._identity.to_dict(),
                **displays,
                "ok": reply["ok"],
                "output": output,
                "output_truncated": reply["output_truncated"],
                "error": None if reply["ok"] else "execution-error",
            }

    def validate_execute(self, code):
        from ahc.domain.workspace import WorkspaceCodeRejected

        if type(code) is not str or any(0xD800 <= ord(char) <= 0xDFFF for char in code):
            raise WorkspaceCodeRejected()
        try:
            payload = {"operation": "execute", "code": code,
                       "batch_enabled": getattr(self, "_batch", None) is not None,
                       "id": self._sequence + 1}
            size = len(json.dumps(payload, allow_nan=False).encode("utf-8")) + 1
        except (UnicodeError, TypeError, ValueError):
            raise WorkspaceCodeRejected() from None
        if size > _REQUEST_LIMIT:
            raise WorkspaceCodeRejected(size, _REQUEST_LIMIT)

    def inspect(self, expected, names):
        from ahc.domain.workspace_inspection import inspection_names, inspection_result

        inspection_names(names)
        with self._lock:
            self._require(expected)
            reply = self._reply({"operation": "inspect", "names": list(names)})
            try:
                if reply.get("ok") is not True:
                    raise ContractError("inspection failed")
                return inspection_result(expected, names, reply.get("variables"))
            except ContractError:
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python workspace inspection is invalid") from None

    def export_artifact(self, expected, name):
        from .workspace_artifact_export import export_artifact

        return export_artifact(self, expected, name)

    def checkpoint(self, expected, names):
        if type(names) is not tuple:
            raise ContractError("checkpoint names must be immutable")
        with self._lock:
            self._require(expected)
            reply = self._reply({"operation": "checkpoint", "names": list(names)})
            if reply.get("ok") is not True:
                if (set(reply) == {"id", "ok", "error"}
                        and reply["error"] == "unsupported-value"):
                    raise CheckpointValueRejected("namespace value is not checkpointable")
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python checkpoint reply is invalid")
            values = reply.get("values")
            if type(values) is not dict or set(values) != set(names):
                self._terminate_indeterminate()
                raise WorkspaceUnavailable("Python checkpoint values do not bind requested names")
            try:
                return WorkspaceCheckpoint(self._identity, json.dumps(values, allow_nan=False))
            except (ContractError, ValueError, UnicodeError, RecursionError):
                raise CheckpointValueRejected("checkpoint values are not portable") from None

    def restore(self, expected, checkpoint):
        with self._lock:
            self._require(expected)
            self._validate_checkpoint(expected, checkpoint)
            successor = replace(expected, generation=expected.generation + 1, revision=0)
            self._terminate_indeterminate()
            self._identity, self._closed = successor, False
            try:
                self._lease = self._custody.acquire(successor.session, successor.workspace_id)
                self._custody.recover(self._lease, self._runner)
                unit = self._custody.create(
                    self._lease,
                    successor,
                    self._runner,
                    (self._interpreter, "-I", "-u", "-c", self._launch),
                    lifetime_seconds=self._lifetime,
                )
                self._transport = self._runner.attach(unit, self._custody, self._lease)
                self._reply({"operation": "restore", "values": {}})
                self._restore_values(checkpoint)
                return successor
            except BaseException:
                self._abort_open()
                raise

    def _abort_open(self):
        if self._cleanup_failed:
            raise WorkspaceUnavailable(
                "Python workspace cleanup is unresolved; restart reconciliation required"
            )
        try:
            cleanup_workspace(self)
        except Exception:
            self._cleanup_failed = True
            raise WorkspaceUnavailable(
                "Python workspace cleanup is unresolved; restart reconciliation required"
            ) from None

    def _terminate_indeterminate(self):
        if not self._closed or self._cleanup_failed:
            self._abort_open()

    def close(self):
        with self._lock:
            self._terminate_indeterminate()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
