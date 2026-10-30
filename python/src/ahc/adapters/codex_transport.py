"""Bounded Codex JSONL transport with pre-execution Windows Job custody.

Transport availability is not agent-turn policy admission. Only payload-free
counters and fixed fault categories are observable; raw errors and paths are excluded.
"""

import json
import os
import queue
import threading
import time
from math import isfinite
from pathlib import Path

from ahc.domain.backend_diagnostics import transport_failure_projection
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.managed import spawn_managed

from .codex_bundle import verify_bundle


class CodexTransportError(RuntimeError):
    """Public transport error with a fixed, payload-free diagnostic category."""

    _TYPES = frozenset({
        "unknown", "closed", "deadline", "write-indeterminate", "eof",
        "invalid-encoding", "malformed-json", "protocol-frame", "reader-io",
        "cleanup-unverified",
    })

    def __init__(self, message, *, transport_error_type="unknown", transport_failure=None):
        super().__init__(message)
        self.transport_error_type = (
            transport_error_type
            if type(transport_error_type) is str and transport_error_type in self._TYPES
            else "unknown"
        )
        self.transport_failure = transport_failure_projection(transport_failure)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate protocol field")
        result[key] = value
    return result


class CodexStdioTransport:
    MAX_FRAME = 16 * 1048576
    MAX_OUTBOUND_FRAME = 1048576
    MAX_QUEUED_BYTES = 32 * 1048576

    @classmethod
    def launch(cls, bundle, *, cwd, home):
        executable = verify_bundle(Path(bundle))
        paths = [Path(cwd), Path(home)]
        for path in paths:
            if not path.is_absolute() or not path.is_dir():
                raise ContractError("Codex requires explicit existing workspace and home")
            if any(p.is_symlink() or p.is_junction() for p in (path, *path.parents)):
                raise ContractError("Codex workspace and home cannot traverse links")
        environment = {
            key: os.environ[key] for key in ("SystemRoot", "WINDIR", "COMSPEC") if key in os.environ
        }
        environment.update(CODEX_HOME=str(paths[1]), TEMP=str(paths[1]), TMP=str(paths[1]))
        owned = spawn_managed(
            (str(executable), "app-server", "--strict-config"),
            cwd=str(paths[0]),
            env=environment,
            protocol_pipes=True,
        )
        try:
            return cls(owned)
        except BaseException:
            owned.cleanup(5)
            raise

    def __init__(self, owned, *, streaming=False, max_wait=300):
        if (
            type(streaming) is not bool
            or (max_wait is None and not streaming)
            or (max_wait is not None and (type(max_wait) not in (int, float)
                or not 0 < max_wait <= 86400))
        ):
            raise ContractError("invalid explicit transport lifetime mode")
        self._streaming, self._max_wait = streaming, max_wait
        if owned.process.stdin is None or owned.process.stdout is None:
            raise ContractError("Codex transport requires owned protocol pipes")
        self.owned = owned
        self._frames = queue.Queue(maxsize=min(8, max(1, self.MAX_QUEUED_BYTES // self.MAX_FRAME)))
        # Fixed diagnostic categories only; never retain protocol payload or stderr.
        self.reader_failure = None
        self._reader_failure_observation = {}
        self.receive_failure = None
        self._fault = threading.Event()
        self._eof = threading.Event()
        self._reader_stop = threading.Event()
        self._closed = False
        self._write_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._metrics = {
            "stdout_bytes": 0, "stdout_reads": 0, "receive_returns": 0,
            "stdin_bytes": 0, "write_attempts": 0, "write_returns": 0,
            "last_stdout_monotonic": None, "last_receive_monotonic": None,
            "last_write_monotonic": None,
        }
        self._reader = threading.Thread(target=self._read_frames, daemon=True)
        self._reader.start()

    def telemetry_snapshot(self):
        """Advisory metadata only: no payload, identity, acceptance or retry grant."""
        with self._metrics_lock:
            result = dict(self._metrics)
        return {
            **result, "queue_entries": self._frames.qsize(),
            "reader_alive": self._reader.is_alive(), "stdout_eof": self._eof.is_set(),
            "reader_fault": self._fault.is_set(), "reader_failure": self.reader_failure,
            "receive_failure": self.receive_failure, "closed": self._closed,
        }

    def _read_frames(self):
        try:
            from itertools import count

            for _ in count():
                if self._reader_stop.is_set():
                    return
                raw = self.owned.process.stdout.readline(self.MAX_FRAME + 1)
                if not raw:
                    self._eof.set()
                    try:
                        self._frames.put_nowait(None)
                    except queue.Full:
                        pass  # EOF consumes no frame capacity; retain queued valid frames.
                    return
                with self._metrics_lock:
                    self._metrics["stdout_reads"] = min(
                        2**63 - 1, self._metrics["stdout_reads"] + 1)
                    self._metrics["stdout_bytes"] = min(
                        2**63 - 1, self._metrics["stdout_bytes"] + len(raw))
                    self._metrics["last_stdout_monotonic"] = time.monotonic()
                if len(raw) > self.MAX_FRAME:
                    self.reader_failure = "frame-size-limit"
                elif not raw.endswith(b"\n"):
                    self.reader_failure = "unterminated-frame"
                if self.reader_failure is not None:
                    # readline stops at limit+1: this is an observed prefix, not total frame size.
                    self._reader_failure_observation = {
                        "reason": self.reader_failure, "observed_bytes": len(raw),
                        "limit_bytes": self.MAX_FRAME,
                    }
                    raise ValueError()
                # A synchronous tool call may temporarily pause the consumer.
                # Bound memory with backpressure, not loss of valid frames.
                while not self._reader_stop.is_set():
                    try:
                        self._frames.put(raw, timeout=0.1)
                        break
                    except queue.Full:
                        continue
                else:
                    return
            raise ValueError()
        except (OSError, ValueError, queue.Full):
            if self.reader_failure is None:
                self.reader_failure = "reader-io"
            self._fault.set()
            try:
                self._frames.put_nowait(None)
            except queue.Full:
                pass

    def _remaining(self, deadline):
        from ahc.domain.adaptive_time_budget import deadline_value
        deadline = deadline_value(deadline)
        if type(deadline) not in (int, float) or not isfinite(deadline):
            raise CodexTransportError(
                "Codex transport deadline expired or invalid", transport_error_type="deadline"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0 or (self._max_wait is not None and remaining > self._max_wait):
            raise CodexTransportError(
                "Codex transport deadline expired or invalid", transport_error_type="deadline"
            )
        return remaining

    def send(self, message, *, deadline):
        if self._closed:
            raise CodexTransportError("Codex transport is closed", transport_error_type="closed")
        try:
            if type(message) is not dict:
                raise ValueError()
            encoded = json.dumps(message, allow_nan=False, ensure_ascii=False,
                                 separators=(",", ":")).encode("utf-8") + b"\n"
            if len(encoded) > self.MAX_OUTBOUND_FRAME:
                raise ValueError()
        except (ValueError, TypeError, RecursionError):
            raise ContractError("Codex outbound frame is invalid") from None
        # An already-expired deadline must never start a writer thread.
        try:
            self._remaining(deadline)
        except CodexTransportError:
            self.close()
            raise
        completed = queue.Queue(maxsize=1)
        with self._metrics_lock:
            self._metrics["write_attempts"] = min(2**63 - 1, self._metrics["write_attempts"] + 1)

        def write():
            try:
                with self._write_lock:
                    self._remaining(deadline)
                    self.owned.process.stdin.write(encoded)
                    self.owned.process.stdin.flush()
                with self._metrics_lock:
                    self._metrics["write_returns"] = min(
                        2**63 - 1, self._metrics["write_returns"] + 1)
                    self._metrics["stdin_bytes"] = min(
                        2**63 - 1, self._metrics["stdin_bytes"] + len(encoded))
                    self._metrics["last_write_monotonic"] = time.monotonic()
                completed.put(True)
            except (OSError, ValueError, CodexTransportError):
                completed.put(False)

        writer = threading.Thread(target=write, daemon=True)
        writer.start()
        try:
            while True:
                try:
                    if not completed.get(timeout=self._remaining(deadline)):
                        raise CodexTransportError("Codex write is indeterminate")
                    break
                except queue.Empty:
                    if not callable(deadline):
                        raise
                    self._remaining(deadline)  # Renew this same write at expiry, never resend.
        except (CodexTransportError, queue.Empty):
            self.close()
            raise CodexTransportError(
                "Codex write is indeterminate", transport_error_type="write-indeterminate"
            ) from None
        finally:
            writer.join(timeout=1)

    def receive(self, *, deadline, on_idle=None):
        """Receive one frame, optionally letting the decoder advance local control.

        ``on_idle`` runs only on a bounded local wait expiry.  It must not read
        this transport: the conversation decoder remains the sole frame owner.
        """
        if on_idle is not None and not callable(on_idle):
            raise ContractError("Codex transport idle callback invalid")
        reason = "closed"
        raw = None
        try:
            if self._closed:
                raise ValueError()
            if self._fault.is_set():
                reason = self.reader_failure
                raise ValueError()
            reason = "eof"
            if self._eof.is_set() and self._frames.empty():
                raise ValueError()
            reason = "deadline"
            while True:
                remaining = self._remaining(deadline)
                reason = "wait-timeout"
                try:
                    raw = self._frames.get(timeout=min(remaining, 0.25) if on_idle else remaining)
                    break
                except queue.Empty:
                    if on_idle is None:
                        if callable(deadline):
                            self._remaining(deadline)  # The expiry event may renew the owner.
                            continue
                        raise
                    on_idle()
            if self._fault.is_set():
                reason = self.reader_failure
                raise ValueError()
            reason = "eof"
            if raw is None:
                raise ValueError()
            reason = "invalid-encoding"
            decoded = raw.decode("utf-8")
            reason = "invalid-json"
            value = json.loads(decoded, object_pairs_hook=_object)
            reason = "nonobject-frame"
            if type(value) is not dict:
                raise ValueError()
            with self._metrics_lock:
                self._metrics["receive_returns"] = min(
                    2**63 - 1, self._metrics["receive_returns"] + 1)
                self._metrics["last_receive_monotonic"] = time.monotonic()
            return value
        except (CodexTransportError, ValueError, TypeError, RecursionError, queue.Empty):
            self.receive_failure = reason
            failure = {"reason": reason}
            if self._fault.is_set():
                failure.update(self._reader_failure_observation)
            elif raw is not None:
                failure.update(observed_bytes=len(raw), limit_bytes=self.MAX_FRAME)
            try:
                self.close()
            except CodexTransportError as cleanup_error:
                cleanup_error.transport_failure = transport_failure_projection(failure)
                raise
            error_type = {
                "closed": "closed",
                "nonobject-frame": "protocol-frame",
                "invalid-encoding": "invalid-encoding",
                "invalid-json": "malformed-json",
                "eof": "eof",
                "deadline": "deadline",
                "wait-timeout": "deadline",
                "reader-io": "reader-io",
                "frame-count-limit": "protocol-frame",
                "frame-size-limit": "protocol-frame",
                "byte-count-limit": "protocol-frame",
                "unterminated-frame": "protocol-frame",
            }.get(reason, "unknown")
            raise CodexTransportError(
                "Codex response is unavailable or invalid", transport_error_type=error_type,
                transport_failure=failure,
            ) from None

    def close(self):
        if self._closed:
            return
        try:
            result = self.owned.cleanup(5)
            if not result.complete or result.remaining_count != 0:
                raise ValueError()
            self._reader_stop.set()
            self.owned.process.wait(timeout=5)
            self.owned.process.stdin.close()
            self.owned.process.stdout.close()
            self._reader.join(timeout=1)
        except Exception:
            raise CodexTransportError(
                "Codex process custody cleanup is unverified",
                transport_error_type="cleanup-unverified",
            ) from None
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
