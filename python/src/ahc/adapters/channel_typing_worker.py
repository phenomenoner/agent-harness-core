"""Bounded ephemeral typing owner; close must finish before final delivery."""

import threading
import time

from ahc.domain.sessions import ContractError


class ChannelTypingWorker:
    def __init__(self, transport, can_send, *, interval_seconds=4, join_timeout=15):
        if (not callable(getattr(transport, "send", None)) or not callable(can_send)
                or type(interval_seconds) is not int or not 4 <= interval_seconds <= 10
                or type(join_timeout) not in (int, float) or not 0 < join_timeout <= 15):
            raise ContractError("typing worker configuration invalid")
        self.transport, self.can_send = transport, can_send
        self.interval, self.join_timeout = interval_seconds, join_timeout
        self._stop, self._lock = threading.Event(), threading.Lock()
        self._thread = None
        self._state = "new"
        self._failures = 0
        self._sends = 0

    def status(self):
        with self._lock:
            return {"state": self._state, "attempts": self._sends,
                    "consecutive_failures": self._failures, "failure_limit": 2}

    def start(self):
        with self._lock:
            if self._state != "new":
                raise ContractError("typing worker already consumed")
            self._state = "running"
            self._thread = threading.Thread(target=self._run, name="ahc-typing", daemon=True)
            self._thread.start()

    def _attempt(self):
        """One refresh; false ends this owner without creating another retry loop."""
        try:
            admitted = self.can_send()
            if admitted is not True or self._stop.is_set():
                return False
            with self._lock:
                self._sends += 1
            self.transport.send()
            with self._lock:
                self._failures = 0
            return True
        except Exception:
            with self._lock:
                self._failures += 1
                if self._failures >= 2:
                    self._state = "paused"
                    return False
            return True

    def _run(self):
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                if not self._attempt():
                    break
                if self._stop.wait(max(0, self.interval - (time.monotonic() - started))):
                    break
        finally:
            with self._lock:
                if self._state != "paused":
                    self._state = "stopped"

    def close(self):
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(self.join_timeout)
            if thread.is_alive():
                raise ContractError("typing worker shutdown unverified")
        with self._lock:
            self._state = "closed"
