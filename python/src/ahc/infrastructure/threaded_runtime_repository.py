"""Bounded in-process writer serialization for a foreground with background ingress."""

import threading
from contextlib import contextmanager

from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


class ThreadedRuntimeRepository(RuntimeRepository):
    """Share this instance between foreground and receiver; OS ownership remains unchanged."""

    # Local filesystem scheduling may exceed one second. Keep this finite and below
    # the receiver's 15-second shutdown join; external owners still fail immediately.
    _thread_wait_seconds = 10

    def __init__(self, root):
        super().__init__(root)
        self._thread_writer = threading.RLock()

    def _open_connection(self, *, readonly):
        connection = super()._open_connection(readonly=readonly)
        try:
            # A separate reader can briefly block COMMIT despite the writer lease.
            # SQLite waits inside the statement; application callbacks are not replayed.
            connection.execute("PRAGMA busy_timeout=1000")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def writer_lock(self):
        if not self._thread_writer.acquire(timeout=self._thread_wait_seconds):
            raise WorkingWriterBusy("foreground writer wait exceeded")
        try:
            with super().writer_lock():
                yield
        finally:
            self._thread_writer.release()

    @contextmanager
    def _read(self):
        if not self._thread_writer.acquire(timeout=self._thread_wait_seconds):
            raise WorkingWriterBusy("foreground reader wait exceeded")
        try:
            with super()._read() as connection:
                yield connection
        finally:
            self._thread_writer.release()
