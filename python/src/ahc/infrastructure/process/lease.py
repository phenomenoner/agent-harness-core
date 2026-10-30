"""Nonblocking one-supervisor lease."""

from __future__ import annotations

import os
from pathlib import Path
from typing import IO

import portalocker


class LeaseUnavailable(RuntimeError):
    """Another supervisor owns the state-directory lease."""


class InstanceLease:
    """Hold an exclusive advisory lock until the foreground run exits."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._handle: IO[bytes] | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()).encode("ascii"))
            handle.flush()
        except (portalocker.exceptions.LockException, BlockingIOError, OSError) as exc:
            handle.close()
            raise LeaseUnavailable(f"lease is already held: {self.path}") from exc
        except Exception:
            handle.close()
            raise
        self._handle = handle

    def release(self) -> None:
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            portalocker.unlock(handle)
        finally:
            handle.close()

    def __enter__(self) -> InstanceLease:
        self.acquire()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.release()
