"""Hold existing cooperative owners while capturing an offline restore source."""

import re
from contextlib import ExitStack, contextmanager
from pathlib import Path

import portalocker

from ahc.domain.sessions import ContractError
from ahc.domain.turns import _sha256_hex

from .runtime_backup import _digest
from .working_db import (
    WORKING_DB_FILENAME,
    decode_snapshot,
    require_real_directory,
    require_regular_file,
)
from .working_repository import WorkingWriterBusy


@contextmanager
def offline_restore_ownership(store, supervisor_lock, *, reconcile=False):
    """Hold cooperative process owners without opening SQLite or changing custody."""
    supervisor_lock = Path(supervisor_lock).absolute()
    require_real_directory(supervisor_lock.parent, allow_absent=False)
    require_regular_file(supervisor_lock, allow_absent=True)
    with ExitStack() as stack:
        def hold(path):
            require_regular_file(path, allow_absent=True)
            try:
                stack.enter_context(portalocker.Lock(str(path), mode="a+b",
                    flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0))
            except portalocker.exceptions.LockException:
                raise WorkingWriterBusy("restore source owner remains active") from None

        hold(supervisor_lock)
        stack.enter_context(store.execution_lock(restore=reconcile))
        if reconcile and (store.root / "sidecar-pending.json").exists():
            hold(store.lock_path)  # Database may already be retained in the pending attempt.
        else:
            stack.enter_context(store.writer_lock())
        owners = [path for path in store.root.iterdir()
                  if path.name.startswith(("backend-owner-", "workspace-owner-",
                                           "runtime-foreground-"))]
        if len(owners) > 4096:
            raise ContractError("restore owner inventory exceeds bound")
        for path in sorted(owners):
            if not re.fullmatch(
                    r"(?:(?:backend|workspace)-owner|runtime-foreground)-[0-9a-f]{64}\.lock",
                    path.name):
                raise ContractError("restore owner filename invalid")
            hold(path)
        yield


@contextmanager
def offline_restore_source(store, supervisor_lock, expected_source_digest, *, reconcile=False):
    """Caller binds the named supervisor to this root; no live stop or cleanup."""
    _sha256_hex(expected_source_digest)
    with offline_restore_ownership(store, supervisor_lock, reconcile=reconcile):
        database = store.root / WORKING_DB_FILENAME
        require_regular_file(database, allow_absent=False)
        # Initial offline mode requires a closed, sidecar-free source. It never
        # discards a journal or interprets unknown sidecar state as safe absence.
        for suffix in ("-wal", "-shm", "-journal"):
            if (store.root / (WORKING_DB_FILENAME + suffix)).exists():
                raise ContractError("restore source sidecar requires reconciliation")
        if _digest(database) != expected_source_digest:
            raise ContractError("restore source digest changed")
        with store._read() as connection:
            for table, predicate in (
                ("backend_custody", "state='held'"),
                ("workspace_custody", "state='held'"),
                ("backend_creation_intents", "state='pending'"),
            ):
                exists = connection.execute("SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name=?", (table,)).fetchone()
                if exists and connection.execute(
                        f"SELECT 1 FROM {table} WHERE {predicate} LIMIT 1").fetchone():
                    raise ContractError("restore source external custody unresolved")
            snapshots = tuple(decode_snapshot(row[0]) for row in connection.execute(
                "SELECT s.snapshot_json FROM current_working_sets c "
                "JOIN snapshots s ON s.snapshot_digest=c.snapshot_digest"))
        if _digest(database) != expected_source_digest:
            raise ContractError("restore source changed during capture")
        yield snapshots
