"""Transactional SQLite/CAS repository for M2.2 working continuity."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import portalocker

from ahc.domain.sessions import _identifier
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition

from .working_db import (
    WORKING_DB_FILENAME,
    WORKING_LOCK_FILENAME,
    WorkingDatabaseError,
    decode_snapshot,
    initialize_connection,
    insert_snapshot,
    open_connection,
    require_real_directory,
    require_regular_file,
    require_schema,
    snapshot_pointer,
    sqlite_is_busy,
)


class WorkingRepositoryError(RuntimeError):
    """Base repository failure without raw rejected content."""


class WorkingNotInitialized(WorkingRepositoryError):
    pass


class WorkingSetNotFound(WorkingRepositoryError):
    pass


class WorkingSetConflict(WorkingRepositoryError):
    pass


class WorkingWriterBusy(WorkingRepositoryError):
    pass


class WorkingSetCorrupt(WorkingRepositoryError):
    pass


class WorkingSetRepository:
    """One explicit-root transactional working-set authority."""

    def __init__(self, root: str | Path, *, allow_pending_sidecar=False) -> None:
        try:
            self.root = require_real_directory(Path(root), allow_absent=False)
            self.db_path = self.root / WORKING_DB_FILENAME
            self.lock_path = self.root / WORKING_LOCK_FILENAME
            if allow_pending_sidecar:
                require_regular_file(self.root / "sidecar-pending.json", allow_absent=False)
            require_regular_file(self.db_path, allow_absent=allow_pending_sidecar)
            require_regular_file(self.lock_path, allow_absent=True)
        except WorkingDatabaseError as exc:
            raise WorkingNotInitialized("working continuity is not initialized") from exc

    @classmethod
    def initialize(cls, root: str | Path) -> WorkingSetRepository:
        try:
            path = require_real_directory(Path(root), allow_absent=True)
            path.mkdir(parents=True, exist_ok=True)
            path = require_real_directory(path, allow_absent=False)
            db_path = path / WORKING_DB_FILENAME
            lock_path = path / WORKING_LOCK_FILENAME
            require_regular_file(db_path, allow_absent=True)
            require_regular_file(lock_path, allow_absent=True)
            with portalocker.Lock(
                str(lock_path), flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0
            ):
                connection = open_connection(db_path, readonly=False, allow_create=True)
                try:
                    if db_path.stat().st_size == 0:
                        initialize_connection(connection)
                    else:
                        require_schema(connection)
                    connection.commit()
                finally:
                    connection.close()
        except portalocker.exceptions.LockException as exc:
            raise WorkingWriterBusy("working continuity writer is busy") from exc
        except (OSError, WorkingDatabaseError, sqlite3.Error) as exc:
            if sqlite_is_busy(exc):
                raise WorkingWriterBusy("working continuity writer is busy") from exc
            raise WorkingRepositoryError("working continuity initialization failed") from exc
        return cls(path)

    @contextmanager
    def writer_lock(self) -> Iterator[None]:
        try:
            require_regular_file(self.db_path, allow_absent=False)
            require_regular_file(self.lock_path, allow_absent=True)
            with portalocker.Lock(
                str(self.lock_path), flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0
            ):
                yield
        except portalocker.exceptions.LockException as exc:
            raise WorkingWriterBusy("working continuity writer is busy") from exc
        except WorkingDatabaseError as exc:
            raise WorkingRepositoryError("working continuity artifacts are invalid") from exc

    def _open_connection(self, *, readonly):
        return open_connection(self.db_path, readonly=readonly)

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        try:
            connection = self._open_connection(readonly=True)
            require_schema(connection)
            yield connection
        except WorkingDatabaseError as exc:
            raise WorkingSetCorrupt("working continuity state is unreadable") from exc
        finally:
            if connection is not None:
                connection.close()

    def _mutate(
        self, operation: Callable[[sqlite3.Connection], Any], *, fail_before_commit: bool = False
    ) -> Any:
        with self.writer_lock():
            try:
                connection = self._open_connection(readonly=False)
            except WorkingDatabaseError as exc:
                if sqlite_is_busy(exc):
                    raise WorkingWriterBusy("working continuity writer is busy") from exc
                raise WorkingRepositoryError("working continuity state is unavailable") from exc
            try:
                require_schema(connection)
                connection.execute("BEGIN IMMEDIATE")
                result = operation(connection)
                if fail_before_commit:
                    raise WorkingRepositoryError("injected pre-commit failure")
                connection.commit()
                return result
            except (WorkingDatabaseError, sqlite3.Error) as exc:
                connection.rollback()
                if sqlite_is_busy(exc):
                    raise WorkingWriterBusy("working continuity writer is busy") from exc
                raise WorkingRepositoryError("working continuity mutation failed") from exc
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    def create_snapshot(
        self, snapshot: WorkingSetSnapshot, *, fail_before_commit: bool = False
    ) -> WorkingSetSnapshot:
        if type(snapshot) is not WorkingSetSnapshot:
            raise WorkingRepositoryError("create requires WorkingSetSnapshot")
        if snapshot.session.ref.generation != 1 or snapshot.revision != 1:
            raise WorkingSetConflict("create requires an initial snapshot")

        def operation(connection: sqlite3.Connection) -> WorkingSetSnapshot:
            row = connection.execute(
                "SELECT snapshot_digest FROM current_working_sets WHERE virtual_session_id = ?",
                (snapshot.session.ref.virtual_session_id,),
            ).fetchone()
            if row is not None:
                if row[0] == snapshot.snapshot_digest:
                    return self._load_current(connection, snapshot.session.ref.virtual_session_id)
                raise WorkingSetConflict("virtual session is already bound to different state")
            insert_snapshot(connection, snapshot)
            connection.execute(
                "INSERT INTO current_working_sets VALUES(?,?,?,?,?,?)",
                snapshot_pointer(snapshot),
            )
            return snapshot

        return self._mutate(operation, fail_before_commit=fail_before_commit)

    def create(self, snapshot: WorkingSetSnapshot) -> WorkingSetSnapshot:
        return self.create_snapshot(snapshot)

    def append_snapshot(
        self,
        expected: WorkingSetSnapshot,
        successor: WorkingSetSnapshot,
        *,
        fail_before_commit: bool = False,
    ) -> WorkingSetSnapshot:
        if type(expected) is not WorkingSetSnapshot or type(successor) is not WorkingSetSnapshot:
            raise WorkingRepositoryError("append requires WorkingSetSnapshot values")
        validate_working_set_transition(expected, successor)

        def operation(connection: sqlite3.Connection) -> WorkingSetSnapshot:
            try:
                current = self._load_current(connection, expected.session.ref.virtual_session_id)
            except WorkingSetNotFound as exc:
                raise WorkingSetConflict(
                    "expected working set is stale or bound to another owner"
                ) from exc
            if current != expected:
                raise WorkingSetConflict("expected working set is stale or bound to another owner")
            insert_snapshot(connection, successor)
            cursor = connection.execute(
                """UPDATE current_working_sets
                   SET generation=?, revision=?, agent_id=?, route_id=?, snapshot_digest=?
                   WHERE virtual_session_id=? AND generation=? AND revision=?
                     AND agent_id=? AND route_id=? AND snapshot_digest=?""",
                (
                    successor.session.ref.generation,
                    successor.revision,
                    successor.session.agent_id,
                    successor.session.route_id,
                    successor.snapshot_digest,
                    expected.session.ref.virtual_session_id,
                    expected.session.ref.generation,
                    expected.revision,
                    expected.session.agent_id,
                    expected.session.route_id,
                    expected.snapshot_digest,
                ),
            )
            if cursor.rowcount != 1:
                raise WorkingSetConflict("working-set compare-and-set lost current authority")
            return successor

        return self._mutate(operation, fail_before_commit=fail_before_commit)

    def append(
        self, expected: WorkingSetSnapshot, successor: WorkingSetSnapshot
    ) -> WorkingSetSnapshot:
        return self.append_snapshot(expected, successor)

    def read_current(self, virtual_session_id: str) -> WorkingSetSnapshot:
        session_id = _identifier(virtual_session_id, "virtual_session_id")
        with self._read() as connection:
            return self._load_current(connection, session_id)

    def history(self, virtual_session_id: str) -> tuple[WorkingSetSnapshot, ...]:
        session_id = _identifier(virtual_session_id, "virtual_session_id")
        with self._read() as connection:
            rows = connection.execute(
                """SELECT snapshot_json FROM snapshots
                   WHERE virtual_session_id=? ORDER BY generation, revision""",
                (session_id,),
            ).fetchall()
        try:
            return tuple(decode_snapshot(row[0]) for row in rows)
        except WorkingDatabaseError as exc:
            raise WorkingSetCorrupt("working-set snapshot is invalid") from exc

    def _load_current(self, connection: sqlite3.Connection, session_id: str) -> WorkingSetSnapshot:
        row = connection.execute(
            """SELECT c.generation, c.revision, c.agent_id, c.route_id,
                      c.snapshot_digest, s.snapshot_json
               FROM current_working_sets c
               JOIN snapshots s
                 ON s.virtual_session_id=c.virtual_session_id
                AND s.generation=c.generation AND s.revision=c.revision
                AND s.agent_id=c.agent_id AND s.route_id=c.route_id
                AND s.snapshot_digest=c.snapshot_digest
               WHERE c.virtual_session_id=?""",
            (session_id,),
        ).fetchone()
        if row is None:
            exists = connection.execute(
                "SELECT 1 FROM current_working_sets WHERE virtual_session_id=?", (session_id,)
            ).fetchone()
            if exists is None:
                raise WorkingSetNotFound("working set is not published")
            raise WorkingSetCorrupt("working-set current pointer is inconsistent")
        try:
            snapshot = decode_snapshot(row[5])
        except WorkingDatabaseError as exc:
            raise WorkingSetCorrupt("working-set snapshot is invalid") from exc
        observed = (
            snapshot.session.ref.generation,
            snapshot.revision,
            snapshot.session.agent_id,
            snapshot.session.route_id,
            snapshot.snapshot_digest,
        )
        if observed != tuple(row[:5]):
            raise WorkingSetCorrupt("working-set projection disagrees with canonical snapshot")
        return snapshot


__all__ = [
    "WorkingNotInitialized",
    "WorkingRepositoryError",
    "WorkingSetConflict",
    "WorkingSetCorrupt",
    "WorkingSetNotFound",
    "WorkingSetRepository",
    "WorkingWriterBusy",
]
