from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import portalocker
import pytest

import ahc.infrastructure.working_repository as working_repository_module
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.judgment_goal import GOAL_SCHEMA, GoalContract
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.working_db import (
    WORKING_DB_FILENAME,
    WORKING_LOCK_FILENAME,
    WorkingDatabaseError,
)
from ahc.infrastructure.working_repository import (
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
    WorkingSetRepository,
    WorkingWriterBusy,
)
from ahc.infrastructure.working_status import inspect_working_continuity


def goal() -> GoalContract:
    return GoalContract(
        GOAL_SCHEMA,
        "goal-main",
        1,
        None,
        "Survive restart without stale authority.",
        "Persist one bounded working set.",
        ("CAS and continuation pass.",),
        ("M2-A3 and M2-A4.",),
        ("No provider or memory plugin.",),
        ("Repository root is explicit.",),
        (),
        "acknowledged",
    )


def initialized(root: Path) -> tuple[WorkingSetRepository, WorkingContinuityService]:
    repository = WorkingSetRepository.initialize(root)
    return repository, WorkingContinuityService(repository)


def create_current(root: Path):
    repository, service = initialized(root)
    current = service.create(
        VirtualSessionIdentity.initial("session-main", "agent-main", "route-main"),
        goal(),
        progress=("created",),
        evidence_refs=("evidence-1",),
    )
    return repository, service, current


def test_create_reopen_and_read_only_status_do_not_mutate(tmp_path: Path) -> None:
    absent = tmp_path / "absent"
    before = set(tmp_path.iterdir())
    assert inspect_working_continuity(absent) == {
        "schema_version": "ahc.working-continuity-status.v1",
        "status": "not-initialized",
        "current_count": 0,
    }
    assert set(tmp_path.iterdir()) == before

    root = tmp_path / "working"
    _, _, current = create_current(root)
    files_before = {path.name: path.read_bytes() for path in root.iterdir() if path.is_file()}
    reopened = WorkingSetRepository(root)
    assert reopened.read_current("session-main") == current
    assert inspect_working_continuity(root) == {
        "schema_version": "ahc.working-continuity-status.v1",
        "status": "ready",
        "current_count": 1,
        "snapshot_count": 1,
    }
    files_after = {path.name: path.read_bytes() for path in root.iterdir() if path.is_file()}
    assert files_after == files_before
    assert set(files_after) == {WORKING_DB_FILENAME, WORKING_LOCK_FILENAME}


def test_stale_and_wrong_owner_writes_leave_history_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, service, initial = create_current(root)
    accepted = service.advance(initial, progress=("created", "accepted"))

    with pytest.raises(WorkingSetConflict):
        service.advance(initial, progress=("stale",))
    assert repository.read_current("session-main") == accepted
    assert len(repository.history("session-main")) == 2

    wrong_repository = WorkingSetRepository(root)
    wrong_session = VirtualSessionIdentity.initial("session-main", "agent-other", "route-main")
    wrong = WorkingContinuityService(MemoryCreateOnly()).create(wrong_session, goal())
    with pytest.raises(WorkingSetConflict):
        wrong_repository.append_snapshot(wrong, wrong.revise(progress=("wrong",)))

    wrong_session_id = VirtualSessionIdentity.initial("session-other", "agent-main", "route-main")
    wrong_id = WorkingContinuityService(MemoryCreateOnly()).create(wrong_session_id, goal())
    with pytest.raises(WorkingSetConflict):
        wrong_repository.append_snapshot(wrong_id, wrong_id.revise(progress=("wrong-id",)))

    wrong_generation = initial.continue_as(initial.session.successor(), "compaction")
    with pytest.raises(WorkingSetConflict):
        wrong_repository.append_snapshot(
            wrong_generation, wrong_generation.revise(progress=("wrong-generation",))
        )
    assert len(repository.history("session-main")) == 2


def test_equal_create_is_idempotent_and_conflicting_create_rejects(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, _, initial = create_current(root)
    assert repository.create_snapshot(initial) == initial

    conflicting = WorkingContinuityService(MemoryCreateOnly()).create(
        initial.session, goal(), progress=("different",)
    )
    with pytest.raises(WorkingSetConflict):
        repository.create_snapshot(conflicting)
    assert repository.history("session-main") == (initial,)


class MemoryCreateOnly:
    def create(self, snapshot):
        return snapshot

    def read_current(self, virtual_session_id):
        raise AssertionError

    def append(self, expected, successor):
        raise AssertionError


def test_two_writers_from_same_revision_have_one_winner(tmp_path: Path) -> None:
    root = tmp_path / "working"
    _, _, initial = create_current(root)
    barrier = Barrier(2)

    def mutate(label: str) -> str:
        repository = WorkingSetRepository(root)
        successor = initial.revise(progress=(label,))
        barrier.wait()
        try:
            repository.append_snapshot(initial, successor)
            return "accepted"
        except WorkingSetConflict:
            return "conflict"
        except WorkingWriterBusy:
            return "busy"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(mutate, ("one", "two")))
    assert results.count("accepted") == 1
    assert len(WorkingSetRepository(root).history("session-main")) == 2


def test_writer_lock_is_nonblocking_and_writes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, _, initial = create_current(root)
    successor = initial.revise(progress=("blocked",))

    with portalocker.Lock(
        str(root / WORKING_LOCK_FILENAME),
        flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        timeout=0,
    ):
        with pytest.raises(WorkingWriterBusy):
            repository.append_snapshot(initial, successor)
    assert repository.history("session-main") == (initial,)


def test_external_sqlite_writer_busy_is_typed_and_writes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, _, initial = create_current(root)
    successor = initial.revise(progress=("external-busy",))
    external = sqlite3.connect(root / WORKING_DB_FILENAME, isolation_level=None, timeout=0.0)
    external.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(WorkingWriterBusy):
            repository.append_snapshot(initial, successor)
    finally:
        external.rollback()
        external.close()
    assert repository.history("session-main") == (initial,)

    external = sqlite3.connect(root / WORKING_DB_FILENAME, isolation_level=None, timeout=0.0)
    external.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(WorkingWriterBusy):
            WorkingSetRepository.initialize(root)
    finally:
        external.rollback()
        external.close()
    assert repository.history("session-main") == (initial,)


def test_wrapped_busy_while_opening_mutation_is_typed_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "working"
    repository, _, initial = create_current(root)
    successor = initial.revise(progress=("wrapped-open-busy",))
    real_open = working_repository_module.open_connection

    def busy_open(*args: object, **kwargs: object) -> sqlite3.Connection:
        busy = sqlite3.OperationalError("injected busy")
        busy.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise WorkingDatabaseError("injected open failure") from busy

    monkeypatch.setattr(working_repository_module, "open_connection", busy_open)
    with pytest.raises(WorkingWriterBusy):
        repository.append_snapshot(initial, successor)
    monkeypatch.setattr(working_repository_module, "open_connection", real_open)
    assert repository.history("session-main") == (initial,)


def test_sqlite_backed_state_machine_persists_only_allowed_transitions(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, service, initial = create_current(root)
    interrupted = service.interrupt(initial, "timeout")
    resumed = service.resume(interrupted)
    cancelled = service.cancel(resumed)
    assert WorkingSetRepository(root).read_current("session-main") == cancelled
    assert len(repository.history("session-main")) == 4

    before = repository.history("session-main")
    with pytest.raises(ContractError):
        service.complete(cancelled)
    assert repository.history("session-main") == before


def test_compaction_and_rollover_fence_old_generations(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, service, initial = create_current(root)
    compacted = service.continue_generation(initial, initial.session.successor(), "compaction")
    assert WorkingSetRepository(root).read_current("session-main") == compacted

    with pytest.raises(WorkingSetConflict):
        service.advance(initial, progress=("stale-old-generation",))

    rolled = service.continue_generation(
        compacted, compacted.session.successor(), "backend-rollover"
    )
    assert rolled.session.ref.generation == 3
    assert rolled.progress == initial.progress
    assert rolled.evidence_refs == initial.evidence_refs
    assert len(repository.history("session-main")) == 3


def test_precommit_failure_rolls_back_snapshot_and_pointer(tmp_path: Path) -> None:
    root = tmp_path / "working"
    repository, _, initial = create_current(root)
    successor = initial.revise(progress=("not-committed",))

    with pytest.raises(WorkingRepositoryError, match="injected"):
        repository.append_snapshot(initial, successor, fail_before_commit=True)

    reopened = WorkingSetRepository(root)
    assert reopened.read_current("session-main") == initial
    assert reopened.history("session-main") == (initial,)


def test_null_wire_digest_in_database_is_corrupt_and_not_repaired(tmp_path: Path) -> None:
    root = tmp_path / "working"
    _, _, current = create_current(root)
    db_path = root / WORKING_DB_FILENAME
    payload = current.to_dict()
    payload["snapshot_digest"] = None
    connection = sqlite3.connect(db_path)
    connection.execute(
        "UPDATE snapshots SET snapshot_json=?",
        (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),),
    )
    connection.commit()
    connection.close()

    before = db_path.read_bytes()
    with pytest.raises(WorkingSetCorrupt):
        WorkingSetRepository(root).read_current("session-main")
    assert inspect_working_continuity(root)["status"] == "degraded"
    assert db_path.read_bytes() == before


def test_corrupt_snapshot_and_unsupported_schema_are_degraded(tmp_path: Path) -> None:
    root = tmp_path / "working"
    create_current(root)
    db_path = root / WORKING_DB_FILENAME

    connection = sqlite3.connect(db_path)
    connection.execute("UPDATE snapshots SET snapshot_json='{}'")
    connection.commit()
    connection.close()

    with pytest.raises(WorkingSetCorrupt):
        WorkingSetRepository(root).read_current("session-main")
    assert inspect_working_continuity(root)["status"] == "degraded"

    other = tmp_path / "unsupported"
    WorkingSetRepository.initialize(other)
    connection = sqlite3.connect(other / WORKING_DB_FILENAME)
    connection.execute("PRAGMA user_version=99")
    connection.commit()
    connection.close()
    assert inspect_working_continuity(other)["status"] == "degraded"


def test_symlink_root_database_and_lock_are_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    root_link = tmp_path / "root-link"
    try:
        root_link.symlink_to(target, target_is_directory=True)
    except OSError as error:
        if getattr(error, "winerror", None) == 1314:
            pytest.skip("Windows token cannot create symbolic links; covered on native Linux")
        raise
    with pytest.raises(WorkingRepositoryError):
        WorkingSetRepository.initialize(root_link)
    assert inspect_working_continuity(root_link)["status"] == "degraded"

    nested_link = root_link / "nested"
    with pytest.raises(WorkingRepositoryError):
        WorkingSetRepository.initialize(nested_link)
    assert not (target / "nested").exists()
    assert inspect_working_continuity(nested_link)["status"] == "degraded"

    db_root = tmp_path / "db-root"
    db_root.mkdir()
    outside = tmp_path / "outside.db"
    outside.write_bytes(b"outside")
    (db_root / WORKING_DB_FILENAME).symlink_to(outside)
    with pytest.raises(WorkingRepositoryError):
        WorkingSetRepository.initialize(db_root)
    assert inspect_working_continuity(db_root)["status"] == "degraded"

    lock_root = tmp_path / "lock-root"
    repository, _, initial = create_current(lock_root)
    (lock_root / WORKING_LOCK_FILENAME).unlink()
    outside_lock = tmp_path / "outside.lock"
    outside_lock.write_bytes(b"outside")
    (lock_root / WORKING_LOCK_FILENAME).symlink_to(outside_lock)
    with pytest.raises(WorkingRepositoryError):
        repository.append_snapshot(initial, initial.revise(progress=("blocked",)))
    assert inspect_working_continuity(lock_root)["status"] == "degraded"


def test_nonregular_artifacts_are_degraded_and_relative_root_is_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root_file = tmp_path / "root-file"
    root_file.write_bytes(b"not-a-directory")
    assert inspect_working_continuity(root_file)["status"] == "degraded"

    db_directory_root = tmp_path / "db-directory"
    db_directory_root.mkdir()
    (db_directory_root / WORKING_DB_FILENAME).mkdir()
    with pytest.raises(WorkingRepositoryError):
        WorkingSetRepository.initialize(db_directory_root)
    assert inspect_working_continuity(db_directory_root)["status"] == "degraded"

    monkeypatch.chdir(tmp_path)
    relative_repository, relative_service = initialized(Path("relative-root"))
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.chdir(other)
    current = relative_service.create(
        VirtualSessionIdentity.initial("session-relative", "agent-main", "route-main"), goal()
    )
    assert relative_repository.root == (tmp_path / "relative-root").absolute()
    assert relative_repository.read_current("session-relative") == current
