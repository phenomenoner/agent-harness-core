"""Portable checkpoint durability, current-owner fences and corruption detection."""

import sqlite3
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceCheckpoint, WorkspaceIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import (
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
)


def checkpoint(snapshot, values='{"x":42}'):
    return WorkspaceCheckpoint(WorkspaceIdentity(snapshot.session, "work", 1, 2), values)


def test_exact_checkpoint_survives_reopen_without_mutating_working_revision(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)
    assert repo.save_workspace_checkpoint(snapshot, value) == value.digest
    assert repo.save_workspace_checkpoint(snapshot, value) == value.digest
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_workspace_checkpoint(snapshot.session, "work", value.digest) == value
    assert reopened.read_current("session") == snapshot
    assert reopened.read_workspace_checkpoint(snapshot.session, "work", "a" * 64) is None
    with pytest.raises(ContractError):
        repo.save_workspace_checkpoint(snapshot, checkpoint(snapshot, '{"x":99}'))


def test_checkpoint_rollback_leaves_no_partial_record(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)
    with pytest.raises(WorkingRepositoryError, match="injected"):
        repo.save_workspace_checkpoint(snapshot, value, fail_before_commit=True)
    assert (
        RuntimeRepository(repo.root).read_workspace_checkpoint(
            snapshot.session, "work", value.digest
        )
        is None
    )


def test_foreign_checkpoint_cannot_cross_session_or_workspace(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)
    repo.save_workspace_checkpoint(snapshot, value)
    with pytest.raises(ContractError):
        repo.read_workspace_checkpoint(snapshot.session, "other", value.digest)
    foreign = replace(value.origin, session=replace(snapshot.session, agent_id="other"))
    with pytest.raises(ContractError):
        repo.save_workspace_checkpoint(snapshot, WorkspaceCheckpoint(foreign, value.values_json))


def test_cancelled_owner_cannot_save_or_restore_checkpoint(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)
    repo.save_workspace_checkpoint(snapshot, value)
    WorkingContinuityService(repo).cancel(snapshot)
    with pytest.raises(WorkingSetConflict):
        repo.save_workspace_checkpoint(snapshot, value)
    with pytest.raises(WorkingSetConflict):
        repo.read_workspace_checkpoint(snapshot.session, "work", value.digest)


@pytest.mark.parametrize(
    "column,value",
    [
        ("values_json", '{"x":99}'),
        ("origin_json", "{}"),
        ("revision", 8),
    ],
)
def test_corrupt_checkpoint_is_not_restorable(tmp_path, column, value):
    repo, snapshot, _ = setup(tmp_path)
    original = checkpoint(snapshot)
    repo.save_workspace_checkpoint(snapshot, original)
    with sqlite3.connect(repo.db_path) as connection:
        connection.execute(f"UPDATE workspace_checkpoints SET {column}=?", (value,))
    with pytest.raises(WorkingSetCorrupt):
        repo.read_workspace_checkpoint(snapshot.session, "work", original.digest)


def test_corrupt_duplicate_cannot_be_acknowledged_as_saved(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)
    repo.save_workspace_checkpoint(snapshot, value)
    with sqlite3.connect(repo.db_path) as connection:
        connection.execute("UPDATE workspace_checkpoints SET values_json=?", ('{"x":99}',))
    with pytest.raises(WorkingSetCorrupt):
        repo.save_workspace_checkpoint(snapshot, value)


def test_runtime_checkpoint_uses_exact_live_port_and_current_owner(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    value = checkpoint(snapshot)

    class Port:
        identity = value.origin

        def checkpoint(self, expected, names):
            assert expected == self.identity and names == ("x",)
            return value

    digest = RuntimeService(repo, None).checkpoint_workspace(snapshot, Port(), ("x",))
    assert repo.read_workspace_checkpoint(snapshot.session, "work", digest) == value
