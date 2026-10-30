"""Persisted ownership and failed-cleanup behavior, without native execution."""

import threading
from dataclasses import replace

import pytest
from skill_test_support import goal

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.backend_custody import (
    BackendCustody,
    BackendCustodyUnavailable,
    LinuxUnitIdentity,
    UnitObservation,
)
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt, WorkingWriterBusy


def setup(tmp_path):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = repo.create(
        WorkingSetSnapshot.initial(
            VirtualSessionIdentity.initial("session", "agent", "route"), goal()
        )
    )
    workspace = repo.reserve_workspace_generation(snapshot, "work")
    custody = BackendCustody(repo)
    lease = custody.acquire(snapshot.session, "work")
    unit = LinuxUnitIdentity(
        "AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "a" * 32 + ".service",
        "b" * 32,
    )
    custody.record_created(lease, workspace, unit)
    return repo, custody, lease, workspace, unit


class Runner:
    def __init__(self, unit, *, changed=None, fail_stop=False):
        self.unit, self.changed, self.fail_stop = unit, changed, fail_stop
        self.stopped = False
        self.calls = []

    def observe(self, unit):
        self.calls.append("observe")
        assert unit == self.unit
        value = UnitObservation(
            unit.distribution,
            unit.boot_id,
            unit.unit,
            unit.invocation_id,
            "stopped" if self.stopped else "running",
            self.stopped,
        )
        return replace(value, **self.changed) if self.changed else value

    def stop(self, unit):
        assert unit == self.unit
        self.calls.append("stop")
        if not self.fail_stop:
            self.stopped = True


class IndeterminateStopRunner(Runner):
    def __init__(self, unit, *, terminal, after_change=None, after_error=None):
        super().__init__(unit)
        self.terminal = terminal
        self.after_change, self.after_error = after_change, after_error
        self.stop_error = BackendCustodyUnavailable("stop response unverified")

    def observe(self, unit):
        value = super().observe(unit)
        if self.stopped:
            if self.after_error is not None:
                raise self.after_error
            if self.after_change is not None:
                return replace(value, **self.after_change)
        return value

    def stop(self, unit):
        assert unit == self.unit
        self.calls.append("stop")
        self.stopped = self.terminal
        raise self.stop_error


def held_count(repo):
    with repo._read() as c:
        return c.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0]


def test_live_owner_excludes_other_controller_and_released_owner_cannot_recover(tmp_path):
    repo, custody, lease, workspace, unit = setup(tmp_path)
    other = BackendCustody(RuntimeRepository(repo.root))
    try:
        with pytest.raises(WorkingWriterBusy):
            other.acquire(workspace.session, "work")
    finally:
        lease.close()
    runner = Runner(unit)
    with pytest.raises(ContractError):
        custody.recover(lease, runner)
    assert runner.calls == []


def test_reopen_reconciles_exact_identity_once(tmp_path):
    repo, _, lease, workspace, unit = setup(tmp_path)
    lease.close()
    custody = BackendCustody(RuntimeRepository(repo.root))
    current = custody.acquire(workspace.session, "work")
    try:
        runner = Runner(unit)
        assert custody.recover(current, runner) == (unit,)
        assert runner.calls == ["observe", "stop", "observe"]
        assert held_count(repo) == 0
        assert custody.recover(current, runner) == ()
        assert len(runner.calls) == 3
        with pytest.raises(ContractError, match="rebound"):
            custody.record_created(current, workspace, unit)
    finally:
        current.close()


@pytest.mark.parametrize(
    "changed",
    [
        {"state": "unknown"},
        {"invocation_id": "c" * 32},
        {"boot_id": "87654321-1234-1234-1234-123456789abc"},
        {"distribution": "Ubuntu-24.04"},
        {"unit": "other.service"},
    ],
)
def test_unverified_or_replaced_unit_never_stopped(tmp_path, changed):
    repo, custody, lease, _, unit = setup(tmp_path)
    runner = Runner(unit, changed=changed)
    try:
        with pytest.raises(BackendCustodyUnavailable):
            custody.recover(lease, runner)
        assert runner.calls == ["observe"]
        assert held_count(repo) == 1
    finally:
        lease.close()


def test_failed_cleanup_keeps_durable_record(tmp_path):
    repo, custody, lease, _, unit = setup(tmp_path)
    try:
        with pytest.raises(BackendCustodyUnavailable):
            custody.recover(lease, Runner(unit, fail_stop=True))
        assert held_count(repo) == 1
        assert custody.recover(lease, Runner(unit)) == (unit,)
    finally:
        lease.close()


def test_unverified_stop_reconciles_only_proven_terminal_unit(tmp_path):
    repo, custody, lease, _, unit = setup(tmp_path)
    runner = IndeterminateStopRunner(unit, terminal=True)
    try:
        assert custody.recover(lease, runner) == (unit,)
        assert runner.calls == ["observe", "stop", "observe"]
        assert held_count(repo) == 0
    finally:
        lease.close()


@pytest.mark.parametrize(
    "terminal,after_change,after_error",
    [
        (False, None, None),
        (True, {"invocation_id": "c" * 32}, None),
        (True, None, BackendCustodyUnavailable("post-stop observation unavailable")),
    ],
)
def test_unverified_stop_preserves_custody_when_reconciliation_is_not_terminal(
    tmp_path, terminal, after_change, after_error
):
    repo, custody, lease, _, unit = setup(tmp_path)
    runner = IndeterminateStopRunner(
        unit, terminal=terminal, after_change=after_change, after_error=after_error
    )
    try:
        with pytest.raises(BackendCustodyUnavailable) as caught:
            custody.recover(lease, runner)
        assert caught.value is runner.stop_error
        assert runner.calls == ["observe", "stop", "observe"]
        assert held_count(repo) == 1
    finally:
        lease.close()


def test_record_corruption_rejected_before_native_observation(tmp_path):
    repo, custody, lease, _, unit = setup(tmp_path)
    repo._mutate(lambda c: c.execute("UPDATE backend_custody SET record_digest='corrupt'"))
    runner = Runner(unit)
    try:
        with pytest.raises(WorkingSetCorrupt):
            custody.recover(lease, runner)
        assert runner.calls == []
    finally:
        lease.close()


def test_cannot_rebind_held_unit_to_another_invocation(tmp_path):
    _, custody, lease, workspace, unit = setup(tmp_path)
    try:
        with pytest.raises(ContractError, match="rebound"):
            custody.record_created(lease, workspace, replace(unit, invocation_id="d" * 32))
    finally:
        lease.close()


def test_arbitrary_system_unit_cannot_be_owned():
    with pytest.raises(ContractError, match="namespace"):
        LinuxUnitIdentity("AHC", "12345678-1234-1234-1234-123456789abc", "ssh.service", "b" * 32)


def test_absent_unit_without_cgroup_evidence_remains_held(tmp_path):
    repo, custody, lease, _, unit = setup(tmp_path)
    runner = Runner(unit, changed={"state": "absent", "invocation_id": None, "cgroup_empty": None})
    try:
        with pytest.raises(BackendCustodyUnavailable):
            custody.recover(lease, runner)
        assert runner.calls == ["observe"]
        assert held_count(repo) == 1
    finally:
        lease.close()


def test_lease_release_waits_for_inflight_recovery(tmp_path):
    repo, custody, lease, workspace, unit = setup(tmp_path)
    entered, proceed, close_requested, closed = [threading.Event() for _ in range(4)]
    failures = []

    class PausedRunner(Runner):
        def observe(self, value):
            if not self.calls:
                entered.set()
                assert proceed.wait(5)
            return super().observe(value)

    def recover():
        try:
            custody.recover(lease, PausedRunner(unit))
        except BaseException as exc:
            failures.append(exc)

    def close():
        close_requested.set()
        lease.close()
        closed.set()

    worker = threading.Thread(target=recover)
    closer = threading.Thread(target=close)
    worker.start()
    try:
        assert entered.wait(5)
        closer.start()
        assert close_requested.wait(5)
        assert not closed.wait(0.05)
        with pytest.raises(WorkingWriterBusy):
            BackendCustody(RuntimeRepository(repo.root)).acquire(workspace.session, "work")
    finally:
        proceed.set()
        worker.join(5)
        if closer.ident is not None:
            closer.join(5)
        lease.close()
    assert not failures and closed.is_set()
    assert held_count(repo) == 0


def ready_to_create(tmp_path):
    repo, custody, lease, workspace, unit = setup(tmp_path)
    custody.recover(lease, Runner(unit))
    return (
        repo,
        custody,
        lease,
        workspace,
        replace(unit, unit="ahc-backend-" + "c" * 32 + ".service"),
    )


def test_creation_intent_precedes_effect_and_success_resolves_atomically(tmp_path):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)

    class Creator:
        def create(self, command, *, lifetime_seconds):
            with repo._read() as c:
                assert (
                    c.execute("SELECT state FROM backend_creation_intents").fetchone()[0]
                    == "pending"
                )
            return unit

    try:
        assert (
            custody.create(lease, workspace, Creator(), ["/bin/sleep", "1"], lifetime_seconds=1)
            == unit
        )
        with repo._read() as c:
            assert (
                c.execute("SELECT state FROM backend_creation_intents").fetchone()[0] == "recorded"
            )
        assert held_count(repo) == 1
        assert custody.recover(lease, Runner(unit)) == (unit,)
    finally:
        lease.close()


def test_lost_creation_survives_reopen_and_blocks_replay_and_name_cleanup(tmp_path):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)

    class LostCreator:
        def create(self, *args, **kwargs):
            raise OSError("response lost")

    with pytest.raises(OSError):
        custody.create(lease, workspace, LostCreator(), ["/bin/sleep", "1"], lifetime_seconds=1)
    lease.close()
    reopened = BackendCustody(RuntimeRepository(repo.root))
    lease = reopened.acquire(workspace.session, "work")
    runner = Runner(unit)
    try:
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            reopened.recover(lease, runner)
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            reopened.create(
                lease, workspace, LostCreator(), ["/bin/sleep", "1"], lifetime_seconds=1
            )
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            reopened.record_created(lease, workspace, unit)
        assert not runner.calls and held_count(repo) == 0
    finally:
        lease.close()


def test_identity_and_intent_resolution_rollback_together(tmp_path):
    from ahc.infrastructure.working_repository import WorkingRepositoryError

    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    repo._mutate(
        lambda c: c.execute(
            "CREATE TRIGGER fail_resolution BEFORE UPDATE ON backend_creation_intents "
            "WHEN NEW.state='recorded' BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    )

    class Creator:
        def create(self, *args, **kwargs):
            return unit

    try:
        with pytest.raises(WorkingRepositoryError, match="mutation failed"):
            custody.create(lease, workspace, Creator(), ["/bin/sleep", "1"], lifetime_seconds=1)
        assert held_count(repo) == 0
        with repo._read() as c:
            assert (
                c.execute("SELECT state FROM backend_creation_intents").fetchone()[0] == "pending"
            )
    finally:
        lease.close()


def test_controller_exit_keeps_intent_and_releases_owner_lease(tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path

    repo, _, lease, workspace, unit = ready_to_create(tmp_path)
    lease.close()
    script = """
import sys, json, os
sys.path.insert(0, sys.argv[1])
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.backend_custody import BackendCustody
workspace = WorkspaceIdentity.from_dict(json.loads(sys.argv[3]))
custody = BackendCustody(RuntimeRepository(sys.argv[2]))
lease = custody.acquire(workspace.session, workspace.workspace_id)
class ExitCreator:
    def create(self, *args, **kwargs): os._exit(23)
custody.create(lease, workspace, ExitCreator(), ['/bin/sleep', '1'], lifetime_seconds=1)
"""
    child = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            script,
            str(Path(__file__).parents[1] / "src"),
            str(repo.root),
            json.dumps(workspace.to_dict()),
        ],
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == 23, child.stderr
    reopened = BackendCustody(RuntimeRepository(repo.root))
    lease = reopened.acquire(workspace.session, "work")
    runner = Runner(unit)
    try:
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            reopened.recover(lease, runner)
        assert runner.calls == []
    finally:
        lease.close()
