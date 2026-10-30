"""Native identity contracts over the real repository, with controlled OS peers."""

from dataclasses import asdict, replace

import pytest
from skill_test_support import goal

from ahc.domain.native_process_identity import (
    NativeCreationAttempt,
    NativeProcessIdentity,
    NativeProcessObservation,
    attempt_from_dict,
    identity_from_dict,
)
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.backend_custody import BackendCustody, BackendCustodyUnavailable
from ahc.infrastructure.runtime_repository import RuntimeRepository


def native_identity(letter="b"):
    return NativeProcessIdentity(
        "a" * 32, "12345678-1234-1234-1234-123456789abc", "ahc-native-test.service",
        "c" * 32, "/user.slice/ahc-native-test.service/ahc-process-" + letter * 32,
        30, 1234)


def setup(tmp_path, role="backend"):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("session", "agent", "route"), goal()))
    workspace = store.reserve_workspace_generation(current, "work")
    custody = BackendCustody(store, resource_role=role)
    lease = custody.acquire(current.session, "work")
    return store, custody, lease, workspace


class Runner:
    def __init__(self, store, lease, *, lost=False, letter="b"):
        self.store, self.lease = store, lease
        self.identity = native_identity(letter)
        self.lost = lost
        self.running = False
        self.creates = self.stops = 0

    def prepare_creation(self, command, *, lifetime_seconds):
        return NativeCreationAttempt(self.identity)

    def create_attempt(self, attempt, command, *, lifetime_seconds):
        with self.store._read() as connection:
            assert connection.execute("SELECT state FROM backend_creation_intents WHERE owner_key=?",
                                      (self.lease.key,)).fetchone()[0] == "pending"
        assert attempt.identity == self.identity
        self.creates += 1
        self.running = True
        if self.lost:
            raise RuntimeError("lost creation acknowledgement")
        return self.identity

    def reconcile_creation(self, attempt):
        assert attempt.identity == self.identity
        return self.identity

    def observe(self, identity):
        return NativeProcessObservation(self.identity, "running" if self.running else "stopped",
                                        not self.running)

    def stop(self, identity):
        assert identity == self.identity
        self.stops += 1
        self.running = False


def test_native_identity_and_attempt_roundtrip_without_fake_unit():
    identity = native_identity()
    assert identity_from_dict(asdict(identity)) == identity
    assert attempt_from_dict(asdict(NativeCreationAttempt(identity))).identity == identity
    assert "unit" not in asdict(identity) and identity.kind == "linux-cgroup.v1"


@pytest.mark.parametrize("change", [
    {"inode": True}, {"device": -1}, {"kind": "unit"}, {"invocation_id": "0" * 32},
    {"cgroup": "/user.slice/ahc-native-test.service/../ahc-process-" + "b" * 32},
    {"service": "other.service"}, {"cgroup": "//user.slice/ahc-native-test.service/ahc-process-" + "b" * 32},
])
def test_invalid_native_identity_rejects(change):
    with pytest.raises(ContractError):
        replace(native_identity(), **change)


@pytest.mark.parametrize("role", ["backend", "python-workspace"])
def test_pre_effect_intent_then_full_identity_attachment(tmp_path, role):
    store, custody, lease, workspace = setup(tmp_path, role)
    runner = Runner(store, lease)
    try:
        identity = custody.create(lease, workspace, runner, ("controlled",), lifetime_seconds=30)
        with custody.attachment(lease, identity):
            assert custody._held(lease)[0][2] == identity
        with pytest.raises(ContractError):
            with custody.attachment(lease, replace(identity, inode=999)):
                pytest.fail("foreign identity attached")
        assert custody.recover(lease, runner) == (identity,)
        assert runner.stops == 1 and custody._held(lease) == []
    finally:
        lease.close()


def test_lost_create_ack_reconciles_without_replay_then_new_work(tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    runner = Runner(store, lease, lost=True)
    try:
        with pytest.raises(RuntimeError, match="lost creation"):
            custody.create(lease, workspace, runner, ("controlled",), lifetime_seconds=30)
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            custody.create(lease, workspace, runner, ("controlled",), lifetime_seconds=30)
        assert custody.recover(lease, runner) == (runner.identity,)
        assert runner.creates == 1 and runner.stops == 1
        successor = Runner(store, lease, letter="d")
        identity = custody.create(lease, workspace, successor, ("different",), lifetime_seconds=30)
        assert identity == successor.identity and successor.creates == 1
        custody.recover(lease, successor)
    finally:
        lease.close()


def test_replacement_observation_never_authorizes_stop(tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    runner = Runner(store, lease)
    try:
        custody.create(lease, workspace, runner, ("controlled",), lifetime_seconds=30)
        runner.identity = replace(runner.identity, inode=999)
        with pytest.raises(BackendCustodyUnavailable, match="identity"):
            custody.recover(lease, runner)
        assert runner.stops == 0 and custody._held(lease)
    finally:
        lease.close()


def test_failed_intent_commit_does_not_launch(tmp_path, monkeypatch):
    store, custody, lease, workspace = setup(tmp_path)
    runner = Runner(store, lease)
    try:
        def refuse(_write):
            raise RuntimeError("durable write refused")
        monkeypatch.setattr(store, "_mutate", refuse)
        with pytest.raises(RuntimeError, match="durable write"):
            custody.create(lease, workspace, runner, ("controlled",), lifetime_seconds=30)
        assert runner.creates == 0
    finally:
        lease.close()
