"""Portable admission composition; these peers do not prove Linux containment."""

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from test_native_process_custody import native_identity

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError


@pytest.fixture
def peers(monkeypatch, tmp_path):
    from ahc.infrastructure.process import linux_workspace_boundary

    created, spawned = [], []

    class Boundary:
        def __init__(self):
            self.workspace, self.calls = tmp_path, []
            self.refused, self.prefix = False, ('/qualified/bwrap', '--',)
            self.scratch_limits = None

        def command(self, command):
            self.calls.append(command)
            if self.refused:
                raise ContractError('boundary refused')
            return self.prefix + command

    class Group:
        complete = False
        identity = native_identity()

        def __init__(self):
            created.append(self)
            self.limit_events = []

        def cleanup(self, *_):
            self.complete = True

        def configure_limits(self, limits):
            self.limit_events.append(('configure', limits))

        def verify_limits(self, limits):
            self.limit_events.append(('verify', limits))

    def spawn(command, **kwargs):
        spawned.append((command, kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(linux_workspace_boundary, 'LinuxWorkspaceBoundary', Boundary)
    native_peer = ModuleType('ahc.infrastructure.process.linux_native')
    native_peer.LinuxProcessGroup = Group
    native_peer.spawn_native = spawn
    monkeypatch.setitem(sys.modules, native_peer.__name__, native_peer)
    command = (str(tmp_path / 'python'), '-I', '-c', 'pass')
    return Boundary(), created, spawned, command


def runner(boundary, **overrides):
    return NativeProcessRunner(cwd=str(boundary.workspace), environment={}, persistent=True,
                               owner_lifetime=True, boundary=boundary, **overrides)


@pytest.mark.parametrize('missing', ['limits', 'zero-swap', 'deadline'])
def test_ephemeral_runner_refuses_unbounded_resources_before_group_creation(peers, missing):
    boundary, groups, spawned, _ = peers
    boundary.scratch_limits = NativeScratchLimits(2 * 1024**2, 2 * 1024**2)
    limits = (None if missing == 'limits' else NativeResourceLimits(
        128 * 1024**2, 16, 50000, swap_bytes=65536 if missing == 'zero-swap' else 0))
    with pytest.raises(ContractError, match='zero swap and a deadline'):
        NativeProcessRunner(cwd=str(boundary.workspace), environment={}, persistent=True,
                            boundary=boundary, resource_limits=limits,
                            owner_lifetime=missing == 'deadline')
    assert not groups and not spawned


def test_ephemeral_runner_uses_existing_lifetime_timer_and_resource_custody(peers, monkeypatch):
    from ahc.adapters import native_process_runner as adapter

    boundary, groups, spawned, command = peers
    boundary.scratch_limits = NativeScratchLimits(2 * 1024**2, 2 * 1024**2)
    started = []
    monkeypatch.setattr(adapter.threading, 'Timer', lambda seconds, callback, args:
        SimpleNamespace(start=lambda: started.append((seconds, callback, args)), daemon=False))
    limits = NativeResourceLimits(128 * 1024**2, 16, 50000)
    owner = NativeProcessRunner(cwd=str(boundary.workspace), environment={}, persistent=True,
                                boundary=boundary, resource_limits=limits)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert groups[0].limit_events == [('configure', limits), ('verify', limits)]
    assert len(spawned) == 1 and len(started) == 1 and started[0][0] == 10
    assert started[0][2] == (owner._owned,)


def test_ephemeral_deadline_cannot_be_removed_after_reservation(peers):
    boundary, groups, spawned, command = peers
    boundary.scratch_limits = NativeScratchLimits(2 * 1024**2, 2 * 1024**2)
    owner = NativeProcessRunner(cwd=str(boundary.workspace), environment={}, persistent=True,
        boundary=boundary, resource_limits=NativeResourceLimits(128 * 1024**2, 16, 50000))
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    owner.owner_lifetime = True
    with pytest.raises(ContractError, match='deadline'):
        owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert len(groups) == 1 and not spawned and owner._attempt == attempt


def test_wrapper_is_used_under_original_command_attempt_and_existing_group(peers):
    boundary, groups, spawned, command = peers
    owner = runner(boundary)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    assert owner.create_attempt(attempt, command, lifetime_seconds=10) == attempt.identity
    assert boundary.calls == [command, command]
    assert spawned == [(boundary.prefix + command,
                        {'cwd': str(boundary.workspace), 'env': {},
                         'protocol_pipes': True, 'group': groups[0]})]
    assert owner._command == command


def test_boundary_refusal_allocates_no_group_and_never_launches(peers):
    boundary, groups, spawned, command = peers
    boundary.refused = True
    with pytest.raises(ContractError, match='boundary refused'):
        runner(boundary).prepare_creation(command, lifetime_seconds=10)
    assert groups == spawned == []


@pytest.mark.parametrize('change', ['refused', 'different-wrapper'])
def test_changed_boundary_after_reservation_has_no_unconfined_fallback(peers, change):
    boundary, groups, spawned, command = peers
    owner = runner(boundary)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    if change == 'refused':
        boundary.refused = True
    else:
        boundary.prefix = ('/different/wrapper', '--')
    with pytest.raises(ContractError):
        owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert spawned == [] and len(groups) == 1
    assert owner._attempt == attempt  # Original custody remains available for reconciliation.


def test_optional_boundary_does_not_change_an_ordinary_runner(peers):
    boundary, _, spawned, command = peers
    owner = NativeProcessRunner(cwd=str(boundary.workspace), environment={'KEY': 'fixture'},
                                persistent=True, owner_lifetime=True)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert spawned[0][0] == command and spawned[0][1]['env'] == {'KEY': 'fixture'}


@pytest.mark.parametrize('change', ['environment', 'workspace'])
def test_reserved_boundary_rechecks_mutable_runner_configuration(peers, change):
    boundary, _, spawned, command = peers
    owner = runner(boundary)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    if change == 'environment':
        owner.environment['LD_PRELOAD'] = 'changed'
    else:
        owner.cwd = str(boundary.workspace / 'different')
    with pytest.raises(ContractError, match='configuration changed'):
        owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert not spawned


@pytest.mark.parametrize('change', ['environment', 'workspace', 'fake-boundary'])
def test_boundary_cannot_inherit_host_environment_or_another_work_root(peers, change):
    boundary, _, spawned, _ = peers
    kwargs = {'cwd': str(boundary.workspace), 'environment': {}, 'boundary': boundary}
    if change == 'environment':
        kwargs['environment'] = {'LD_PRELOAD': 'untrusted'}
    elif change == 'workspace':
        kwargs['cwd'] = str(Path(boundary.workspace) / 'other')
    else:
        kwargs['boundary'] = SimpleNamespace(workspace=boundary.workspace)
    with pytest.raises(ContractError, match='isolated runner'):
        NativeProcessRunner(**kwargs)
    assert not spawned


def test_limits_are_configured_before_attempt_and_verified_before_spawn(peers):
    boundary, groups, spawned, command = peers
    limits = NativeResourceLimits(128 * 1024**2, 16, 50000)
    owner = runner(boundary, resource_limits=limits)
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    assert groups[0].limit_events == [('configure', limits)] and not spawned
    owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert groups[0].limit_events == [('configure', limits), ('verify', limits)]
    assert len(spawned) == 1


def test_failed_limit_setup_retains_empty_group_cleanup_without_workload(peers, monkeypatch):
    boundary, groups, spawned, command = peers

    def unavailable(self, limits):
        raise OSError('controller unavailable')

    monkeypatch.setattr(sys.modules['ahc.infrastructure.process.linux_native'].LinuxProcessGroup,
                        'configure_limits', unavailable)
    owner = runner(boundary, resource_limits=NativeResourceLimits(128 * 1024**2, 16, 50000))
    with pytest.raises(OSError, match='controller unavailable'):
        owner.prepare_creation(command, lifetime_seconds=10)
    assert not spawned and groups[0].complete and owner._attempt is None


def test_disabling_limits_after_reservation_cannot_bypass_launch_check(peers):
    boundary, _, spawned, command = peers
    owner = runner(boundary, resource_limits=NativeResourceLimits(128 * 1024**2, 16, 50000))
    attempt = owner.prepare_creation(command, lifetime_seconds=10)
    owner._resource_limits = None
    with pytest.raises(ContractError, match='resource limits changed'):
        owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert not spawned


def test_limit_readback_failure_after_reservation_never_dispatches(peers, monkeypatch):
    boundary, groups, spawned, command = peers
    owner = runner(boundary, resource_limits=NativeResourceLimits(128 * 1024**2, 16, 50000))
    attempt = owner.prepare_creation(command, lifetime_seconds=10)

    def changed(limits):
        raise ContractError('kernel limits drifted')

    monkeypatch.setattr(groups[0], 'verify_limits', changed)
    with pytest.raises(ContractError, match='kernel limits drifted'):
        owner.create_attempt(attempt, command, lifetime_seconds=10)
    assert owner._attempt == attempt and not spawned
