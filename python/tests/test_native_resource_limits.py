"""Portable limit contracts and descriptor-operation peers, not kernel enforcement."""

import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest

from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process import linux_resource_limits as adapter


def limits(**kwargs):
    return NativeResourceLimits(128 * 1024**2, 16, 50000, **kwargs)


@pytest.mark.parametrize('changes', [
    {'memory_bytes': 'max'}, {'memory_bytes': 0}, {'memory_bytes': 16777217},
    {'pids': True}, {'pids': 0}, {'cpu_quota_us': 0}, {'cpu_period_us': 1000001},
    {'swap_bytes': -1}, {'swap_bytes': 256 * 1024**2},
])
def test_finite_limits_reject_unlimited_invalid_or_rounded_values(changes):
    with pytest.raises(ContractError):
        replace(limits(), **changes)


@pytest.fixture
def peer(monkeypatch):
    files = {name: 'max' for name in ('memory.max', 'memory.swap.max', 'pids.max', 'cpu.max')}
    events, opened = [], {}
    group = SimpleNamespace(owner_pid=123, claimed=False, complete=False, kill_attempted=False,
                            directory_fd=77, lock=threading.Lock(), _populated=lambda: False)

    def open_file(name, flags, *, dir_fd):
        assert dir_fd == 77 and '/' not in name
        if name not in files:
            raise FileNotFoundError(name)
        descriptor = 100 + len(events)
        opened[descriptor] = (name, flags)
        events.append(('open', name, flags, dir_fd))
        return descriptor

    def write(descriptor, raw):
        name, flags = opened[descriptor]
        assert flags & 1
        files[name] = raw.decode('ascii')
        events.append(('write', name, raw))
        return len(raw)

    def read(descriptor, size):
        name, flags = opened[descriptor]
        assert not flags & 1 and size == 256
        return files[name].encode('ascii')

    def close(descriptor):
        opened.pop(descriptor)

    system = SimpleNamespace(getpid=lambda: 123, O_RDONLY=0, O_WRONLY=1,
                             O_NOFOLLOW=2, O_CLOEXEC=4,
                             open=open_file, write=write, read=read, close=close)
    monkeypatch.setattr(adapter, 'os', system)
    return group, files, events, opened, system


def test_limits_write_only_owned_child_controls_then_read_back(peer):
    group, files, events, opened, _ = peer
    adapter.configure_limits(group, limits())
    assert files == {'memory.max': '134217728\n', 'memory.swap.max': '0\n',
                     'pids.max': '16\n', 'cpu.max': '50000 100000\n'}
    assert len([event for event in events if event[0] == 'write']) == 4
    assert not opened
    adapter.verify_limits(group, limits())
    assert len([event for event in events if event[0] == 'write']) == 4


@pytest.mark.parametrize('change', ['foreign', 'claimed', 'populated', 'complete', 'kill'])
def test_nonempty_or_unowned_group_is_rejected_before_any_file_operation(peer, change):
    group, _, events, _, _ = peer
    if change == 'foreign':
        group.owner_pid = 999
    elif change == 'populated':
        group._populated = lambda: True
    else:
        setattr(group, {'claimed': 'claimed', 'complete': 'complete', 'kill': 'kill_attempted'}[
            change], True)
    with pytest.raises(ContractError, match='owned empty'):
        adapter.configure_limits(group, limits())
    assert events == []


def test_missing_controller_stops_without_parent_enablement_or_unlimited_fallback(peer):
    group, files, events, opened, _ = peer
    del files['pids.max']
    with pytest.raises(FileNotFoundError):
        adapter.configure_limits(group, limits())
    assert [event[1] for event in events if event[0] == 'write'] == [
        'memory.max', 'memory.swap.max']
    assert not opened


def test_partial_write_closes_descriptor_and_does_not_retry(peer):
    group, _, events, opened, system = peer
    system.write = lambda descriptor, raw: len(raw) - 1
    with pytest.raises(ContractError, match='incomplete'):
        adapter.configure_limits(group, limits())
    assert len(events) == 1 and not opened


def test_readback_drift_is_not_accepted_or_silently_repaired(peer):
    group, files, events, opened, _ = peer
    adapter.configure_limits(group, limits())
    writes = len(events)
    files['cpu.max'] = 'max 100000'
    with pytest.raises(ContractError, match='readback differs'):
        adapter.verify_limits(group, limits())
    assert all(event[0] == 'open' for event in events[writes:])
    assert not opened
