"""Portable descriptor peers; these checks do not qualify kernel controller setup."""

from types import SimpleNamespace

import pytest

from ahc.domain.sessions import ContractError
from ahc.infrastructure.process import linux_service_resources as adapter


@pytest.fixture
def peer(monkeypatch):
    files = {'cgroup.procs': '', 'cgroup.controllers': 'cpu memory pids io',
             'cgroup.subtree_control': 'io'}
    opened, writes = {}, []

    def open_file(name, flags, *, dir_fd):
        assert dir_fd == 77 and name in files
        descriptor = 100 + len(opened)
        opened[descriptor] = name
        return descriptor

    def write(descriptor, raw):
        assert opened[descriptor] == 'cgroup.subtree_control'
        writes.append(raw)
        existing = set(files['cgroup.subtree_control'].split())
        existing.update(value.removeprefix('+') for value in raw.decode('ascii').split())
        files['cgroup.subtree_control'] = ' '.join(sorted(existing))
        return len(raw)

    system = SimpleNamespace(O_RDONLY=0, O_WRONLY=1, O_NOFOLLOW=2, O_CLOEXEC=4,
                             open=open_file, write=write, close=opened.pop,
                             read=lambda fd, size: files[opened[fd]].encode('ascii')[:size])
    monkeypatch.setattr(adapter, 'os', system)
    return files, opened, writes, system


def test_adds_only_missing_service_controllers_and_preserves_existing_ones(peer):
    files, opened, writes, _ = peer
    adapter.enable_service_controllers(77)
    assert writes == [b'+cpu +memory +pids\n'] and not opened
    assert set(files['cgroup.subtree_control'].split()) == {'cpu', 'memory', 'pids', 'io'}
    adapter.enable_service_controllers(77)
    assert len(writes) == 1 and not opened


@pytest.mark.parametrize('file,value', [
    ('cgroup.procs', '1000\n'), ('cgroup.controllers', 'cpu pids'),
    ('cgroup.procs', '1' * 4096),
])
def test_ineligible_service_refuses_without_a_write(peer, file, value):
    files, opened, writes, _ = peer
    files[file] = value
    with pytest.raises(ContractError):
        adapter.enable_service_controllers(77)
    assert not writes and not opened


@pytest.mark.parametrize('failure', ['short', 'lost', 'denied'])
def test_uncertain_or_failed_enablement_is_never_retried_or_undone(peer, failure):
    _, opened, writes, system = peer

    def faulty(descriptor, raw):
        writes.append(raw)
        if failure == 'denied':
            raise PermissionError()
        return len(raw) - (failure == 'short')

    system.write = faulty
    with pytest.raises((ContractError, PermissionError)):
        adapter.enable_service_controllers(77)
    assert writes == [b'+cpu +memory +pids\n'] and not opened
