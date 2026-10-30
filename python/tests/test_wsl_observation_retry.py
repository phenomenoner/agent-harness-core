from dataclasses import asdict

import pytest

from ahc.adapters import wsl_unit_runner as module
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable, LinuxUnitIdentity

IDENTITY = LinuxUnitIdentity('AHC-Test', '12345678-1234-1234-1234-123456789abc',
                           'ahc-backend-' + 'a' * 32 + '.service', 'b' * 32)


def install(monkeypatch, replies):
    calls = []

    class Transport:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send(self, request, **kwargs):
            calls.append(dict(request))

        def receive(self, **kwargs):
            value = replies.pop(0)
            if isinstance(value, Exception):
                raise value
            return value

    runner = module.WslUnitRunner('C:/Windows/System32/wsl.exe', 'AHC-Test')
    monkeypatch.setattr(runner, '_open_transport', lambda source: Transport())
    return runner, calls


def reply(**changes):
    return {'ok': True, 'result': {**asdict(IDENTITY), 'state': 'running',
                                  'cgroup_empty': False, **changes}}


def test_read_only_observation_reacquires_one_lost_reply(monkeypatch):
    runner, calls = install(monkeypatch, [module.CodexTransportError('lost'), reply()])
    result = runner.observe(IDENTITY)
    assert result.invocation_id == IDENTITY.invocation_id
    assert calls == [{'action': 'observe', 'unit': IDENTITY.unit}] * 2
    assert runner.observation_transport_failures == 1


def test_two_transport_failures_exhaust_budget(monkeypatch):
    runner, calls = install(monkeypatch, [module.CodexTransportError('lost')] * 2)
    with pytest.raises(module.WslControlTransportUnavailable):
        runner.observe(IDENTITY)
    assert len(calls) == 2
    assert runner.observation_transport_failures == 2


@pytest.mark.parametrize('first_lost', [False, True])
def test_invalid_identity_response_is_never_retried_or_accepted(monkeypatch, first_lost):
    replies = ([module.CodexTransportError('lost')] if first_lost else [])
    runner, calls = install(monkeypatch, replies + [reply(distribution='Other')])
    with pytest.raises(BackendCustodyUnavailable):
        runner.observe(IDENTITY)
    assert len(calls) == 1 + int(first_lost)


@pytest.mark.parametrize('action', ['create', 'stop', 'confirm_retirement'])
def test_effects_and_retirement_do_not_inherit_observe_retry(monkeypatch, action):
    runner, calls = install(monkeypatch, [module.CodexTransportError('lost')])
    with pytest.raises(BackendCustodyUnavailable):
        if action == 'create':
            runner.create(['/usr/bin/cat'], lifetime_seconds=60)
        else:
            getattr(runner, action)(IDENTITY)
    assert len(calls) == 1
    assert runner.observation_transport_failures == 0
