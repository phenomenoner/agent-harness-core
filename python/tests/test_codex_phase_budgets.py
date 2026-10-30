"""Elapsed budgets and failure scope, with no provider or service calls."""
import json
from dataclasses import replace

import pytest
from test_codex_client import Peer, setup
from test_foreground_config_native import native_config

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.cli.foreground_config_native import parse_foreground_config_native


class Clock:
    value = 0

    def __call__(self):
        return self.value


class TimedPeer(Peer):
    def __init__(self, clock, advances):
        super().__init__()
        self.clock = clock
        self.advances = iter(advances)
        self.read_deadlines = []

    def receive(self, *, deadline):
        self.read_deadlines.append(deadline)
        self.clock.value += next(self.advances, 0)
        if self.clock() >= deadline:
            from ahc.adapters.codex_transport import CodexTransportError
            raise CodexTransportError('deadline', transport_error_type='deadline')
        return super().receive(deadline=deadline)


def bounded(peer, clock, request, **options):
    budgets = {'startup_timeout': 180, 'request_timeout': 60, 'settlement_timeout': 120}
    budgets.update(options)
    client = CodexProtocolClient(peer, cwd='workspace', model='fixture', clock=clock,
                                **budgets)
    return client, replace(request, deadline=3600, configuration_digest=client.configuration_digest)


def test_model_execution_crosses_600_seconds_without_resetting_task_deadline():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [1, 1, 1, 650])
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert result.disposition == 'completed' and result.output_text == 'answer'
    assert clock() > 600
    assert peer.read_deadlines[:3] == [60, 61, 62]
    assert peer.read_deadlines[-1] == request.deadline == 3600
    assert len([m for m in peer.sent if m.get('method') == 'turn/start']) == 1


def test_control_request_timeout_does_not_wait_for_the_whole_long_task():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [61])
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert result.disposition == 'not-submitted'
    assert clock() == 61 and not any(m.get('method') == 'turn/start' for m in peer.sent)
    assert result.backend_diagnostic['code'] == 'control-request-timeout'


def test_advisory_flood_does_not_renew_control_deadline():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [20, 20, 21])
    peer.messages[:0] = [{'method': 'future/information', 'params': {}}] * 3
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert result.disposition == 'not-submitted'
    assert peer.read_deadlines == [60, 60, 60]
    assert result.backend_diagnostic['code'] == 'control-request-timeout'


def test_sequential_startup_requests_share_one_startup_deadline():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [50, 50])
    client, request = bounded(peer, clock, request, startup_timeout=90)
    result = client.run_admitted(request)
    assert result.disposition == 'not-submitted'
    assert peer.read_deadlines == [60, 90]
    assert result.backend_diagnostic['code'] == 'backend-startup-timeout'


def test_activity_cannot_extend_the_absolute_task_budget():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [1, 1, 1, 1800, 1798])
    peer.messages.insert(3, {'method': 'future/information', 'params': {}})
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert result.disposition == 'unknown' and result.output_text == ''
    assert peer.read_deadlines[-2:] == [3600, 3600]


@pytest.mark.parametrize('value', [True, 0, -1, float('nan'), float('inf')])
def test_phase_budgets_reject_unbounded_or_ambiguous_values(value):
    with pytest.raises(Exception, match='request_timeout'):
        CodexProtocolClient(Peer(), cwd='workspace', model='fixture', request_timeout=value)


def test_exact_terminal_receipt_has_a_separate_bounded_full_readback():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [1, 1, 1, 3596, 2])
    full = peer.messages[-1]['params']['turn']
    peer.messages[-1] = {'method': 'turn/completed', 'params': {'threadId': 'thread',
        'turn': {**full, 'items': [], 'itemsView': 'summary'}}}
    peer.messages.append({'id': 5, 'result': {'data': [full]}})
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert clock() > request.deadline
    assert result.disposition == 'completed' and result.output_text == 'answer'
    assert peer.read_deadlines[-1] == 3659
    assert len([m for m in peer.sent if m.get('method') == 'turn/start']) == 1


def test_foreign_terminal_cannot_obtain_post_execution_time():
    _, _, request = setup()
    clock = Clock()
    peer = TimedPeer(clock, [1, 1, 1, 3596])
    peer.messages[-1]['params']['threadId'] = 'foreign'
    peer.messages[-1]['params']['turn']['itemsView'] = 'summary'
    client, request = bounded(peer, clock, request)
    result = client.run_admitted(request)
    assert result.disposition == 'unknown' and result.output_text == ''
    assert not any(m.get('method') == 'thread/turns/list' for m in peer.sent)


def test_native_long_profile_aligns_lifetime_with_settlement_reserve():
    value = native_config()
    value['backend'].update(turn_timeout=3600, lifetime_seconds=4200,
        startup_timeout=180, request_timeout=60, settlement_timeout=120)
    value['workspace']['lifetime_seconds'] = 4200
    parsed = parse_foreground_config_native(value)
    assert parsed.backend['turn_timeout'] == 3600
    assert parsed.backend['settlement_timeout'] == 120
    value['backend']['lifetime_seconds'] = 3600
    with pytest.raises(Exception, match='settlement'):
        parse_foreground_config_native(value)


@pytest.mark.parametrize('retry,foreign', [(True, False), (False, False), (False, True)])
def test_provider_failure_is_distinct_from_retryable_or_foreign_information(retry, foreign):
    peer, client, request = setup()
    peer.messages.insert(3, {'method': 'error', 'params': {
        'threadId': 'foreign' if foreign else 'thread', 'turnId': 'turn',
        'willRetry': retry, 'error': {'message': 'PRIVATE_FAILURE'}}})
    result = client.run_admitted(request)
    if retry or foreign:
        assert result.disposition == 'completed'
    else:
        assert result.disposition == 'unknown' and result.unresolved_effects
        assert result.backend_diagnostic['code'] == 'backend-nonretryable-error'
    assert 'PRIVATE_FAILURE' not in json.dumps(result.public_dict())


def test_optional_read_unavailability_fences_that_resource_and_keeps_work_available(tmp_path):
    from test_workspace_tools import setup as workspace_setup
    from ahc.domain.agent_backend import AgentTurnRequest

    repo, workspace, tools = workspace_setup(tmp_path)
    peer = Peer()
    client = CodexProtocolClient(peer, cwd='workspace', model='fixture', clock=lambda: 0,
                                workspace_tools=tools)
    snapshot = repo.read_current('session')
    request = AgentTurnRequest(snapshot.session, 'optional-read', snapshot.goal.digest, 'do work', (),
        client.executable_sha256, client.configuration_digest, 'work', 100,
        capabilities=tools.capabilities())
    invoked = []
    original = tools.invoke

    def invoke(name, args):
        invoked.append(name)
        if name == 'workspace.environment':
            return {'disposition': 'rejected', 'unresolved_effects': False,
                    'resource_available': False, 'reason': 'environment-guide-unavailable'}
        return original(name, args)

    tools.invoke = invoke
    expected = workspace.identity.to_dict()
    requests = [('environment', {'expected': expected}),
                ('execute', {'expected': expected, 'code': 'x=42'}),
                ('environment', {'expected': {**expected, 'revision': expected['revision'] + 1}})]
    peer.messages[3:3] = [{'id': 7 + i, 'method': 'item/tool/call', 'params': {
        'threadId': 'thread', 'turnId': 'turn', 'namespace': 'workspace', 'tool': name,
        'callId': f'optional-{i}', 'arguments': args}} for i, (name, args) in enumerate(requests)]
    try:
        result = client.run_admitted(request)
        assert result.disposition == 'completed' and not result.final_report
        assert invoked == ['workspace.environment', 'workspace.execute']
        replies = [json.loads(m['result']['contentItems'][0]['text']) for m in peer.sent
                   if m.get('id') in (7, 8, 9)]
        assert replies[0]['degradation']['scope'] == 'resource'
        assert replies[1]['disposition'] == 'observed'
        assert replies[2]['reason'] == 'tool-resource-unavailable'
        assert not client._bridge.reporting and not client._bridge.unresolved
    finally:
        workspace.close()
