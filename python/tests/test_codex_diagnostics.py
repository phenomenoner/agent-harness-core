"""Distinct failures survive projection without storing private error payloads."""
import json
from dataclasses import replace

import pytest
from test_codex_client import setup

from ahc.adapters.codex_transport import CodexTransportError
from ahc.domain.runtime_turn import accept_observation
from ahc.infrastructure.working_repository import WorkingWriterBusy


@pytest.mark.parametrize('kind,category,code', [
    ('unsupported', 'protocol', 'unsupported-event'),
    ('transport', 'transport', 'eof'),
    ('busy', 'repository-busy', 'none'),
    ('interrupted', 'backend-interrupted', 'none'),
])
def test_failure_categories_survive_durable_projection(kind, category, code):
    peer, client, request = setup()
    if kind == 'unsupported':
        # An actionable method missing its RPC id is still a protocol failure.
        peer.messages.insert(3, {'method': 'item/tool/call', 'params': {
            'threadId': 'thread', 'payload': 'PRIVATE/SECRET'}})
    elif kind == 'interrupted':
        peer.messages[-1]['params']['turn']['status'] = 'interrupted'
    else:
        original = peer.receive
        def receive(*, deadline):
            if len(peer.messages) == 1:
                if kind == 'transport':
                    raise CodexTransportError('PRIVATE/SECRET', transport_error_type='eof')
                raise WorkingWriterBusy('PRIVATE/SECRET')
            return original(deadline=deadline)
        peer.receive = receive
    result = accept_observation(request, client.run_admitted(request))
    assert result['accepted'] is False and result['unresolved_effects'] is True
    diagnostic = result['backend_diagnostic']
    assert diagnostic['category'] == category and diagnostic['code'] == code
    assert diagnostic['cleanup'] == 'client-close-returned'
    assert 'PRIVATE' not in json.dumps(result)
    assert result['backend_completion'] == {'thread': 'thread', 'turn': 'turn'}
    assert peer.closed


def test_original_failure_survives_cleanup_failure():
    peer, client, request = setup()
    peer.messages[-1]['params']['turn']['status'] = 'interrupted'
    peer.fail_close = True
    result = accept_observation(request, client.run_admitted(request))
    assert result['backend_diagnostic']['category'] == 'backend-interrupted'
    assert result['backend_diagnostic']['cleanup'] == 'unverified'
    assert result['accepted'] is False


@pytest.mark.parametrize('source', ['cancelled', 'deadline'])
def test_stop_sources_are_distinct(source):
    peer, client, request = setup()
    if source == 'cancelled':
        client.cancelled = lambda: True
    else:
        client.clock = lambda: request.deadline
    result = accept_observation(request, client.run_admitted(request))
    assert result['backend_diagnostic']['category'] == source
    assert result['admissions'] == 0 and result['accepted'] is False


def test_success_has_no_failure_diagnostic_and_cleanup_failure_keeps_answer():
    peer, client, request = setup()
    result = accept_observation(request, client.run_admitted(request))
    assert result['accepted'] and 'backend_diagnostic' not in result
    peer, client, request = setup()
    peer.fail_close = True
    result = accept_observation(request, client.run_admitted(request))
    assert result['accepted'] and result['output_text'] == 'answer'
    assert result['cleanup_pending']
    assert result['backend_diagnostic']['category'] == 'cleanup'


def test_malformed_diagnostics_cannot_leak_or_change_acceptance():
    peer, client, request = setup()
    observed = client.run_admitted(request)
    observed = replace(observed, backend_diagnostic={'category': 'PRIVATE/SECRET'})
    assert 'backend_diagnostic' not in observed.public_dict()
    projected = accept_observation(request, observed)
    assert projected['accepted'] and 'PRIVATE' not in json.dumps(projected)


def test_optional_workspace_fault_is_sanitized_without_losing_the_primary_diagnostic():
    from ahc.domain.backend_diagnostics import diagnostic_projection

    base = {'category':'workspace', 'phase':'tool', 'last_method':'item/tool/call',
            'code':'workspace-effects-unresolved', 'remaining_ms':123, 'frames':5,
            'cleanup':'client-close-returned'}
    projected = diagnostic_projection({**base, 'workspace_failure':{
        'phase':'result-save', 'code':'writer-busy', 'cleanup':'unverified', 'payload':'PRIVATE'}})
    assert projected['backend_diagnostic']['workspace_failure'] == {
        'phase':'result-save', 'code':'writer-busy', 'cleanup':'unverified'}
    assert 'PRIVATE' not in json.dumps(projected)
    assert diagnostic_projection({**base, 'workspace_failure':{'phase':'PRIVATE'}}) == {
        'backend_diagnostic':base}


def test_terminal_fault_survives_steering_reconciliation_failure():
    peer, client, request = setup()
    class Steering:
        def poll(self, client):
            pass
        def consume_response(self, message):
            return False
        def reconcile_terminal(self):
            raise WorkingWriterBusy('PRIVATE')
    client.steering_port = Steering()
    original = peer.receive
    peer.receive = lambda *, deadline, on_idle: original(deadline=deadline)
    peer.messages.insert(3, {'method': 'item/tool/call', 'params': {
        'threadId': 'thread', 'payload': 'PRIVATE'}})
    result = accept_observation(request, client.run_admitted(request))
    assert result['backend_diagnostic']['category'] == 'protocol'
    assert result['backend_diagnostic']['code'] == 'unsupported-event'


def test_diagnostic_survives_real_runtime_commit_and_reopen(tmp_path):
    from types import SimpleNamespace

    from test_channel_outbox import state

    from ahc.application.runtime_service import RuntimeService
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    repo, snapshot, request, route = state(tmp_path)
    peer, client, _ = setup()
    request = replace(request, artifact_digest=client.executable_sha256,
                      configuration_digest=client.configuration_digest)
    peer.messages[-1]['params']['turn']['status'] = 'interrupted'
    result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
        snapshot, request)
    reopened = RuntimeRepository(repo.root).read_turn(route.session, request.operation_id)
    assert reopened == result
    assert reopened.result['backend_diagnostic']['category'] == 'backend-interrupted'
    assert reopened.result['accepted'] is False
