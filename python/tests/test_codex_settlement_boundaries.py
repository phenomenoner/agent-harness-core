"""Terminal receipts close effects; short child readback keeps its envelope."""
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_approval import DecisionPort, approval_frame
from test_codex_effective_policy import Peer as PolicyPeer, policy, request as policy_request
from test_codex_phase_budgets import Clock, TimedPeer, bounded
from test_codex_client import Peer, setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_transport import CodexStdioTransport


def summary(peer):
    full = peer.messages[-1]['params']['turn']
    peer.messages[-1]['params']['turn'] = {**full, 'items': [], 'itemsView': 'summary'}
    return full


@pytest.mark.parametrize('mode', ['manual', 'auto-approve', 'always-approve'])
@pytest.mark.parametrize('method', ['item/commandExecution/requestApproval',
                                   'item/fileChange/requestApproval'])
@pytest.mark.parametrize('after_expiry', [False, True])
def test_successful_terminal_never_dispatches_a_new_approval(mode, method, after_expiry):
    clock = Clock()
    effective = policy(approval_mode=mode)
    port = DecisionPort('approve-once')

    class ApprovalsDuringReadback(PolicyPeer):
        def receive(self, *, deadline):
            if self.messages[0].get('method') == 'turn/completed':
                clock.value = 99 if after_expiry else 5
            elif self.messages[0].get('id') == 'approval':
                clock.value += 2
            return super().receive(deadline=deadline)

    peer = ApprovalsDuringReadback(effective)
    if effective.codex_approvals_reviewer is not None:
        peer.messages[1]['result']['approvalsReviewer'] = effective.codex_approvals_reviewer
    full = summary(peer)
    peer.messages.extend([approval_frame(method=method), {'id': 5, 'result': {'data': [{'turnId': full['id'], 'item': full['items'][0]}]}}])
    client = CodexProtocolClient(peer, cwd='workspace', model='fixture', clock=clock,
        effective_policy=effective, approval_port=port,
        startup_timeout=180, request_timeout=60, settlement_timeout=120)
    result = client.run_admitted(policy_request(client, effective))
    assert result.accepted_backend_turns == 1, result.backend_diagnostic
    assert port.requests == []
    assert not any(m.get('id') == 'approval' and m.get('result', {}).get('decision') == 'accept'
                   for m in peer.sent)
    assert result.disposition == 'unknown' and result.unresolved_effects


@pytest.mark.parametrize('after_expiry', [False, True])
def test_readback_idle_keeps_real_pending_channel_steering_unsent(tmp_path, after_expiry):
    from test_channel_controls import make
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    store, current, original, _, controls, _, submit = make(tmp_path)
    active = submit('active work')
    clock = Clock()
    directed = []

    class IdlePeer(PolicyPeer):
        def receive(self, *, deadline, on_idle=None):
            if self.messages[0].get('method') == 'turn/completed':
                clock.value = 29 if after_expiry else 5
            if self.messages[0].get('id') == 5:
                directed.append(submit('/steer preserve this next task'))
                clock.value += 2
                on_idle()
            return super().receive(deadline=deadline)

    peer = IdlePeer(original.effective_policy)
    full = summary(peer)
    peer.messages.append({'id': 5, 'result': {'data': [{'turnId': full['id'], 'item': full['items'][0]}]}})
    client = CodexProtocolClient(peer, cwd='workspace', model='fixture', clock=clock,
        effective_policy=original.effective_policy,
        startup_timeout=180, request_timeout=60, settlement_timeout=120)
    admitted = replace(original, operation_id=active.operation_id, input_text=active.input_text,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest,
        deadline=30)
    store.begin(current, admitted)
    client.steering_port = controls.steering_port(admitted)
    result = client.run_admitted(admitted)
    assert result.disposition == 'completed'
    assert not any(m.get('method') == 'turn/steer' for m in peer.sent)
    with store._read() as db:
        assert db.execute('SELECT state FROM channel_steering WHERE operation_id=?',
                          (directed[0].operation_id,)).fetchone()[0] == 'queued'
    pending = RuntimeInbox(store).query(current.session, directed[0].operation_id)
    assert pending['status'] == 'pending'


@pytest.mark.parametrize('terminal_at,readback_delay,expected', [(1, 1, 'completed'),
                                                              (29, 2, 'unknown')])
def test_child_full_readback_uses_actual_short_transport_and_original_deadline(
        monkeypatch, terminal_at, readback_delay, expected):
    from test_foreground_config_native import native_config
    from ahc.cli.foreground_config_native import parse_foreground_config_native
    from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory

    clock = Clock()
    monkeypatch.setattr('ahc.adapters.codex_transport.time.monotonic', clock)

    class ShortTransport(Peer):
        _max_wait = 30
        _remaining = CodexStdioTransport._remaining

        def send(self, message, *, deadline):
            self._remaining(deadline)
            super().send(message, deadline=deadline)

        def receive(self, *, deadline):
            self._remaining(deadline)
            if self.messages[0].get('method') == 'turn/completed':
                clock.value = terminal_at
            elif self.messages[0].get('id') == 5:
                clock.value += readback_delay
                self._remaining(deadline)
            return super().receive(deadline=deadline)

    raw = native_config()
    raw['backend'].update(startup_timeout=180, request_timeout=60,
        settlement_timeout=120, turn_timeout=3600, lifetime_seconds=4200)
    raw['workspace']['lifetime_seconds'] = 4200
    config = parse_foreground_config_native(raw)
    owner = TrustedCodexBackendFactory(SimpleNamespace(), config)
    peer = ShortTransport()
    peer.messages = PolicyPeer(config.effective_policy).messages
    peer.messages[0]['result'].update(platformFamily='unix', platformOs='linux')
    peer.messages[1]['result'].update(cwd=config.workspace['cwd'], model=config.backend['model'])
    full = summary(peer)
    peer.messages.append({'id': 5, 'result': {'data': [{'turnId': full['id'], 'item': full['items'][0]}]}})
    client = owner._client(peer, None, lambda: False, child=True)
    client.clock = clock
    client.turn_context = client.turn_media = None
    _, _, original = setup()
    admitted = replace(original, artifact_digest=client.executable_sha256,
        configuration_digest=client.configuration_digest, deadline=30,
        effective_policy=config.effective_policy)
    assert owner.binding(None, child=True)[1] == client.configuration_digest
    assert owner.binding(None)[1] != client.configuration_digest
    result = client.run_admitted(admitted)
    assert result.disposition == expected
    assert client._settlement_deadline == 30
    assert len([m for m in peer.sent if m.get('method') == 'turn/start']) == 1
    if expected == 'unknown':
        assert result.unresolved_effects and not result.output_text


def test_no_configured_grace_still_closes_effect_admission_on_success():
    _, _, original = setup()
    clock = Clock()
    peer = TimedPeer(clock, [0, 0, 0, 0, 0])
    full = summary(peer)
    peer.messages.extend([approval_frame(), {'id': 5, 'result': {'data': [{'turnId': full['id'], 'item': full['items'][0]}]}}])
    client, admitted = bounded(peer, clock, original, settlement_timeout=None)
    result = client.run_admitted(admitted)
    assert result.disposition == 'unknown'
    assert client._native_terminal_received and not client.effects_open
    assert not any(m.get('id') == 'approval' and 'result' in m for m in peer.sent)


@pytest.mark.parametrize('summary_view', [False, True])
@pytest.mark.parametrize('buffered_started', [False, True])
def test_completion_buffered_before_start_ack_closes_steering_before_poll(
        tmp_path, summary_view, buffered_started):
    from test_channel_controls import make
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    store, current, original, _, controls, _, submit = make(tmp_path)
    active = submit('active work')
    directed = []

    class EarlyTerminalPeer(PolicyPeer):
        def receive(self, *, deadline, on_idle=None):
            if self.messages[0].get('id') == 3:
                directed.append(submit('/steer after the already completed task'))
            return super().receive(deadline=deadline)

    peer = EarlyTerminalPeer(original.effective_policy)
    full = peer.messages[-1]['params']['turn']
    if summary_view:
        summary(peer)
    acknowledgement = peer.messages.pop(2)
    if buffered_started:
        peer.messages.insert(2, {'method': 'turn/started',
            'params': {'threadId': 'thread', 'turn': {'id': 'turn', 'status': 'inProgress'}}})
    peer.messages.append(acknowledgement)
    if summary_view:
        peer.messages.append({'id': 5, 'result': {'data': [{'turnId': full['id'], 'item': full['items'][0]}]}})
    client = CodexProtocolClient(peer, cwd='workspace', model='fixture', clock=lambda: 0,
        effective_policy=original.effective_policy,
        startup_timeout=180, request_timeout=60, settlement_timeout=120)
    admitted = replace(original, operation_id=active.operation_id, input_text=active.input_text,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest,
        deadline=100)
    store.begin(current, admitted)
    client.steering_port = controls.steering_port(admitted)
    result = client.run_admitted(admitted)
    assert result.disposition == 'completed'
    assert not any(m.get('method') == 'turn/steer' for m in peer.sent)
    assert RuntimeInbox(store).query(current.session, directed[0].operation_id)['status'] == 'pending'
    with store._read() as db:
        assert db.execute('SELECT state FROM channel_steering WHERE operation_id=?',
                          (directed[0].operation_id,)).fetchone()[0] == 'queued'
