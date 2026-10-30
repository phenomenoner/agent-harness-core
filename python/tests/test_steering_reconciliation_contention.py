import sqlite3
from dataclasses import replace
from types import MethodType, SimpleNamespace

import pytest
from test_channel_controls import make


def active_port(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit('active work')
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    directed = submit('/steer guide this turn')
    sent = []
    client = SimpleNamespace(thread='thread-1', turn='turn-1', _send=sent.append)
    port = controls.steering_port(request)
    port.poll(client)
    return store, controls, port, client, sent, directed, active


def hold_reader(store):
    reader = sqlite3.connect(store.db_path, isolation_level=None)
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM channel_steering').fetchall()
    return reader


def state(store, event):
    with store._read() as c:
        return c.execute('SELECT state FROM channel_steering WHERE operation_id=?',
                         (event.operation_id,)).fetchone()[0]


@pytest.mark.parametrize('response,expected', [
    ({'result': {'turnId': 'turn-1'}}, 'sent'),
    ({'error': {'code': -32000, 'message': 'rejected'}}, 'rejected'),
    ({'result': {'turnId': 'foreign'}}, 'unknown'),
])
def test_ack_commit_contention_keeps_decoder_alive_and_never_resends(tmp_path, response, expected):
    store, _, port, client, sent, directed, _ = active_port(tmp_path)
    reader = hold_reader(store)
    try:
        assert port.consume_response({'id': sent[0]['id'], **response})
        port.poll(client)
        assert len(sent) == 1
        assert state(store, directed) == 'sending'
    finally:
        reader.close()
    port.poll(client)
    assert state(store, directed) == expected
    assert len(sent) == 1


def test_terminal_busy_preserves_sending_for_later_orphan_reconciliation(tmp_path):
    store, controls, port, _, sent, directed, active = active_port(tmp_path)
    reader = hold_reader(store)
    try:
        port.reconcile_terminal()  # Must not replace a completed model result.
        assert state(store, directed) == 'sending'
    finally:
        reader.close()
    controls.reconcile_steering_terminal(active.operation_id)
    assert state(store, directed) == 'unknown'
    assert len(sent) == 1


def test_ack_storage_corruption_is_not_silenced(tmp_path, monkeypatch):
    store, _, port, _, sent, _, _ = active_port(tmp_path)
    from ahc.infrastructure.working_repository import WorkingSetCorrupt
    def corrupt(*args, **kwargs):
        raise WorkingSetCorrupt('invalid durable state')
    monkeypatch.setattr(store, '_mutate', corrupt)
    with pytest.raises(WorkingSetCorrupt):
        port.consume_response({'id': sent[0]['id'], 'result': {'turnId': 'turn-1'}})


def test_real_decoder_final_survives_reconciliation_commit_busy(tmp_path):
    from ahc.adapters.codex_terminal_readback import receive_terminal
    store, _, port, client, sent, directed, _ = active_port(tmp_path)
    client.steering_port = port
    client._stopped = lambda: False
    client._result = lambda disposition, reason, text: (disposition, reason, text)
    from ahc.adapters.codex_client import CodexProtocolClient
    client.begin_settlement = MethodType(CodexProtocolClient.begin_settlement, client)
    client._native_terminal_received = False
    client.settlement_timeout = None
    client.request, client.clock = port.request, lambda: 0
    from ahc.adapters.codex_native_collaboration import NativeCollaboration
    client._native_collaboration = NativeCollaboration(client)
    event = {'method': 'turn/completed', 'params': {
        'threadId': 'thread-1', 'turn': {'id': 'turn-1', 'status': 'completed', 'error': None,
        'items': [{'type': 'agentMessage', 'id': 'final', 'phase': 'final_answer',
                   'text': 'completed task'}]}}}
    reader = hold_reader(store)
    try:
        assert receive_terminal(client, [event]) == (
            'completed', 'backend-observed', 'completed task')
        assert state(store, directed) == 'sending' and len(sent) == 1
    finally:
        reader.close()


def test_unsaved_ack_after_decoder_loss_remains_unknown_without_replay(tmp_path):
    store, controls, port, client, sent, directed, active = active_port(tmp_path)
    reader = hold_reader(store)
    try:
        assert port.consume_response({'id': sent[0]['id'], 'result': {'turnId': 'turn-1'}})
        port.reconcile_terminal()
    finally:
        reader.close()
    replacement = controls.steering_port(port.request)
    controls.reconcile_steering_terminal(active.operation_id)
    replacement.poll(client)
    assert state(store, directed) == 'unknown' and len(sent) == 1


def test_empty_terminal_probe_is_read_only_and_exclusive_busy_defers(tmp_path, monkeypatch):
    store, _, request, _, controls, _, _ = make(tmp_path)
    port = controls.steering_port(request)
    monkeypatch.setattr(store, '_mutate', lambda *args: pytest.fail('empty write transaction'))
    port.reconcile_terminal()
    writer = sqlite3.connect(store.db_path, isolation_level=None)
    writer.execute('BEGIN EXCLUSIVE')
    try:
        client = SimpleNamespace(thread='thread', turn='turn', _send=lambda _: pytest.fail('send'))
        port.poll(client)
        port.reconcile_terminal()
        from ahc.infrastructure.working_repository import WorkingWriterBusy
        with pytest.raises(WorkingWriterBusy):
            controls.restart_ready()
    finally:
        writer.close()
