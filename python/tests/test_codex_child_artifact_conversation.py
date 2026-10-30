"""Real child protocol/client composition with a controlled wire and unit runner."""
import json
import time
from types import SimpleNamespace

import pytest
from test_backend_custody import Runner
from test_codex_client import Peer
from test_foreground_config_v2 import valid_config
from test_workspace_tools import setup

from ahc.application.child_dispatch import ChildDispatchService
from ahc.cli import trusted_codex_child
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.cli.trusted_codex_child import TrustedCodexChildFactory
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.infrastructure.child_artifacts import ChildArtifacts
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository


@pytest.mark.parametrize('lost_terminal', [False, True])
def test_child_artifact_full_protocol_and_lost_terminal(tmp_path, monkeypatch, lost_terminal):
    store, workspace, _ = setup(tmp_path)
    current = store.read_current('session')
    raw = valid_config()
    raw['backend'].update(enabled=True, model='gpt-5.6-luna', reasoning_effort='max')
    raw['child_limits'] = dict(max_children=2, max_concurrency=1,
                               max_artifacts=1, artifact_bytes=100)
    owner = TrustedCodexBackendFactory(store, parse_foreground_config_v2(raw))
    tools = SimpleNamespace(workspace=workspace, capabilities=lambda: ('workspace.artifact',))
    artifact, binding = owner.binding(tools)
    parent = AgentTurnRequest(current.session, 'parent', current.goal.digest, 'work', (),
        artifact, binding, 'work', time.monotonic() + 60,
        capabilities=('workspace.artifact',), effective_policy=owner.config.effective_policy)
    children = ChildDispatchRepository(store, parent)
    envelope = ChildWorkEnvelope('child', children.parent, 'report', 'work',
                                 time.time_ns() // 1_000_000 + 30_000)
    factory = TrustedCodexChildFactory(owner, children, envelope, 'claim', workspace.identity)

    def call(name, index, **arguments):
        return {'id': index, 'method': 'item/tool/call', 'params': {
            'threadId': 'thread', 'turnId': 'turn', 'namespace': 'workspace',
            'tool': name, 'callId': str(index), 'arguments': {
                'expected': workspace.identity.to_dict(), **arguments}}}

    class Wire(Peer):
        def receive(self, *, deadline):
            if not self.messages:
                raise EOFError('controlled terminal receipt loss')
            return super().receive(deadline=deadline)

        def send(self, message, *, deadline):
            super().send(message, deadline=deadline)
            if message.get('id') == 7:
                assert message['result']['success']
                result = json.loads(message['result']['contentItems'][0]['text'])['result']
                self.messages.insert(0, call('attach', 8, artifact_id=result['artifact_id'],
                                             filename='report.txt'))
            if message.get('id') == 8:
                assert message['result']['success']
                if lost_terminal:
                    self.messages.clear()

    peer = Wire()
    peer.messages[0]['result'].update(platformFamily='unix', platformOs='linux')
    peer.messages[1]['result'].update(cwd=raw['workspace']['cwd'], model='gpt-5.6-luna',
        approvalPolicy='on-request', sandbox={
            'type': 'workspaceWrite', 'writableRoots': [], 'networkAccess': False,
            'excludeTmpdirEnvVar': False, 'excludeSlashTmp': False})
    peer.messages.insert(3, call('publish_text', 7, text='report', media_type='text/plain'))
    unit = LinuxUnitIdentity('AHC-Codex-Ubuntu-24.04',
        '12345678-1234-1234-1234-123456789abc', 'ahc-backend-' + 'c' * 32 + '.service', 'b' * 32)
    processes = []

    class Process(Runner):
        def __init__(self, *args, **kwargs):
            super().__init__(unit)
            processes.append(self)

        def create(self, command, *, lifetime_seconds):
            assert command == owner._command
            return unit

        def attach(self, selected, custody, lease):
            assert selected == unit and custody.envelope == envelope
            return peer

        def close(self):
            pass

    monkeypatch.setattr(owner, 'new_runner', lambda **kwargs: Process())
    try:
        with store.execution_lock():
            store.begin(current, parent)
            children.admit(envelope)
            ChildArtifacts(children).allocate(envelope.parent, 'child', max_count=1, max_bytes=100)
            service = ChildDispatchService(children, lambda item: factory.port())
            with store.workspace_turn_scope(parent):
                result = service.dispatch(envelope, 'claim')
                replay = service.dispatch(envelope, 'other')
            assert result['state'] == ('unknown' if lost_terminal else 'terminal')
            assert replay['state'] == result['state']
            if not lost_terminal:
                assert result['result']['artifacts'][0]['filename'] == 'report.txt'
            assert len(processes) == 1 and processes[0].stopped and peer.closed
            start = next(m for m in peer.sent if m.get('method') == 'thread/start')
            assert [t['name'] for t in start['params']['dynamicTools'][0]['tools']] == [
                'publish_text', 'attach']
            turn = next(m for m in peer.sent if m.get('method') == 'turn/start')
            assert turn['params']['effort'] == 'max'
            with store._read() as connection:
                count = connection.execute('SELECT count(*) FROM runtime_child_artifacts')
                assert count.fetchone()[0] == 1
                state = connection.execute("SELECT state FROM backend_custody").fetchone()[0]
                assert state == 'cleaned'
    finally:
        workspace.close()
