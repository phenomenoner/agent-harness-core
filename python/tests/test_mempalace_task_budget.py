"""Cold reads share admitted time; writes and out-of-turn calls keep their bounds."""

import json
import subprocess

import pytest
from test_codex_client import setup

from ahc.adapters.memory_native_process import NativeMemoryUnavailable
from ahc.adapters.memory_read_budget import memory_read_budget
from ahc.adapters.mempalace_memory_process import MemPalaceMemoryProcess
from ahc.cli.mempalace_memory_binding import MemPalaceMemoryBinding


def port(tmp_path):
    binding = MemPalaceMemoryBinding(agent_id="agent", resource_id="memory", scope="scope",
        root=str(tmp_path/'journal'), model_root=str(tmp_path/'model'),
        credential_file=str(tmp_path/'not-opened'), palace_id="fixture",
        qdrant_url="http://127.0.0.1:1", provider_digest="a"*64, model_identity="fixture",
        store_policy="trusted", observe_granted=True, store_granted=True, timeout=30)
    return MemPalaceMemoryProcess(binding=binding, scratch_root=tmp_path)


def test_actual_client_supplies_parent_budget_to_cold_recall_then_clears_it(tmp_path, monkeypatch):
    peer, client, request = setup()
    clock, launches = [0], []
    client.clock = lambda: clock[0]
    adapter = port(tmp_path)
    def configured(command, **options):
        assert options['timeout'] == 30
        raise subprocess.TimeoutExpired(command, 30)
    class Worker:
        returncode = 0
        stdin = stdout = stderr = None
        def __init__(self, command, **options):
            launches.append(command)
        def communicate(self, *, input=None, timeout=None):
            assert timeout <= .25
            clock[0] = 35.645  # Still inside the original100-second task.
            return b'{"status":"ready"}', b''
    monkeypatch.setattr(subprocess, 'run', configured)
    monkeypatch.setattr(subprocess, 'Popen', Worker)
    client.turn_context = lambda request: json.dumps(adapter.status())
    result = client.run_admitted(request)
    assert result.disposition == 'completed' and len(launches) == 1
    with pytest.raises(NativeMemoryUnavailable, match='timed out'):
        adapter.status()  # No inherited context can leak into the next caller.


@pytest.mark.parametrize('operation', ['store','revise','sync','reconcile','backup'])
def test_mutating_workers_keep_configured_timeout_inside_parent_budget(tmp_path, monkeypatch, operation):
    observed=[]
    def run(command, **options):
        observed.append(options['timeout'])
        assert json.loads(options['input'])['operation'] == operation
        return subprocess.CompletedProcess(command,0,b'{"status":"ready"}',b'')
    monkeypatch.setattr(subprocess,'run',run)
    with memory_read_budget(100,clock=lambda:0):
        assert port(tmp_path)._call(operation,{}) == {'status':'ready'}
    assert observed == [30]


def test_expired_read_cannot_launch_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess,'Popen',lambda *args,**kwargs:pytest.fail('expired worker started'))
    with memory_read_budget(1,clock=lambda:2), pytest.raises(NativeMemoryUnavailable, match='timed out'):
        port(tmp_path).status()
