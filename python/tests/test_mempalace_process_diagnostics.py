"""Transport failure classification without exposing worker inputs or stderr."""

import json
import subprocess
import traceback

import pytest

import ahc.adapters.mempalace_memory_process as process_module
from ahc.adapters.memory_native_process import NativeMemoryUnavailable
from ahc.cli.mempalace_memory_binding import MemPalaceMemoryBinding


@pytest.mark.parametrize(
    ('failure', 'message'),
    [
        ('timeout', 'MemPalace operation timed out'),
        ('nonzero', 'MemPalace operation unavailable'),
        ('invalid-json', 'MemPalace operation unavailable'),
        ('success', None),
    ],
)
def test_status_distinguishes_timeout_without_leaking_worker_values(
    tmp_path, monkeypatch, failure, message,
):
    binding = MemPalaceMemoryBinding(
        agent_id='diagnostic', resource_id='diagnostic-memory',
        root=str(tmp_path / 'journal'), scope='agent-harness-core/main',
        palace_id='diagnostic-only', provider_digest='0' * 64,
        model_identity='unit-transport-only',
        model_root=str(tmp_path / 'model-not-opened'),
        qdrant_url='http://127.0.0.1:1',
        credential_file=str(tmp_path / 'credential-not-opened'), timeout=120.0,
        store_policy='journal-native', observe_granted=True, store_granted=False,
    )
    calls = []
    secret = 'TEST-ONLY-SENSITIVE-DIAGNOSTIC-SENTINEL'

    def fail_worker(command, **options):
        calls.append((command, options))
        assert options['timeout'] == 120.0
        assert json.loads(options['input'])['operation'] == 'status'
        if failure == 'timeout':
            raise subprocess.TimeoutExpired([secret], 120, output=secret, stderr=secret)
        return subprocess.CompletedProcess(
            command, 17 if failure == 'nonzero' else 0,
            stdout=(secret.encode() if failure == 'invalid-json'
                    else json.dumps({'status': 'ready'}).encode()),
            stderr=secret.encode(),
        )

    monkeypatch.setattr(process_module.subprocess, 'run', fail_worker)
    port = process_module.MemPalaceMemoryProcess(binding=binding, scratch_root=tmp_path)
    if failure == 'success':
        assert port.status() == {'status': 'ready'}
    else:
        with pytest.raises(NativeMemoryUnavailable) as raised:
            port.status()
        assert str(raised.value) == message
        assert secret not in ''.join(traceback.format_exception(raised.value))
        assert raised.value.__suppress_context__ is True
    assert len(calls) == 1
    assert not (tmp_path / 'journal').exists()
    assert not (tmp_path / 'model-not-opened').exists()
    assert not (tmp_path / 'credential-not-opened').exists()
    assert not list(tmp_path.glob('palace-memory-*'))
