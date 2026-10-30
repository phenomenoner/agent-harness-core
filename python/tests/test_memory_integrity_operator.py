"""Exact operator CLI admission/IPC; maintenance is not a model workspace tool."""

import json
from dataclasses import replace

import pytest
from profile_test_support import profile_registry_payload
from test_foreground_config_native import native_config
from test_memory_journal import Embedder, Projection
from test_memory_status_integrity import integrity_result
from test_mempalace_binding import document, parse

from ahc.adapters.local_embedding import MODEL_IDENTITY
from ahc.adapters.memory_native_process import NativeMemoryUnavailable
from ahc.adapters.mempalace_identity import mempalace_identity
from ahc.adapters.mempalace_memory_process import MemPalaceMemoryProcess
from ahc.cli.main import build_parser, main
from ahc.cli.runtime_memory_backup import run
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_journal import MemoryJournal


def cli_fixture(tmp_path, *, observe=True):
    class WorkerEmbedder(Embedder):
        identity = MODEL_IDENTITY

    journal = MemoryJournal(tmp_path / 'journal', embedder=WorkerEmbedder(),
                            projection=Projection(), create=True)
    raw = document()
    raw['bindings']['hexi.durable-memory'].update(
        root=str(journal.root), provider_digest=mempalace_identity(),
        model_root=str(tmp_path / 'unused-model'), credential_file=str(tmp_path / 'unused-key'),
        observe_granted=observe, store_granted=False, store_policy='deny',
        qdrant_url='http://127.0.0.1:1')
    binding = parse(raw)
    binding_file = tmp_path / 'binding.json'
    binding_file.write_text(json.dumps(raw))
    runtime = tmp_path / 'runtime'
    runtime.mkdir()
    config = native_config()
    config['session']['agent_id'] = 'hexi'
    config['runtime_root'] = str(runtime)
    config['native_memory'] = {'binding_file': str(binding_file), 'binding_digest': binding.digest}
    foreground_file, profile_file = tmp_path / 'foreground.json', tmp_path / 'profiles.json'
    foreground_file.write_text(json.dumps(config))
    profile_file.write_text(json.dumps(profile_registry_payload()))
    argv = ['runtime-memory-integrity-check', '--foreground-config', str(foreground_file),
            '--profile-registry', str(profile_file)]
    return journal, binding, runtime, config, foreground_file, argv


def test_actual_cli_worker_whole_check_without_initializing_runtime_or_loading_model(
    tmp_path, capsys,
):
    journal, binding, runtime, _, _, argv = cli_fixture(tmp_path)
    before = journal.database.read_bytes()
    assert main(argv) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['command'] == 'runtime-memory-integrity-check'
    assert payload['integrity'] == integrity_result('ok')
    assert payload['deadline_seconds'] == 900
    assert journal.database.read_bytes() == before and not list(runtime.iterdir())
    assert not (tmp_path / 'unused-model').exists() and not (tmp_path / 'unused-key').exists()
    assert binding.timeout == 60


@pytest.mark.parametrize('mutation', ['grant', 'digest', 'owner'])
def test_operator_rejection_precedes_worker_or_runtime_mutation(tmp_path, monkeypatch, mutation):
    _, _, runtime, config, foreground, argv = cli_fixture(
        tmp_path, observe=mutation != 'grant')
    if mutation == 'digest':
        config['native_memory']['binding_digest'] = '0' * 64
    if mutation == 'owner':
        config['session']['agent_id'] = 'lizi'
    foreground.write_text(json.dumps(config))
    calls = []
    monkeypatch.setattr(MemPalaceMemoryProcess, 'integrity_check', lambda self: calls.append(1))
    with pytest.raises(ContractError):
        run(build_parser().parse_args(argv))
    assert not calls and not list(runtime.iterdir())
    assert not (tmp_path / 'unused-key').exists()


@pytest.mark.parametrize('result', ['failed', 'unavailable', 'malformed'])
def test_operator_failure_does_not_report_integrity_success(tmp_path, monkeypatch, result):
    _, _, runtime, _, _, argv = cli_fixture(tmp_path)
    calls = []

    def observe(self):
        calls.append(1)
        if result == 'unavailable':
            raise NativeMemoryUnavailable('TEST-ONLY-PRIVATE-SENTINEL')
        return integrity_result('failed') if result == 'failed' else {'status': 'ok'}

    monkeypatch.setattr(MemPalaceMemoryProcess, 'integrity_check', observe)
    if result == 'malformed':
        with pytest.raises(ContractError):
            run(build_parser().parse_args(argv))
    else:
        code, payload = run(build_parser().parse_args(argv))
        assert code == 1 and payload['integrity']['status'] == result
        assert 'TEST-ONLY-PRIVATE-SENTINEL' not in json.dumps(payload)
    assert len(calls) == 1 and not list(runtime.iterdir())


def test_binding_format_and_digest_unchanged_by_maintenance_policy():
    binding = parse(document())
    assert binding.digest == replace(binding, timeout=60).digest
    assert 'integrity_timeout' not in binding.__dataclass_fields__
    changed = document()
    changed['bindings']['hexi.durable-memory']['timeout'] = 900
    with pytest.raises(ContractError):
        parse(changed)
    parser = build_parser()
    from ahc.cli.main import CliUsageError
    with pytest.raises(CliUsageError):
        parser.parse_args(['runtime-memory-integrity-check', '--foreground-config', '/x',
                          '--profile-registry', '/y', '--timeout', '9000'])
