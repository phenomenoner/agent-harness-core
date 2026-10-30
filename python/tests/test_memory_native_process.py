import hashlib
import json

import pytest

from ahc.adapters.memory_native_process import NativeMemoryProcess, NativeMemoryUnavailable
from ahc.adapters.memory_provider_identity import provider_digest


def fixture(tmp_path, source):
    root = tmp_path / "provider"
    api = root / "openclaw_mem" / "core" / "api.py"
    api.parent.mkdir(parents=True)
    api.write_text(source)
    return api, NativeMemoryProcess(
        database=str(tmp_path / "absent.sqlite"),
        provider_root=str(root),
        provider_digest=provider_digest(root),
        timeout=5,
        scratch_root=tmp_path,
    )


def test_subprocess_no_credential_env_or_provider_logs(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "private-test-secret")
    _, adapter = fixture(
        tmp_path,
        "import os\ndef observe_status(path):\n print('PRIVATE DIAGNOSTIC')\n"
        " return {'token_absent':'TELEGRAM_BOT_TOKEN' not in os.environ}\n",
    )
    assert adapter.status() == {"token_absent": True}
    assert not (tmp_path / "absent.sqlite").exists()


def test_changed_digest_denies_before_import(tmp_path):
    api, adapter = fixture(tmp_path, "def observe_status(path): return {}\n")
    api.write_text("raise RuntimeError('changed candidate')\n")
    with pytest.raises(NativeMemoryUnavailable, match="^native memory operation unavailable$"):
        adapter.status()


def test_network_is_denied_and_errors_sanitized(tmp_path):
    _, adapter = fixture(
        tmp_path,
        "import socket\ndef observe_status(path):\n"
        " socket.getaddrinfo('example.com',443)\n return {}\n",
    )
    with pytest.raises(NativeMemoryUnavailable):
        adapter.status()


def test_operation_allowlist_and_bytecode_rejection(tmp_path):
    api, adapter = fixture(
        tmp_path, "def prepare_native(path): raise RuntimeError('must not call')\n"
    )
    with pytest.raises(NativeMemoryUnavailable):
        adapter._call("prepare_native", {})
    api.with_suffix(".pyc").write_bytes(b"not source")
    with pytest.raises(ValueError):
        provider_digest(api.parents[2])


def test_store_pre_dispatch_rejection_is_known_no_commit(tmp_path):
    api, adapter = fixture(tmp_path, "def store_committed(*a, **kw): raise AssertionError()\n")
    api.with_suffix(".pyc").write_bytes(b"unexpected test bytecode")
    observation = {"kind": "note", "summary": "bounded fixture", "detail": {}}
    result = adapter.store(scope="main", operation_id="once", observation=observation)
    envelope = {"version": 1, "scope": "main", "observation": observation}
    digest = hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(",", ":"),
                                      ensure_ascii=False).encode()).hexdigest()
    assert result == {"status": "failed", "code": "no-commit", "request_digest": digest}
    assert not (tmp_path / "absent.sqlite").exists()


def test_store_after_provider_entry_is_not_classified_no_commit(tmp_path):
    _, adapter = fixture(tmp_path,
        "from pathlib import Path\ndef store_committed(path, **kwargs):\n"
        " Path(path).write_text('effect happened')\n raise RuntimeError('lost receipt')\n")
    with pytest.raises(NativeMemoryUnavailable):
        adapter.store(scope="main", operation_id="once",
                      observation={"kind": "note", "summary": "fixture", "detail": {}})
    assert (tmp_path / "absent.sqlite").read_text() == "effect happened"


@pytest.mark.parametrize("failure", ["timeout", "wrong-request", "wrong-exit", "malformed"])
def test_unverified_transport_never_proves_no_commit(tmp_path, monkeypatch, failure):
    import subprocess
    from types import SimpleNamespace

    _, adapter = fixture(tmp_path, "def store_committed(*a, **kw): return {}\n")
    def run(*args, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired("worker", 1)
        value = {"transport": "not-dispatched",
                 "request_digest": hashlib.sha256(kwargs["input"]).hexdigest()}
        if failure == "wrong-request":
            value["request_digest"] = "0" * 64
        return SimpleNamespace(returncode=3 if failure == "wrong-exit" else 2,
            stdout=b"broken" if failure == "malformed" else json.dumps(value).encode())
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(NativeMemoryUnavailable):
        adapter.store(scope="main", operation_id="once",
                      observation={"kind": "note", "summary": "fixture", "detail": {}})


def test_pre_dispatch_failure_closes_proposal_without_unknown(tmp_path):
    from test_memory_store_service import setup_service

    api, adapter = fixture(tmp_path, "def store_committed(*a, **kw): raise AssertionError()\n")
    api.with_suffix(".pyc").write_bytes(b"unexpected bytecode")
    service, approvals, session, _ = setup_service(
        tmp_path, adapter.store, adapter.reconcile, policy="trusted")
    result = service.store_trusted(session, operation_id="once",
        observation={"kind": "note", "summary": "fixture", "detail": {}})
    assert result["status"] == "failed"
    assert approvals.query(session, result["proposal_digest"])["state"] == "failed"
