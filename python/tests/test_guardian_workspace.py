"""Parent-side indeterminate custody and hostile reply regressions; no child launch."""

import io
import json
import threading

import pytest

from ahc.adapters.docker_workspace import WorkspaceUnavailable
from ahc.adapters.guardian_workspace import GuardianWorkspace
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity


def proxy(messages=(), code=0):
    class Process:
        stdin = io.BytesIO()
        stdout = io.BytesIO(b"".join(json.dumps(m).encode() + b"\n" for m in messages))

        def wait(self, timeout):
            return code

    port = GuardianWorkspace.__new__(GuardianWorkspace)
    port._identity = WorkspaceIdentity(
        VirtualSessionIdentity.initial("session", "agent", "route"), "work"
    )
    port._process = Process()
    port._closed = port._fault = False
    port._lock = threading.RLock()
    port._sequence = 0
    port._timeout = 1
    return port


def test_nonzero_exit_never_becomes_successful_close_on_retry():
    port = proxy([{"id": 1, "ok": True, "closed": True}], code=1)
    with pytest.raises(WorkspaceUnavailable):
        port.close()
    with pytest.raises(WorkspaceUnavailable):
        port.close()
    assert not port._closed


def test_close_ack_and_zero_exit_are_both_required():
    port = proxy([{"id": 1, "ok": True, "closed": True}])
    port.close()
    port.close()
    assert port._closed and port._sequence == 1


def test_malformed_response_consumes_generation_without_replay():
    port = proxy([{"id": True, "ok": True, "result": {}}])
    expected = port.identity
    with pytest.raises(WorkspaceUnavailable):
        port.execute(expected, "x=42")
    assert port.identity.revision == 1 and port._sequence == 1
    with pytest.raises(ContractError):
        port.execute(port.identity, "x=42")
    assert port._sequence == 1


def test_extra_output_fields_are_not_projected():
    port = proxy()
    expected = port.identity
    identity = {**expected.to_dict(), "revision": 1}
    result = {
        "identity": identity,
        "ok": True,
        "output": "42",
        "output_truncated": False,
        "error": None,
        "private": "SENTINEL",
    }
    port._process.stdout = io.BytesIO(
        json.dumps({"id": 1, "ok": True, "result": result}).encode() + b"\n"
    )
    with pytest.raises(WorkspaceUnavailable):
        port.execute(expected, "print(42)")


def test_invalid_checkpoint_response_retires_port():
    port = proxy([{"id": 1, "ok": True, "result": {"origin": {}, "values_json": "{}"}}])
    with pytest.raises(WorkspaceUnavailable):
        port.checkpoint(port.identity, ("x",))
    assert port._fault and port._closed


def test_unqualified_topology_rejects_before_paths_or_processes(monkeypatch):
    from ahc.adapters import guardian_workspace

    def reject():
        raise WorkspaceUnavailable("unqualified topology")

    def forbidden(*args, **kwargs):
        pytest.fail("launch or Docker path inspection occurred")

    monkeypatch.setattr(guardian_workspace, "require_independent_guardian_host", reject)
    monkeypatch.setattr(guardian_workspace, "DockerRunner", forbidden)
    monkeypatch.setattr(guardian_workspace.subprocess, "Popen", forbidden)
    with pytest.raises(WorkspaceUnavailable, match="unqualified topology"):
        GuardianWorkspace(None, None, None)
