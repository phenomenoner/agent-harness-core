"""Exact-reference tool calls compose with durable runtime transitions."""

import json
from dataclasses import replace

import pytest

from skill_test_support import goal

from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.domain.workspace import CheckpointValueRejected, WorkspaceCheckpoint
from ahc.infrastructure.runtime_repository import RuntimeRepository


class Port:
    def __init__(self, identity, checkpoint):
        self.identity = identity
        self.calls = 0
        self.closed = False

    def execute(self, expected, code):
        self.calls += 1
        self.identity = replace(expected, revision=expected.revision + 1)
        return {
            "identity": self.identity.to_dict(),
            "ok": True,
            "output": "42",
            "output_truncated": False,
            "private": "PRIVATE-SENTINEL",
        }

    def checkpoint(self, expected, names):
        return WorkspaceCheckpoint(expected, '{"x":42}')

    def close(self):
        self.closed = True


def setup(tmp_path):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = repo.create(
        WorkingSetSnapshot.initial(
            VirtualSessionIdentity.initial("session", "agent", "route"), goal()
        )
    )
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    return repo, workspace, WorkspaceTools(workspace)


def test_bound_execute_checkpoint_and_stale_replay(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    try:
        request = {"expected": workspace.identity.to_dict(), "code": "x=42"}
        result = tools.invoke("workspace.execute", request)
        assert result["disposition"] == "observed"
        assert "SENTINEL" not in str(result)
        assert result["result"]["identity"]["revision"] == 1
        assert tools.invoke("workspace.execute", request)["disposition"] == "rejected"
        assert workspace.port.calls == 1
        checkpoint = tools.invoke(
            "workspace.checkpoint", {"expected": workspace.identity.to_dict(), "names": ["x"]}
        )
        assert checkpoint["disposition"] == "observed"
        assert "values_json" not in checkpoint["result"]
        saved = repo.read_workspace_checkpoint(
            workspace.identity.session, "work", checkpoint["result"]["digest"]
        )
        assert saved.values_json == '{"x":42}'
    finally:
        workspace.close()
    assert tools.capabilities() == ()


def test_tool_cannot_open_workspace_or_choose_container(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    try:
        assert tools.invoke("workspace.open", {})["disposition"] == "unsupported"
        args = {"expected": workspace.identity.to_dict(), "code": "x=42", "container_id": "a" * 64}
        assert tools.invoke("workspace.execute", args)["disposition"] == "rejected"
        assert workspace.port.calls == 0
    finally:
        workspace.close()
    assert WorkspaceTools().invoke("workspace.execute", {})["disposition"] == "unsupported"


def test_checkpoint_value_feedback_does_not_relabel_repository_or_transport_failure(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    expected = workspace.identity.to_dict()
    original = workspace.port.checkpoint

    def invalid_values(*args):
        raise CheckpointValueRejected("PRIVATE-CONTENT")

    workspace.port.checkpoint = invalid_values
    rejected = tools.invoke("workspace.checkpoint", {"expected": expected, "names": ["x"]})
    assert rejected == {"disposition": "rejected", "reason": "checkpoint-values-not-portable",
                        "unresolved_effects": False, "invalid_fields": ["names"]}
    assert "PRIVATE" not in str(rejected) and not workspace.closed
    workspace.port.checkpoint = original

    def lost_save(*args):
        raise OSError("PRIVATE-DATABASE-ERROR")

    repo.save_workspace_checkpoint = lost_save
    unknown = tools.invoke("workspace.checkpoint", {"expected": expected, "names": ["x"]})
    assert unknown["disposition"] == "unknown" and unknown["unresolved_effects"]
    assert "PRIVATE" not in str(unknown)
    workspace.close()


def test_checkpoint_capture_failure_saves_nothing_and_fences_only_workspace(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    saved = []
    repo.save_workspace_checkpoint = lambda *args: saved.append(args)
    def broken_stream(*args):
        raise OSError("PRIVATE control stream lost")
    workspace.port.checkpoint = broken_stream
    response = tools.invoke("workspace.checkpoint", {"expected": workspace.identity.to_dict(), "names": ["x"]})
    assert response["disposition"] == "rejected" and response["unresolved_effects"] is False
    assert response["reason"] == "checkpoint-capture-unavailable"
    assert response["resource_available"] is False and "PRIVATE" not in str(response)
    assert saved == [] and workspace.unavailable and workspace.closed
    assert tools.capabilities() == ()


def test_lost_execution_has_no_retry_or_exception_projection(tmp_path):
    repo, workspace, tools = setup(tmp_path)

    def fail(expected, code):
        workspace.port.calls += 1
        raise RuntimeError("PRIVATE-SENTINEL")

    workspace.port.execute = fail
    request = {"expected": workspace.identity.to_dict(), "code": "x=42"}
    result = tools.invoke("workspace.execute", request)
    assert result["disposition"] == "unknown" and result["unresolved_effects"]
    assert "SENTINEL" not in str(result)
    assert tools.invoke("workspace.execute", request)["disposition"] == "unsupported"
    assert workspace.port.calls == 1 and workspace.closed


def test_admission_contention_reports_no_execution_and_allows_corrected_call(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, workspace, tools = setup(tmp_path)
    begin = repo.begin_workspace_step
    def busy(*args):
        raise WorkingWriterBusy("PRIVATE lock details")
    repo.begin_workspace_step = busy
    identity = workspace.identity
    response = tools.invoke("workspace.execute", {"expected": identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "rejected" and response["unresolved_effects"] is False
    assert response["reason"] == "workspace-not-executed"
    assert response["workspace_failure"] == {"phase": "admission", "code": "writer-busy"}
    assert workspace.port.calls == 0 and workspace.identity == identity and not workspace.closed
    assert "PRIVATE" not in str(response)
    repo.begin_workspace_step = begin
    following = tools.invoke("workspace.execute", {"expected": identity.to_dict(), "code": "x=42"})
    assert following["disposition"] == "observed" and workspace.port.calls == 1
    workspace.close()


def test_confirmed_execution_reconciles_only_receipt_after_busy_finish(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, workspace, tools = setup(tmp_path)
    finish = repo.finish_workspace_step
    calls = []
    def once_busy(identity):
        calls.append(identity)
        if len(calls) == 1:
            raise WorkingWriterBusy("PRIVATE commit details")
        return finish(identity)
    repo.finish_workspace_step = once_busy
    response = tools.invoke("workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "observed" and not response["unresolved_effects"]
    assert workspace.port.calls == 1 and len(calls) == 2 and calls[0] == calls[1]
    assert repo.require_workspace(workspace.identity).session == workspace.identity.session
    assert response["workspace_recovery"] == "receipt-reconciled"
    assert "PRIVATE" not in str(response)
    workspace.close()


def test_lost_commit_ack_reads_ready_without_repeat_save_or_execution(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, workspace, tools = setup(tmp_path)
    finish = repo.finish_workspace_step
    calls = []
    def acknowledged_late(identity):
        calls.append(identity)
        finish(identity)
        raise WorkingWriterBusy("PRIVATE ACK lost")
    repo.finish_workspace_step = acknowledged_late
    response = tools.invoke("workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "observed" and not response["unresolved_effects"]
    assert workspace.port.calls == 1 and len(calls) == 1 and not workspace.closed
    assert response["workspace_recovery"] == "receipt-reconciled"
    workspace.close()


def test_execution_failure_retains_original_phase_when_cleanup_also_fails(tmp_path):
    _, workspace, tools = setup(tmp_path)
    def lost(*args):
        workspace.port.calls += 1
        raise OSError("PRIVATE original error")
    def cleanup_lost():
        raise RuntimeError("PRIVATE cleanup error")
    workspace.port.execute = lost
    workspace.port.close = cleanup_lost
    response = tools.invoke("workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "unknown" and response["unresolved_effects"]
    assert response["workspace_failure"] == {"phase": "execute", "code": "io", "cleanup": "unverified"}
    assert workspace.port.calls == 1 and workspace.unavailable and not workspace.closed
    assert not tools.capabilities() and "PRIVATE" not in str(response)


def test_receipt_recovery_cannot_accept_a_replaced_owner(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    from ahc.application.working_continuity import WorkingContinuityService
    repo, workspace, tools = setup(tmp_path)
    current = repo.read_current(workspace.identity.session.ref.virtual_session_id)
    calls = []
    def replaced(identity):
        calls.append(identity)
        WorkingContinuityService(repo).cancel(current)
        raise WorkingWriterBusy("PRIVATE owner changed")
    repo.finish_workspace_step = replaced
    response = tools.invoke("workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "unknown" and response["unresolved_effects"]
    assert workspace.port.calls == 1 and len(calls) == 1 and workspace.closed
    assert response["workspace_failure"]["phase"] == "result-save"
    assert "PRIVATE" not in str(response)


def test_receipt_recovery_budget_does_not_loop_or_reexecute(tmp_path):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, workspace, tools = setup(tmp_path)
    attempts = []
    def always_busy(identity):
        attempts.append(identity)
        raise WorkingWriterBusy("PRIVATE contention")
    repo.finish_workspace_step = always_busy
    response = tools.invoke("workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"})
    assert response["disposition"] == "unknown" and response["unresolved_effects"]
    assert len(attempts) == 2 and workspace.port.calls == 1 and workspace.closed
    assert response["workspace_failure"] == {"phase":"result-save", "code":"writer-busy", "cleanup":"confirmed"}


@pytest.mark.parametrize("phase", ["execute", "result-save"])
def test_concurrent_denied_admission_cannot_change_active_failure(tmp_path, phase):
    import threading
    from ahc.infrastructure.working_repository import WorkingWriterBusy

    repo, workspace, tools = setup(tmp_path)
    entered, release = threading.Event(), threading.Event()
    responses = []
    execute = workspace.port.execute

    def active(expected, code):
        result = execute(expected, code)
        entered.set()
        assert release.wait(5)
        if phase == "execute":
            raise OSError("PRIVATE active invocation error")
        return result

    workspace.port.execute = active
    saves = []
    if phase == "result-save":
        def busy(identity):
            saves.append(identity)
            raise WorkingWriterBusy("PRIVATE receipt contention")
        repo.finish_workspace_step = busy

    args = {"expected": workspace.identity.to_dict(), "code": "first"}
    worker = threading.Thread(target=lambda: responses.append(tools.invoke("workspace.execute", args)))
    worker.start()
    try:
        assert entered.wait(3)
        denied = tools.invoke("workspace.execute", {
            "expected": workspace.identity.to_dict(), "code": "competitor",
        })
        assert denied["disposition"] == "rejected" and not denied["unresolved_effects"]
        assert denied["workspace_failure"] == {"phase": "admission", "code": "writer-busy"}
    finally:
        release.set()
        worker.join(3)
    assert not worker.is_alive() and len(responses) == 1
    assert responses[0]["disposition"] == "unknown" and responses[0]["unresolved_effects"]
    assert responses[0]["workspace_failure"] == {
        "phase": phase, "code": "io" if phase == "execute" else "writer-busy",
        "cleanup": "confirmed",
    }
    assert workspace.unavailable and workspace.closed and workspace.port.closed
    assert workspace.port.calls == 1 and len(saves) == (2 if phase == "result-save" else 0)
    assert not tools.capabilities() and "PRIVATE" not in str(responses)


@pytest.mark.parametrize("recovered_revision", [1, 2])
def test_each_caller_keeps_its_receipt_metadata_and_confirmed_identity(tmp_path, recovered_revision):
    import threading
    from ahc.infrastructure.working_repository import WorkingWriterBusy

    repo, workspace, tools = setup(tmp_path)
    entered, release = threading.Event(), threading.Event()
    finish, execute = repo.finish_workspace_step, workspace.execute
    attempts, responses = [], []

    def receipt(identity):
        attempts.append(identity.revision)
        if identity.revision == recovered_revision and attempts.count(identity.revision) == 1:
            raise WorkingWriterBusy("PRIVATE first receipt busy")
        return finish(identity)

    def hold_tool_return(expected, code):
        result = execute(expected, code)
        if code == "first":
            entered.set()
            assert release.wait(5)
        return result

    repo.finish_workspace_step = receipt
    workspace.execute = hold_tool_return
    args = {"expected": workspace.identity.to_dict(), "code": "first"}
    worker = threading.Thread(target=lambda: responses.append(tools.invoke("workspace.execute", args)))
    worker.start()
    try:
        assert entered.wait(3)
        following = tools.invoke("workspace.execute", {
            "expected": workspace.identity.to_dict(), "code": "second",
        })
    finally:
        release.set()
        worker.join(3)
        workspace.close()
    assert not worker.is_alive() and len(responses) == 1
    for revision, response in ((1, responses[0]), (2, following)):
        assert response["disposition"] == "observed" and not response["unresolved_effects"]
        assert response["result"]["identity"]["revision"] == revision
        assert response.get("workspace_recovery") == (
            "receipt-reconciled" if revision == recovered_revision else None
        )
    assert workspace.port.calls == 2 and attempts.count(recovered_revision) == 2


@pytest.mark.parametrize("phase", ["execute", "result-save"])
def test_possible_effect_failure_fences_workspace_before_lock_exit(tmp_path, phase):
    from contextlib import contextmanager
    from ahc.infrastructure.working_repository import WorkingWriterBusy

    repo, workspace, tools = setup(tmp_path)
    lock = repo.workspace_execution_lock
    states = []

    @contextmanager
    def observed_lock(expected):
        with lock(expected):
            try:
                yield
            finally:
                states.append((workspace.unavailable, workspace.closed, workspace.port.closed))

    repo.workspace_execution_lock = observed_lock
    if phase == "execute":
        def lost(*args):
            workspace.port.calls += 1
            raise OSError("PRIVATE execution failed")
        workspace.port.execute = lost
    else:
        def busy(identity):
            raise WorkingWriterBusy("PRIVATE receipt still busy")
        repo.finish_workspace_step = busy

    response = tools.invoke("workspace.execute", {
        "expected": workspace.identity.to_dict(), "code": "first",
    })
    assert response["disposition"] == "unknown" and response["unresolved_effects"]
    assert states == [(True, True, True)] and workspace.port.calls == 1


def test_real_sqlite_reader_contention_reconciles_only_the_known_result(tmp_path):
    import sqlite3
    import threading
    repo, workspace, tools = setup(tmp_path)
    ready, release = threading.Event(), threading.Event()
    errors = []
    def reader():
        try:
            with sqlite3.connect(repo.db_path) as connection:
                connection.execute("BEGIN")
                connection.execute("SELECT * FROM workspace_heads").fetchall()
                ready.set()
                if not release.wait(5):
                    errors.append("reader release timed out")
                connection.rollback()
        except Exception as error:
            errors.append(type(error).__name__)
            ready.set()
    worker = threading.Thread(target=reader)
    execute = workspace.port.execute
    def with_reader(*args):
        result = execute(*args)
        worker.start()
        assert ready.wait(2) and not errors
        return result
    workspace.port.execute = with_reader
    observe = repo.observe_workspace_step
    def read_then_release(identity):
        state = observe(identity)
        release.set();worker.join(timeout=2)
        assert not worker.is_alive() and not errors
        return state
    repo.observe_workspace_step = read_then_release
    try:
        response = tools.invoke("workspace.execute", {"expected":workspace.identity.to_dict(),"code":"x=42"})
        assert response["disposition"] == "observed" and not response["unresolved_effects"]
        assert response["workspace_recovery"] == "receipt-reconciled"
        assert workspace.port.calls == 1 and not worker.is_alive() and not errors
        assert repo.observe_workspace_step(workspace.identity) == "ready"
    finally:
        release.set()
        if worker.ident is not None:worker.join(timeout=2)
        workspace.close()


def test_rejected_schema_names_the_field_and_public_version_before_any_effect(tmp_path):
    _, workspace, tools = setup(tmp_path)
    try:
        expected = workspace.identity.to_dict()
        expected["session"]["ref"]["schema_version"] = "model-invented-ref"
        rejected = tools.invoke("workspace.execute", {"expected": expected, "code": "x=42"})
        assert rejected["reason"] == "invalid-workspace-request"
        assert rejected["invalid_fields"] == ["expected.session.ref.schema_version"]
        assert rejected["required_schema_versions"] == {
            "expected.session.ref.schema_version": "ahc.virtual-session-ref.v1",
        }
        assert workspace.port.calls == 0
        expected["session"]["ref"]["schema_version"] = rejected["required_schema_versions"][
            "expected.session.ref.schema_version"]
        assert tools.invoke("workspace.execute", {"expected": expected, "code": "x=42"})[
            "disposition"] == "observed"
        assert workspace.port.calls == 1
    finally:
        workspace.close()


def test_rejection_feedback_does_not_echo_foreign_values_or_untrusted_field_names(tmp_path):
    _, workspace, tools = setup(tmp_path)
    try:
        expected = workspace.identity.to_dict()
        expected["session"]["agent_id"] = "PRIVATE-FOREIGN-AGENT"
        result = tools.invoke("workspace.execute", {"expected": expected, "code": "x=42"})
        assert result["invalid_fields"] == ["expected.session.agent_id"]
        assert "PRIVATE" not in json.dumps(result) and "required_schema_versions" not in result
        expected["PRIVATE-UNTRUSTED-FIELD"] = "PRIVATE-VALUE"
        result = tools.invoke("workspace.execute", {"expected": expected, "code": "x=42"})
        assert result["invalid_fields"] == ["expected.fields"]
        assert "PRIVATE" not in json.dumps(result) and "expected" not in result
        assert workspace.port.calls == 0
    finally:
        workspace.close()


def test_actual_encoded_frame_rejects_before_execution_and_former_16k_code_continues(tmp_path):
    from ahc.adapters.wsl_python_workspace import WslPythonWorkspace

    _, workspace, tools = setup(tmp_path)
    validator = WslPythonWorkspace.__new__(WslPythonWorkspace)
    validator._sequence, validator._batch = 0, None
    workspace.port.validate_execute = validator.validate_execute
    try:
        expected = workspace.identity.to_dict()
        code = "#" + "\x01" * 11000
        rejected = tools.invoke("workspace.execute", {"expected": expected, "code": code})
        assert rejected["reason"] == "workspace-code-frame-invalid"
        assert rejected["invalid_fields"] == ["code"]
        assert rejected["request_bytes"] > 65536
        assert rejected["max_request_bytes"] == 65536
        assert rejected["unresolved_effects"] is False
        assert code not in str(rejected)
        assert workspace.port.calls == 0 and workspace.identity.to_dict() == expected
        accepted = tools.invoke("workspace.execute", {"expected": expected, "code": "#"+"é"*8192})
        assert accepted["result"]["ok"] and workspace.port.calls == 1
        stale = tools.invoke("workspace.execute", {"expected": expected, "code": code})
        assert stale["invalid_fields"] == ["expected.revision"]
        assert "code_bytes" not in stale and "identity" not in stale
        assert workspace.port.calls == 1
    finally:
        workspace.close()
