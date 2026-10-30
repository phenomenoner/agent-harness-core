import sys
import time
from dataclasses import replace
from types import ModuleType

import pytest

from ahc.adapters import wsl_python_workspace as module
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity

POLICY = EffectivePolicy(1, "manual", "read-only", True, "a" * 64, 30)
IDENTITY = WorkspaceIdentity(VirtualSessionIdentity.initial("session", "agent", "route"), "work")


class Lease:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class Custody:
    instances = []

    def __init__(self, store, *, resource_role):
        assert resource_role == "python-workspace"
        self.calls = []
        self.lease = Lease()
        type(self).instances.append(self)

    def acquire(self, session, workspace):
        self.calls.append(("acquire", session, workspace))
        return self.lease

    def recover(self, lease, runner):
        self.calls.append(("recover", lease, runner))
        return ()

    def create(self, lease, identity, runner, command, *, lifetime_seconds):
        self.calls.append(("create", identity, command, lifetime_seconds))
        return "owned-unit"


class Transport:
    def __init__(self):
        self.values, self.request, self.closed = {}, None, False

    def send(self, request, *, deadline):
        assert deadline > 0
        self.request = request

    def receive(self, *, deadline):
        request = self.request
        result = {"id": request["id"], "ok": True}
        operation = request["operation"]
        if operation == "restore":
            self.values.update(request["values"])
        elif operation == "execute":
            if request["code"] == "raise RuntimeError()":
                result.update(output="", output_truncated=False, ok=False)
            else:
                exec(request["code"], self.values)
                result.update(output="", output_truncated=False)
        elif operation == "inspect":
            result["variables"] = [
                {"name": name, "kind": "integer" if name in self.values else "missing",
                 "size": None}
                for name in request["names"]
            ]
        elif operation == "checkpoint":
            result["values"] = {name: self.values[name] for name in request["names"]}
        return result

    def close(self):
        self.closed = True


class Runner:
    distribution = "AHC-Codex-Ubuntu-24.04"

    def __init__(self):
        self.transport = Transport()
        self.attached = []

    def attach(self, unit, custody, lease):
        self.attached.append((unit, custody, lease))
        return self.transport


class AttachFailureRunner(Runner):
    def __init__(self):
        super().__init__()
        self.close_called = False

    def attach(self, unit, custody, lease):
        raise OSError("stream attachment lost")

    def close(self):
        self.close_called = True


def open_workspace(monkeypatch, **kwargs):
    Custody.instances.clear()
    monkeypatch.setattr(module, "BackendCustody", Custody)
    runner = kwargs.pop("runner", Runner())
    workspace = module.WslPythonWorkspace(
        object(), runner, kwargs.pop("identity", IDENTITY),
        effective_policy=kwargs.pop("effective_policy", POLICY),
        environment_digest=kwargs.pop("environment_digest", "a" * 64),
        cwd=kwargs.pop("cwd", "/home/ahc"), **kwargs,
    )
    return workspace, runner, Custody.instances[-1]


def test_nonportable_checkpoint_is_rejected_before_save_and_keeps_workspace_usable(monkeypatch):
    workspace, runner, _ = open_workspace(monkeypatch)
    try:
        workspace.execute(workspace.identity, "context={'rate':'0.002'}; metrics={'value':0.2}")
        identity = workspace.identity
        with pytest.raises(ContractError) as error:
            workspace.checkpoint(identity, ("context", "metrics"))
        assert type(error.value).__name__ == "CheckpointValueRejected"
        assert workspace.identity == identity and not runner.transport.closed
        saved = workspace.checkpoint(identity, ("context",))
        assert saved.values_json == '{"context":{"rate":"0.002"}}'
        assert workspace.execute(identity, "context['saved']=True")["ok"]
    finally:
        workspace.close()


def test_persistent_namespace_and_structured_error_keep_generation_healthy(monkeypatch):
    workspace, runner, custody = open_workspace(monkeypatch)
    try:
        first = workspace.execute(workspace.identity, "x = 41")
        assert first["ok"] and workspace.execute(workspace.identity, "x += 1")["ok"]
        assert runner.transport.values["x"] == 42 and workspace.identity.revision == 2
        error = workspace.execute(workspace.identity, "raise RuntimeError()")
        assert error["error"] == "execution-error" and runner.transport.values["x"] == 42
        assert workspace.inspect(workspace.identity, ("x",))["variables"][0]["kind"] == "integer"
    finally:
        workspace.close()
    assert runner.transport.closed and custody.lease.closed
    assert [call[0] for call in custody.calls].count("recover") == 2
    command = next(call[2] for call in custody.calls if call[0] == "create")
    assert command[:4] == ("/usr/bin/python3", "-I", "-u", "-c")
    assert len(command[4]) <= 4096 and "\n" not in command[4]


def test_kernel_command_applies_the_explicit_posix_cwd(monkeypatch):
    seen = []
    fake_os = ModuleType("os")
    fake_os.chdir = seen.append
    monkeypatch.setitem(sys.modules, "os", fake_os)
    from ahc.adapters import workspace_launch
    monkeypatch.setattr(workspace_launch, "KERNEL", "pass")
    exec(module._kernel_command("/home/ahc"), {})
    assert seen == ["/home/ahc"]


def test_checkpoint_restore_uses_new_generation_and_rejects_stale_identity(monkeypatch):
    workspace, runner, _ = open_workspace(monkeypatch)
    try:
        workspace.execute(workspace.identity, "x = 42")
        checkpoint = workspace.checkpoint(workspace.identity, ("x",))
        old = workspace.identity
        successor = workspace.restore(old, checkpoint)
        assert successor.generation == old.generation + 1 and runner.transport.values["x"] == 42
        with pytest.raises(ContractError):
            workspace.execute(old, "x = 1")
    finally:
        workspace.close()


@pytest.mark.parametrize(
    "change",
    [
        {"effective_policy": replace(POLICY, workspace_granted=False)},
        {"environment_digest": "b" * 64},
        {"cwd": "relative"},
        {"interpreter": "/"},
        {"user": "root"},
        {"timeout": 3601},
        {"lifetime_seconds": 3601},
        {"runner": type("Foreign", (), {"distribution": "Ubuntu"})()},
    ],
)
def test_rejects_ungranted_or_non_dedicated_configuration_before_custody(monkeypatch, change):
    Custody.instances.clear()
    monkeypatch.setattr(module, "BackendCustody", Custody)
    with pytest.raises(ContractError):
        open_workspace(monkeypatch, **change)
    assert Custody.instances == []


def test_cancel_terminates_recorded_generation_without_replay(monkeypatch):
    cancelled = [False]
    workspace, runner, custody = open_workspace(monkeypatch, cancelled=lambda: cancelled[0])
    cancelled[0] = True
    with pytest.raises(module.WorkspaceUnavailable, match="cancelled"):
        workspace.execute(workspace.identity, "x = 1")
    assert runner.transport.closed and custody.lease.closed
    with pytest.raises(ContractError):
        workspace.execute(workspace.identity, "x = 2")


def test_timeout_can_exceed_legacy_thirty_second_ceiling(monkeypatch):
    workspace, _, _ = open_workspace(monkeypatch, timeout=301)
    workspace.close()


@pytest.mark.parametrize("phase", ["send", "receive"])
def test_fixed_transport_cause_survives_workspace_translation(monkeypatch, phase):
    from ahc.adapters.codex_transport import CodexTransportError
    workspace, runner, _ = open_workspace(monkeypatch)
    def lost(*args, **kwargs):
        raise CodexTransportError("PRIVATE wire payload", transport_error_type="eof")
    setattr(runner.transport, phase, lost)
    with pytest.raises(module.WorkspaceUnavailable):
        workspace.execute(workspace.identity, "x=42")
    assert workspace.last_failure == {"phase": phase, "code": "eof", "cleanup": "confirmed"}
    assert runner.transport.closed and workspace._closed
    assert "PRIVATE" not in str(workspace.last_failure)


def test_original_transport_cause_is_not_replaced_by_cleanup_failure(monkeypatch):
    from ahc.adapters.codex_transport import CodexTransportError
    workspace, runner, _ = open_workspace(monkeypatch)
    def lost(**kwargs):
        raise CodexTransportError("PRIVATE payload", transport_error_type="malformed-json")
    runner.transport.receive = lost
    original = workspace._terminate_indeterminate
    def failed_cleanup():
        raise OSError("PRIVATE cleanup")
    workspace._terminate_indeterminate = failed_cleanup
    with pytest.raises(module.WorkspaceUnavailable, match="execution is indeterminate"):
        workspace.execute(workspace.identity, "x=42")
    assert workspace.last_failure == {"phase":"receive","code":"malformed-json","cleanup":"unverified"}
    workspace._terminate_indeterminate = original
    workspace.close()


def test_invalid_returned_output_has_its_own_phase(monkeypatch):
    workspace, runner, _ = open_workspace(monkeypatch)
    def malformed(**kwargs):
        return {"id":runner.transport.request["id"],"ok":True,"output":42,"output_truncated":False}
    runner.transport.receive = malformed
    with pytest.raises(module.WorkspaceUnavailable, match="output is invalid"):
        workspace.execute(workspace.identity, "x=42")
    assert workspace.last_failure == {"phase":"result-validation","code":"invalid-output","cleanup":"confirmed"}
    assert workspace._closed


def test_admitted_turn_uses_its_deadline_instead_of_a_thirty_second_cell_limit(monkeypatch):
    workspace, runner, _ = open_workspace(monkeypatch, timeout=30)
    seen = []
    send = runner.transport.send
    runner.transport.send = lambda request, *, deadline: (seen.append(deadline), send(request, deadline=deadline))
    deadline = time.monotonic() + 120
    try:
        with workspace.turn_scope(deadline):
            workspace.execute(workspace.identity, "answer=42")
            assert seen[-1] == deadline
            assert not workspace._closed
        workspace.execute(workspace.identity, "answer+=1")
        assert 29 < seen[-1] - time.monotonic() <= 30
        assert runner.transport.values["answer"] == 43
    finally:
        workspace.close()


def test_admitted_parent_does_not_lock_out_a_child_thread_kernel_call(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    workspace, runner, _ = open_workspace(monkeypatch, timeout=30)
    pool=ThreadPoolExecutor(max_workers=1)
    try:
        with workspace.turn_scope(time.monotonic()+120):
            future=pool.submit(workspace.execute,workspace.identity,'child_answer=42')
            result=future.result(timeout=2)
            assert result['ok'] is True and runner.transport.values['child_answer']==42
    finally:
        pool.shutdown(wait=True)
        workspace.close()


def test_expired_or_invalid_turn_scope_never_executes(monkeypatch):
    workspace, runner, _ = open_workspace(monkeypatch)
    try:
        for deadline in (time.monotonic() - 1, float('nan'), True):
            with pytest.raises(ContractError):
                with workspace.turn_scope(deadline):
                    pytest.fail("invalid turn was admitted")
        assert "answer" not in runner.transport.values
        assert workspace.execute(workspace.identity, "answer=42")["ok"]
    finally:
        workspace.close()


def test_cancel_during_silent_receive_ends_owned_transport(monkeypatch):
    cancelled = [False]
    workspace, runner, custody = open_workspace(monkeypatch, cancelled=lambda: cancelled[0])
    runner.transport.receive = lambda *, deadline: time.sleep(1)
    timer = __import__("threading").Timer(0.02, lambda: cancelled.__setitem__(0, True))
    timer.start()
    try:
        with pytest.raises(module.WorkspaceUnavailable, match="cancelled"):
            workspace.execute(workspace.identity, "x = 1")
    finally:
        timer.cancel()
    assert runner.transport.closed and custody.lease.closed


def test_bad_transport_reply_terminates_recorded_generation(monkeypatch):
    workspace, runner, custody = open_workspace(monkeypatch)
    runner.transport.receive = lambda *, deadline: {"id": True, "ok": True}
    with pytest.raises(module.WorkspaceUnavailable, match="indeterminate"):
        workspace.execute(workspace.identity, "x = 1")
    assert runner.transport.closed and custody.lease.closed


def test_attach_failure_closes_pending_stream_then_retains_custody_recovery(monkeypatch):
    runner = AttachFailureRunner()
    Custody.instances.clear()
    monkeypatch.setattr(module, "BackendCustody", Custody)
    with pytest.raises(OSError, match="attachment"):
        module.WslPythonWorkspace(
            object(), runner, IDENTITY, effective_policy=POLICY,
            environment_digest="a" * 64, cwd="/home/ahc",
        )
    custody = Custody.instances[-1]
    assert runner.close_called and custody.lease.closed
    assert [call[0] for call in custody.calls].count("recover") == 2
