from dataclasses import replace
import os
from types import SimpleNamespace

import pytest
from test_backend_custody import Runner
from test_foreground_config_v2 import valid_config
from test_foreground_config_native import native_config
from test_runtime_core import setup

from ahc.application.child_dispatch import ChildDispatchService
from ahc.cli import trusted_codex_child
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.cli.trusted_codex_child import TrustedCodexChildFactory
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.child_approval_store import ChildApprovalStore
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository


@pytest.mark.parametrize("effort", [None, "max"])
def test_concrete_child_factory_composes_protocol_approval_and_custody(
    tmp_path, monkeypatch, effort
):
    repo, snapshot, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(snapshot, original.workspace_scope)
    raw = native_config() if os.name == 'posix' else valid_config()
    raw["runtime_root"] = str(repo.root)
    raw["backend"]["enabled"] = True
    raw["backend"]["model"] = "gpt-5.6-luna"
    if effort is not None:
        raw["backend"]["reasoning_effort"] = effort
    config = (parse_foreground_config_native(raw) if os.name == 'posix'
              else parse_foreground_config_v2(raw))
    owner = TrustedCodexBackendFactory(repo, config)
    tools = SimpleNamespace(workspace=SimpleNamespace(identity=workspace),
                            capabilities=lambda: ("workspace.execute",))
    artifact, parent_config = owner.binding(tools)
    parent = replace(original, artifact_digest=artifact, configuration_digest=parent_config,
                     effective_policy=config.effective_policy, deadline=100)
    repo.begin(repo.read_current("session"), parent)
    children = ChildDispatchRepository(repo, parent, clock=lambda: 90000, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope, 100000)
    factory = TrustedCodexChildFactory(owner, children, envelope, "token", workspace)
    _, child_config = factory.binding()
    assert child_config != parent_config
    request = factory.build_request()
    assert request.operation_id == "child" and request.input_text == "task"
    assert request.deadline == 9.999 and request.capabilities == ()
    assert request.backend_thread is None and request.effective_policy == parent.effective_policy
    assert request.skill_bodies == parent.skill_bodies
    assert request.revision_digests == parent.revision_digests
    assert request.configuration_digest == child_config
    effects = []
    unit = LinuxUnitIdentity("AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc", "ahc-backend-" + "c" * 32 + ".service", "b" * 32)

    class Process(Runner):
        def __init__(self, *args, **kwargs):
            super().__init__(unit)

        def create(self, command, *, lifetime_seconds):
            effects.append("create")
            assert command == owner._command and lifetime_seconds == 10
            return unit

        def attach(self, selected, custody, lease):
            assert selected == unit and custody.envelope == envelope
            assert custody._held(lease)[0][2] == unit
            return SimpleNamespace(close=lambda: effects.append("transport-close"))

        def close(self):
            effects.append("runner-close")

    monkeypatch.setattr(owner, "new_runner", Process)
    real_client = owner._client

    def client(transport, selected_tools, cancelled, approval_port=None, *, child=False):
        if transport is None:
            return real_client(transport, selected_tools, cancelled, approval_port, child=child)
        assert selected_tools is None and isinstance(approval_port.store, ChildApprovalStore)
        assert approval_port.admitted == request
        configured = real_client(transport, selected_tools, cancelled, approval_port, child=child)
        assert child and configured.settlement_within_task_deadline
        assert configured.model == "gpt-5.6-luna"
        assert configured.reasoning_effort == effort
        assert configured.configuration_digest == request.configuration_digest

        def run(bound):
            assert bound == request
            effects.append("invoke")
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1, output_text="answer")
        return SimpleNamespace(run_admitted=run)

    monkeypatch.setattr(owner, "_client", client)
    parent_custody = BackendCustody(repo)
    lease = parent_custody.acquire(parent.session, parent.workspace_scope)
    try:
        with pytest.raises(ContractError):
            factory.port(replace(request, configuration_digest=parent_config))
        assert effects == []
        service = ChildDispatchService(children, lambda _: factory.port())
        with repo.execution_lock(), repo.workspace_turn_scope(parent):
            result = service.dispatch(envelope, "token")
            assert result["state"] == "terminal" and result["result"] == {"output_text": "answer"}
            assert service.dispatch(envelope, "other")["state"] == "terminal"
        assert effects == ["create", "invoke", "transport-close", "runner-close"]
        assert lease.active and repo.read_turn(parent.session, "child") is None
        with repo._read() as connection:
            row = connection.execute("SELECT state FROM backend_custody").fetchone()
            assert row[0] == "cleaned"
    finally:
        lease.close()


@pytest.mark.parametrize("wall,parent_deadline,expected", [
    (90000, 5, 5), (90000, 100, 9.999), (99999, 100, None), (90000, 0, None),
])
def test_child_request_uses_earliest_deadline_without_starting_backend(
        tmp_path, wall, parent_deadline, expected):
    repo, snapshot, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(snapshot, original.workspace_scope)
    raw = valid_config()
    raw["backend"]["enabled"] = True
    owner = TrustedCodexBackendFactory(repo, parse_foreground_config_v2(raw))
    parent = replace(original, deadline=parent_deadline, backend_thread="parent-thread",
                     capabilities=("workspace.execute",))
    children = ChildDispatchRepository(repo, parent, clock=lambda: wall,
                                       monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope, 100000)
    factory = TrustedCodexChildFactory(owner, children, envelope, "token", workspace)
    if expected is None:
        with pytest.raises(ContractError, match="expired"):
            factory.build_request()
    else:
        request = factory.build_request()
        assert request.deadline == expected
        assert request.backend_thread is None and request.capabilities == ()
        assert request.upstream_request_limit == parent.upstream_request_limit
    assert repo.read_turn(parent.session, "child") is None
