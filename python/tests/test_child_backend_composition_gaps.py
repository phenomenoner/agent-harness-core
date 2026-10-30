"""Constructibility probes documenting why the parent factory cannot launch a child yet."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_child_dispatch import state

from ahc.adapters.child_agent_backend import ChildAgentBackend
from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.custodied_backend import CustodiedBackend
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_child_ledger_is_not_parent_turn_admission(tmp_path):
    repo, parent, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, "token")
    child = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text)
    workspace = WorkspaceIdentity(parent.session, parent.workspace_scope, 1, 0)
    backend = CustodiedBackend(repo, workspace, object(), (), 60, lambda _: None, custody=object())
    with pytest.raises(ContractError, match="durable admitted turn"):
        backend.invoke(child)
    assert not backend.used and backend.lease is None
    assert repo.read_turn(parent.session, envelope.child_id) is None


def test_parent_backend_owner_excludes_same_scope_child(tmp_path):
    repo, parent, _, _, _ = state(tmp_path)
    custody = BackendCustody(repo)
    owner = custody.acquire(parent.session, parent.workspace_scope)
    try:
        with pytest.raises(WorkingWriterBusy):
            custody.acquire(parent.session, parent.workspace_scope)
    finally:
        owner.close()


def test_tool_free_child_has_distinct_protocol_configuration(tmp_path):
    _, parent, _, _, envelope = state(tmp_path)
    workspace = SimpleNamespace(
        identity=WorkspaceIdentity(parent.session, parent.workspace_scope, 1, 0))
    tools = SimpleNamespace(workspace=workspace, capabilities=lambda: ("workspace.execute",))
    parent_client = CodexProtocolClient(
        None, cwd="/workspace", model="fixture", workspace_tools=tools)
    child_client = CodexProtocolClient(None, cwd="/workspace", model="fixture")
    assert parent_client.executable_sha256 == child_client.executable_sha256
    assert parent_client.configuration_digest != child_client.configuration_digest
    child = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                    capabilities=(), configuration_digest=child_client.configuration_digest)
    def factory(*a, **k):
        return None

    with pytest.raises(ContractError, match="binding"):
        ChildAgentBackend(envelope, child, factory, parent_request=parent)
    port = ChildAgentBackend(envelope, child, factory, parent_request=parent,
                             configuration_digest=child_client.configuration_digest)
    assert not port.used and port.backend is None
