"""Environment handoff is available without persona/history and never grants host access."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.cli.foreground_turn_context import turn_context
from ahc.cli.runtime_environment import ENVIRONMENT_INSTRUCTIONS, environment_snapshot
from ahc.domain.sessions import ContractError


def test_environment_reaches_each_new_thread_with_full_persona_budget():
    from test_codex_client import Peer, setup

    _, _, request = setup()
    for operation in ("first", "after-restart"):
        peer = Peer()
        client = CodexProtocolClient(
            peer, cwd="workspace", model="fixture", clock=lambda: 0,
            developer_instructions="p" * 12000,
            environment_instructions=ENVIRONMENT_INSTRUCTIONS)
        bound = replace(request, operation_id=operation,
                        configuration_digest=client.configuration_digest)
        assert client.run_admitted(bound).disposition == "completed"
        start = next(m["params"] for m in peer.sent if m.get("method") == "thread/start")
        assert start["developerInstructions"].startswith(ENVIRONMENT_INSTRUCTIONS)
        assert start["developerInstructions"].endswith("p" * 12000)
        assert start["developerInstructions"].count(ENVIRONMENT_INSTRUCTIONS) == 1
        changed = CodexProtocolClient(
            Peer(), cwd="workspace", model="fixture", clock=lambda: 0,
            developer_instructions="p" * 12000, environment_instructions="different contract")
        assert changed.configuration_digest != client.configuration_digest


@pytest.mark.parametrize("invalid", ["", "x" * 4097, "\ud800", 42])
def test_environment_instruction_bound_is_separate_from_persona(invalid):
    with pytest.raises(ContractError, match="environment instructions"):
        CodexProtocolClient(None, cwd="workspace", model="fixture",
                            environment_instructions=invalid)


@pytest.mark.parametrize("text", ["continue", "[ISO] continue", "[NOMEM] continue"])
def test_environment_survives_disabled_persona_and_history(text, tmp_path):
    from test_runtime_core import setup

    store, _, request = setup(tmp_path)
    document = {"runtime_root": "D:/runtime", "password": "PRIVATE-SENTINEL"}
    config = SimpleNamespace(session=request.session,
        workspace={"kind": "fixture", "cwd": "/home/agent/work", "distribution": "AHC"},
        to_dict=lambda: document)
    tools = SimpleNamespace(workspace=SimpleNamespace(store=store),
                            capabilities=lambda: ("workspace.environment", "workspace.inspect"))
    result = turn_context(store, config, tools, replace(request, input_text=text))
    payload = json.loads(result.split("\n", 1)[1])
    environment = payload["runtime_environment"]
    assert environment["backend"]["cwd"] == {
        "status": "configured_not_probed", "path": "/home/agent/work"}
    assert environment["host_path_access"]["mappings"] == []
    assert environment["host_path_access"]["mounts"] == "not_probed"
    assert environment["available_workspace_tools"] == list(tools.capabilities())
    assert payload["conversation_history"] == [] and "PRIVATE-SENTINEL" not in result
    document["runtime_root"] = "D:/" + "x" * 2000
    assert environment_snapshot(config, tools)["host"]["runtime_state"] == {
        "status": "omitted_over_limit"}


def test_tool_free_child_gets_environment_without_invented_tools(tmp_path):
    from test_foreground_runtime import setup
    from test_trusted_codex_backend import factory_for

    runtime, _, _, request = setup(tmp_path)
    client = factory_for(runtime)._client(None, None, lambda: False)
    assert client.environment_instructions == ENVIRONMENT_INSTRUCTIONS
    payload = json.loads(client.turn_context(request).split("\n", 1)[1])
    assert payload["runtime_environment"]["available_workspace_tools"] == []
    assert payload["runtime_environment"]["operating_guide"] is None
    assert payload["runtime_environment"]["workbench"]["state_access"] == []


def test_guide_is_packaged_readonly_and_rejects_stale_or_arbitrary_arguments(tmp_path):
    from test_workspace_tools import setup

    _, workspace, tools = setup(tmp_path)
    identity = workspace.identity
    try:
        result = tools.invoke("workspace.environment", {"expected": identity.to_dict()})
        assert result["disposition"] == "observed" and not result["unresolved_effects"]
        assert "not automatically /mnt/d" in result["result"]["guide"]
        assert len(result["result"]["guide"]) <= 8192
        assert workspace.identity == identity and workspace.port.calls == 0
        for arguments in (
            {"expected": replace(identity, revision=identity.revision + 1).to_dict()},
            {"expected": identity.to_dict(), "path": "D:/secret"},
        ):
            assert tools.invoke("workspace.environment", arguments)["disposition"] == "rejected"
    finally:
        workspace.close()


def test_guide_has_an_actual_dynamic_declaration_and_readonly_admission(tmp_path):
    from test_channel_tool_approval import ChannelToolGate, admitted, authorize
    from test_codex_client import setup as client_setup
    from test_workspace_tools import setup

    _, workspace, tools = setup(tmp_path)
    try:
        _, _, request = client_setup()
        request = replace(request, capabilities=tools.capabilities())
        namespace = CodexWorkspaceTools.describe(request, tools)[0]
        guide = next(t for t in namespace["tools"] if t["name"] == "environment")
        assert guide["inputSchema"]["required"] == ["expected"]
        for mode in ("manual", "auto-approve", "always-approve", "yolo"):
            gate = ChannelToolGate(admitted(mode=mode, capabilities=("workspace.environment",)),
                                   None, clock=lambda: 10)
            assert authorize(gate, "workspace.environment")
            assert not authorize(gate, "workspace.execute")
    finally:
        workspace.close()
