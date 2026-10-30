"""Authorized metadata reaches the actual turn input; procedures remain lazy reads."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from skill_test_support import revision
from test_codex_client import setup
from test_skill_graph_convergence import _activate_with_report
from test_workspace_skills import call, tools_for

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.cli.foreground_turn_context import turn_context
from ahc.cli.runtime_environment import ENVIRONMENT_INSTRUCTIONS
from ahc.cli.runtime_skill_discovery import MAX_DISCOVERY_BYTES, skill_discovery_snapshot
from ahc.domain.sessions import ContractError


def bound_tools(repository, generation=1):
    peer, client, request = setup()
    tools = tools_for(repository, generation)
    request = replace(request, session=tools.workspace.identity.session,
                      capabilities=tools.capabilities())
    return tools, request, peer, client


def test_semantic_catalog_reaches_wire_without_bodies_or_forced_tool_selection(repository,
                                                                             monkeypatch):
    item = revision(trigger="unmentioned-keyword")
    _activate_with_report(repository, item, "activate")
    tools, request, peer, client = bound_tools(repository)
    request = replace(request, input_text="請找出這件事為何會發生")
    store = object()
    tools.workspace.store = store
    config = SimpleNamespace(session=request.session, to_dict=lambda: {})
    monkeypatch.setattr("ahc.cli.foreground_turn_context.read_context", lambda *a: None)
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0,
                                workspace_tools=tools)
    request = replace(request, configuration_digest=client.configuration_digest,
                      workspace_scope=tools.workspace.identity.workspace_id)
    client.turn_context = lambda bound: turn_context(store, config, tools, bound)
    assert client.run_admitted(request).disposition == "completed"
    wire = next(m["params"]["input"] for m in peer.sent if m.get("method") == "turn/start")
    context, _ = json.JSONDecoder().raw_decode(wire[0]["text"].split("\n", 1)[1])
    snapshot = context["skill_discovery"]
    assert snapshot["status"] == "available" and snapshot["next_offset"] is None
    assert snapshot["skills"] == [{"skill_name": item.skill_name,
        "revision_digest": item.revision_digest, "description": item.description}]
    assert item.body not in str(wire) and wire[1]["text"] == request.input_text
    read = call(tools, "skills_read", revision_digest=item.revision_digest)
    assert read["result"]["text"] == item.body
    assert "next_offset" in ENVIRONMENT_INSTRUCTIONS and len(ENVIRONMENT_INSTRUCTIONS) <= 4096


def test_index_uses_same_agent_tool_and_generation_guards_as_full_reads(repository):
    old = revision(name="allowed")
    foreign = replace(revision(name="foreign"), agent_allowlist=("other",), revision_digest=None)
    missing = revision(name="missing", required_tools=("workspace.secret",))
    for item in (old, foreign, missing):
        _activate_with_report(repository, item, "activate-" + item.skill_name)
    tools, request, _, _ = bound_tools(repository)
    initial = skill_discovery_snapshot(tools, request)
    assert [row["skill_name"] for row in initial["skills"]] == [old.skill_name]
    new = revision(name="allowed", version="1.1.0", predecessor=old.revision_digest)
    _activate_with_report(repository, new, "replace")
    reopened, again, _, _ = bound_tools(repository)
    assert skill_discovery_snapshot(reopened, again) == initial
    successor, later, _, _ = bound_tools(repository, 2)
    later_index = skill_discovery_snapshot(successor, later)
    assert later_index["skills"][0]["revision_digest"] == new.revision_digest
    tools.workspace.closed = True
    assert skill_discovery_snapshot(tools, request)["status"] == "disabled"
    with pytest.raises(ContractError, match="owner mismatch"):
        skill_discovery_snapshot(successor, request)


def test_large_unicode_catalog_is_bounded_and_can_be_paged_without_body_disclosure(repository):
    for index in range(19):
        item = replace(revision(name=f"skill-{index:02d}"), description="長" * 256,
                       revision_digest=None)
        _activate_with_report(repository, item, f"activate-{index}")
    tools, request, _, _ = bound_tools(repository)
    snapshot = skill_discovery_snapshot(tools, request)
    assert snapshot["status"] == "partial" and snapshot["total"] == 19
    assert len(json.dumps(snapshot, ensure_ascii=False).encode("utf-8")) <= MAX_DISCOVERY_BYTES
    remaining = call(tools, offset=snapshot["next_offset"])["result"]
    names = [row["skill_name"] for row in snapshot["skills"] + remaining["skills"]]
    assert names == [f"skill-{i:02d}" for i in range(19)]


def test_unavailable_is_sanitized_and_tool_free_turn_does_not_open_repository(repository):
    tools, request, _, _ = bound_tools(repository)
    def unavailable(*a, **k):
        raise OSError("PRIVATE-SENTINEL")
    tools.invoke = unavailable
    snapshot = skill_discovery_snapshot(tools, request)
    assert snapshot["status"] == "unavailable" and "PRIVATE" not in str(snapshot)
    disabled = skill_discovery_snapshot(tools, replace(request, capabilities=()))
    assert disabled["status"] == "disabled"
    assert skill_discovery_snapshot(None, request)["status"] == "disabled"


@pytest.mark.parametrize("declared,available,enabled,lazy", [
    (("workspace.skills_list", "workspace.skills_read"),
     ("workspace.skills_list", "workspace.skills_read"), True, True),
    (("workspace.skills_list", "workspace.skills_read"),
     ("workspace.skills_list", "workspace.skills_read"), False, False),
    (("workspace.skills_list", "workspace.skills_read"), (), True, False),
    ((), ("workspace.skills_list", "workspace.skills_read"), True, False),
])
def test_lazy_disclosure_keeps_selected_revision_and_legacy_compatibility(
    declared, available, enabled, lazy,
):
    from ahc.application.runtime_service import RuntimeService
    _, _, request = setup()
    item = revision()
    selected = SimpleNamespace(entries=(SimpleNamespace(revision_digest=item.revision_digest),))
    selector = SimpleNamespace(select_and_pin=lambda *a, **k: {
        "manifest": selected, "disclosure": {item.revision_digest: item.body}})
    service = RuntimeService(None, None, selector=selector)
    service.execute = lambda expected, bound: bound
    result = service.execute_with_skills(SimpleNamespace(goal=object()),
        replace(request, capabilities=declared), tools=available, lazy_disclosure=enabled)
    assert result.revision_digests == (item.revision_digest,)
    assert result.skill_bodies == (() if lazy else ((item.revision_digest, item.body),))
