"""Real repository discovery and dynamic-tool checks, without provider inference."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from skill_test_support import goal, request, revision, session
from test_channel_tool_approval import Port, admitted
from test_skill_graph_convergence import _activate_with_report

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.channel_tool_approval import ChannelToolGate
from ahc.application.skill_service import SkillTurnService
from ahc.application.workspace_skills import WorkspaceSkillTools
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.workspace import WorkspaceIdentity


def tools_for(repository, generation=1):
    workspace = SimpleNamespace(
        identity=WorkspaceIdentity(session(generation), "work"), closed=False
    )
    return WorkspaceTools(workspace, skills=WorkspaceSkillTools(repository))


def call(tools, name="skills_list", **arguments):
    return tools.invoke(
        "workspace." + name, {"expected": tools.workspace.identity.to_dict(), **arguments}
    )


def test_discovery_does_not_require_trigger_or_imply_material_use(repository):
    item = revision()
    _activate_with_report(repository, item, "activate")
    selection = SkillTurnService(repository).select_and_pin(
        request(1, goal().digest, "請協助判斷問題的原因"), episode_id="natural-task", goal=goal()
    )
    assert selection["manifest"].entries == ()
    tools = tools_for(repository)
    result = call(tools, query="完全不同的措辭")
    assert result["result"]["skills"][0]["revision_digest"] == item.revision_digest
    assert result["result"]["query_is_filter"] is False
    read = call(tools, "skills_read", revision_digest=item.revision_digest)
    assert read["result"]["text"] == item.body
    assert repository.get_episode("natural-task").used_revisions == ()


def test_foreign_owner_missing_tools_and_inactive_revisions_are_not_disclosed(repository):
    public = revision(name="public")
    foreign = replace(
        revision(name="foreign"), agent_allowlist=("other-agent",), revision_digest=None
    )
    missing = revision(name="missing", required_tools=("workspace.secret",))
    inactive = revision(name="inactive")
    for item in (public, foreign, missing):
        _activate_with_report(repository, item, "activate-" + item.skill_name)
    repository.register_revision(inactive)
    tools = tools_for(repository)
    assert [row["skill_name"] for row in call(tools)["result"]["skills"]] == ["public"]
    for item in (foreign, missing, inactive):
        assert (
            call(tools, "skills_read", revision_digest=item.revision_digest)["disposition"]
            == "rejected"
        )


def test_pin_survives_reopen_and_successor_adopts_new_revision(repository):
    old = revision()
    _activate_with_report(repository, old, "old")
    first = tools_for(repository)
    assert call(first)["result"]["skills"][0]["revision_digest"] == old.revision_digest
    new = revision(version="1.1.0", predecessor=old.revision_digest)
    _activate_with_report(repository, new, "new")
    reopened = tools_for(repository)
    assert call(reopened)["result"]["skills"][0]["revision_digest"] == old.revision_digest
    assert (
        call(reopened, "skills_read", revision_digest=new.revision_digest)["disposition"]
        == "rejected"
    )
    assert (
        call(tools_for(repository, 2))["result"]["skills"][0]["revision_digest"]
        == new.revision_digest
    )


def test_catalog_and_body_pagination_are_complete_and_wire_bounded(repository):
    for index in range(3):
        item = revision(name=f"skill-{index}")
        _activate_with_report(repository, item, f"activate-{index}")
    tools = tools_for(repository)
    rows = []
    offset = 0
    while offset is not None:
        result = call(tools, limit=1, offset=offset)["result"]
        rows.extend(result["skills"])
        offset = result["next_offset"]
    assert [item["skill_name"] for item in rows] == [f"skill-{i}" for i in range(3)]
    text, offset = "", 0
    while offset is not None:
        result = call(
            tools,
            "skills_read",
            revision_digest=rows[0]["revision_digest"],
            limit=23,
            offset=offset,
        )
        assert len(json.dumps(result).encode()) < 32768
        text += result["result"]["text"]
        offset = result["result"]["next_offset"]
    assert text == repository.get_revision(rows[0]["revision_digest"]).body


def test_astral_unicode_body_stays_within_dynamic_tool_wire_limit(repository):
    item = revision()
    item = replace(
        item,
        body=item.body.replace("Bounded analysis.", "😀" * 9000),
        body_digest=None,
        revision_digest=None,
    )
    _activate_with_report(repository, item, "unicode")
    tools = tools_for(repository)
    text, offset = "", 0
    while offset is not None:
        result = call(tools, "skills_read", revision_digest=item.revision_digest, offset=offset)
        assert len(json.dumps(result).encode("utf-8")) < 32768
        text += result["result"]["text"]
        offset = result["result"]["next_offset"]
    assert text == item.body


@pytest.mark.parametrize(
    "arguments", [{"offset": True}, {"limit": 0}, {"limit": 33}, {"query": []}, {"root": "foreign"}]
)
def test_bad_discovery_arguments_reject_without_effect(repository, arguments):
    result = call(tools_for(repository), **arguments)
    assert result["disposition"] == "rejected"
    assert result["unresolved_effects"] is False


def test_stale_workspace_cannot_disclose(repository):
    tools = tools_for(repository)
    expected = tools.workspace.identity.to_dict()
    expected["revision"] += 1
    assert (
        tools.invoke("workspace.skills_list", {"expected": expected})["disposition"] == "rejected"
    )


def test_dynamic_declarations_and_read_only_gate(repository):
    tools = tools_for(repository)
    request = SimpleNamespace(capabilities=tools.capabilities())
    declarations = CodexWorkspaceTools.describe(request, tools)[0]["tools"]
    listed = next(item for item in declarations if item["name"] == "skills_list")
    read = next(item for item in declarations if item["name"] == "skills_read")
    assert listed["inputSchema"]["required"] == ["expected"]
    assert read["inputSchema"]["required"] == ["expected", "revision_digest"]
    for name in ("workspace.skills_list", "workspace.skills_read"):
        port = Port()
        gate = ChannelToolGate(admitted(capabilities=(name,)), port, clock=lambda: 10)
        assert gate.authorize(name, {}, "thread", "turn", "call")
        assert port.requests == []


def test_discovery_not_advertised_without_composition(repository):
    tools = tools_for(repository)
    tools.skills = None
    assert "workspace.skills_list" not in tools.capabilities()
    assert call(tools)["disposition"] == "unsupported"
