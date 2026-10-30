"""Foreground receipt ownership and natural proposal tool composition."""

from types import SimpleNamespace

import pytest
from skill_test_support import session
from test_skill_experiments import learning
from test_workspace_skills import tools_for

from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.cli.foreground_learning import compose_learning, parse_learning
from ahc.domain.sessions import ContractError


def request(owner, operation="turn", text="Please use the terminology from my document"):
    return SimpleNamespace(session=owner, operation_id=operation, input_text=text)


def invoke(tools, action, payload=None):
    return tools.invoke(
        "workspace.learning",
        {
            "expected": tools.workspace.identity.to_dict(),
            "action": action,
            "payload": payload or {},
        },
    )


def setup(repository):
    tools = tools_for(repository)
    tools.learning = WorkspaceLearningTools(learning(repository), session(1))
    return tools


def test_user_input_tool_outcome_and_proposal_are_connected_without_fabricated_eval(repository):
    tools = setup(repository)
    source = tools.learning.record_input(request(session(1)))
    tools.learning.record_tool(
        request(session(1)),
        "call",
        "workspace.execute",
        {
            "disposition": "observed",
            "unresolved_effects": False,
            "result": {"ok": False, "output": "private data is not copied"},
        },
    )
    status = invoke(tools, "status")["result"]
    assert len(status["experiences"]) == 2
    assert "output" not in status["experiences"][0]["facts"]
    result = invoke(
        tools,
        "propose",
        {
            "skill_name": "terminology",
            "body": "# Procedure\nUse the supplied glossary.",
            "description": "Document terminology",
            "source_ref": source,
            "cause": "user clarification",
            "hypothesis": "Reduce terminology inconsistencies",
        },
    )
    assert result["disposition"] == "observed"
    assert result["result"]["benefit"] == "unmeasured"
    assert repository.get_active("terminology") is None
    trial = result["result"]["trial_id"]
    assert invoke(tools, "promote", {"trial_id": trial})["disposition"] == "rejected"


def test_same_agent_other_conversation_has_no_source_or_trial_access(repository):
    tools = setup(repository)
    source = tools.learning.record_input(request(session(1)))
    result = invoke(
        tools,
        "propose",
        {
            "skill_name": "local-skill",
            "body": "# Local procedure",
            "description": "Local",
            "source_ref": source,
            "cause": "clarification",
            "hypothesis": "useful",
        },
    )["result"]
    other = tools_for(repository)
    other.workspace.identity = type(other.workspace.identity)(
        session(1, "other-conversation"), "work"
    )
    other.learning = WorkspaceLearningTools(learning(repository), other.workspace.identity.session)
    assert invoke(other, "status")["result"]["experiences"] == []
    assert invoke(other, "get", {"trial_id": result["trial_id"]})["disposition"] == "rejected"
    assert (
        invoke(
            other,
            "observe",
            {"trial_id": result["trial_id"], "source_ref": source, "judgment": "fake success"},
        )["disposition"]
        == "rejected"
    )


def test_next_generation_retains_same_conversation_receipts(repository):
    first = setup(repository)
    source = first.learning.record_input(request(session(1)))
    second = tools_for(repository, generation=2)
    second.learning = WorkspaceLearningTools(learning(repository), session(2))
    assert invoke(second, "status")["result"]["experiences"][0]["ref"] == source


def test_failed_evidence_write_exposes_gap_without_replaying_tool(repository, monkeypatch):
    tools = setup(repository)

    def unavailable(**_):
        raise OSError("writer unavailable")

    monkeypatch.setattr(tools.learning.experiments, "record_experience", unavailable)
    assert tools.learning.record_input(request(session(1))) is None
    assert invoke(tools, "status")["result"]["capture_gap"] is True


def test_proposal_mode_cannot_claim_trusted_foreground_is_protected(repository):
    raw = {
        "skill_root": str(repository.root),
        "learning": {"mode": "proposal-only", "protected_skills": ["pmo-policy"]},
    }
    document = parse_learning(raw)
    config = SimpleNamespace(session=session(1), to_dict=lambda: document)
    composed = compose_learning(config, SimpleNamespace(repository=repository))
    assert composed.experiments.protection_check() is False
    raw["learning"]["mode"] = "automatic"
    with pytest.raises(ContractError):
        parse_learning(raw)
