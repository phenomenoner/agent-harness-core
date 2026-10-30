"""Skill trial uptake across generations; no model inference or OS protection claim."""

import json

import pytest
from skill_test_support import session
from test_skill_experiments import candidate
from test_skill_graph_convergence import _activate_with_report
from test_workspace_learning import invoke, request, setup
from test_workspace_skills import call, tools_for

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.domain.sessions import ContractError
from ahc.infrastructure.skill_repository import SkillRepository


def propose(tools, source, name="analyze-skill"):
    result = invoke(tools, "propose", {
        "skill_name": name, "body": "# Procedure\nUse the actual supplied glossary.",
        "description": "Honor supplied terminology", "source_ref": source,
        "cause": "user correction", "hypothesis": "This may improve terminology consistency",
    })
    assert result["disposition"] == "observed"
    return result["result"]


@pytest.mark.parametrize("effect", ["promote", "rollback"])
def test_lost_effect_reply_reopen_preserves_newer_revision_and_receipts(
    repository, monkeypatch, effect
):
    from dataclasses import replace

    old = candidate()
    _activate_with_report(repository, old, "baseline-before-lost-reply")
    first = setup(repository)
    source = first.learning.record_input(request(session(1)))
    trial = propose(first, source)
    service = first.learning.experiments
    service.configure(enabled=True)  # Controlled fixture, never foreground activation.
    target = "activate" if effect == "promote" else "rollback"
    if effect == "rollback":
        service.promote(trial["trial_id"])
    action = getattr(repository, target)

    def lost(**arguments):
        action(**arguments)
        raise OSError("fixture lost committed effect reply")

    monkeypatch.setattr(repository, target, lost)
    outcome = invoke(first, effect, {"trial_id": trial["trial_id"]})
    assert outcome["disposition"] == "unknown" and outcome["unresolved_effects"]
    reopened = SkillRepository(repository.root)
    actual = reopened.get_active(old.skill_name)
    assert actual.revision_digest == (trial["candidate"] if effect == "promote"
                                     else old.revision_digest)
    operation_key = ("trial:" if effect == "promote" else "trial-rollback:") + trial["trial_id"]
    with reopened._read() as db:
        evidence = db.execute("SELECT receipt_json FROM receipts WHERE operation_key=?",
                              (operation_key,)).fetchone()[0]
    newer = replace(actual, version="1.0.2", predecessor_digest=actual.revision_digest,
                    body="# Later revision\nPreserve the newer authorized work.",
                    body_digest=None, revision_digest=None)
    _activate_with_report(reopened, newer, "later-authorized-revision")
    recovered = type(service)(reopened, owner=session(2).agent_id,
        granted_tools=("workspace.execute",), protected_names=("pmo-policy",),
        protection_check=lambda: True)
    recovered.configure(enabled=False)
    later = recovered.record_experience(source_ref="later-owned-evidence", kind="task-result",
        source_digest="a" * 64, receipt={"retained": True})
    # Read durable exact-operation evidence before reconciliation. Neither branch
    # grants a second activation/rollback or restores the historical active pointer.
    if effect == "promote":
        assert recovered.promote(trial["trial_id"])["activation"] == json.loads(evidence)
        with pytest.raises(ContractError):
            recovered.rollback(trial["trial_id"])
    else:
        assert recovered.rollback(trial["trial_id"])["rollback"] == json.loads(evidence)
        assert recovered.get(trial["trial_id"])["state"] == "rolled-back"
    assert reopened.get_active(old.skill_name) == newer
    assert reopened.get_revision(old.revision_digest) == old
    with reopened._read() as db:
        assert db.execute("SELECT receipt_json FROM receipts WHERE operation_key=?",
                          (operation_key,)).fetchone()[0] == evidence
        assert db.execute("SELECT count(*) FROM learning_experiences WHERE ref=?",
                          (later,)).fetchone()[0] == 1
        assert db.execute("SELECT enabled FROM learning_control").fetchone()[0] == 0


def test_reopen_discovers_trial_then_successor_reads_it_and_rollback_preserves_pins(repository):
    old = candidate()
    _activate_with_report(repository, old, "baseline")
    first = setup(repository)
    assert call(first)["result"]["skills"][0]["revision_digest"] == old.revision_digest
    source = first.learning.record_input(request(session(1)))
    trial = propose(first, source)
    first.learning.experiments.configure(enabled=True)  # Fixture protection, not live admission.
    assert invoke(first, "promote", {"trial_id": trial["trial_id"]})["disposition"] == "observed"
    assert call(first, "skills_read", revision_digest=trial["candidate"])[
        "disposition"] == "rejected"

    reopened = SkillRepository(repository.root)
    second = tools_for(reopened, generation=2)
    second.learning = WorkspaceLearningTools(type(first.learning.experiments)(
        reopened, owner=session(2).agent_id, granted_tools=("workspace.execute",),
        protected_names=("pmo-policy",), protection_check=lambda: True), session(2))
    status = invoke(second, "status")["result"]
    assert status["trials"][0]["trial_id"] == trial["trial_id"]
    assert status["trials"][0]["state"] == "active"
    assert call(second)["result"]["skills"][0]["revision_digest"] == trial["candidate"]
    actual = call(second, "skills_read", revision_digest=trial["candidate"])
    assert "glossary" in actual["result"]["text"]
    reference = second.learning.record_tool(request(session(2), "subsequent-task"),
                                            "read", "workspace.skills_read", actual)
    facts = invoke(second, "status")["result"]["experiences"][0]["facts"]
    assert facts["skill_read"]["revision_digest"] == trial["candidate"]
    assert facts["delivery_or_material_use_verified"] is False
    assessment = invoke(second, "observe", {"trial_id": trial["trial_id"],
        "source_ref": reference, "judgment": "Read revised instructions; usefulness untested"})
    assert assessment["result"]["benefit"] == "model-assessment"
    second.learning.experiments.configure(enabled=False)
    assert invoke(second, "rollback", {"trial_id": trial["trial_id"]})["disposition"] == "observed"
    assert call(second)["result"]["skills"][0]["revision_digest"] == trial["candidate"]
    assert call(tools_for(reopened, generation=3))["result"]["skills"][0][
        "revision_digest"] == old.revision_digest
    assert invoke(second, "trials")["result"]["trials"][0]["state"] == "rolled-back"


def test_trial_cursor_survives_new_insert_and_excludes_other_conversation(repository):
    tools = setup(repository)
    tools.learning.experiments.configure(enabled=False, daily_limit=100)
    source = tools.learning.record_input(request(session(1)))
    expected = []
    for index in range(18):
        trial = propose(tools, source, name=f"trial-{index}")
        expected.append(trial["trial_id"])
        invoke(tools, "reject", {"trial_id": trial["trial_id"]})
    first = invoke(tools, "trials", {"limit": 7})["result"]
    new = propose(tools, source, name="new-trial")
    rows = first["trials"]
    cursor = first["next_before"]
    for _ in range(3):
        if cursor is None:
            break
        page = invoke(tools, "trials", {"before": cursor, "limit": 7})["result"]
        rows.extend(page["trials"])
        cursor = page["next_before"]
    assert cursor is None
    assert [row["trial_id"] for row in rows] == list(reversed(expected))
    assert new["trial_id"] not in [row["trial_id"] for row in rows]
    other = tools_for(repository)
    other.workspace.identity = type(other.workspace.identity)(session(1, "foreign"), "work")
    other.learning = WorkspaceLearningTools(tools.learning.experiments, session(1, "foreign"))
    assert invoke(other, "trials")["result"] == {"trials": [], "next_before": None}


@pytest.mark.parametrize("payload", [{"limit": True}, {"limit": 17}, {"before": 0},
                                    {"before": "1"}, {"before": 2**63}, {"owner": "foreign"}])
def test_invalid_trial_queries_do_not_disclose(repository, payload):
    assert invoke(setup(repository), "trials", payload)["disposition"] == "rejected"


def test_prepared_revision_facts_are_bounded_and_do_not_claim_delivery(repository):
    tools = setup(repository)
    for index in range(4):
        task = request(session(1), f"turn-{index}")
        task.revision_digests = tuple(f"{n:064x}" for n in range(64))
        task.skill_bodies = tuple((digest, "body") for digest in task.revision_digests)
        tools.learning.record_input(task)
    result = invoke(tools, "status")
    assert len(json.dumps(result).encode()) < 32768
    assert result["result"]["experiences_truncated"] is True
    facts = result["result"]["experiences"][0]["facts"]
    assert facts["selected_revisions"] == list(task.revision_digests)
    assert facts["prepared_body_revisions"] == list(task.revision_digests)
    assert facts["delivery_or_material_use_verified"] is False


def test_trial_discovery_is_on_wire_and_closed_direct_wrapper_rejects(repository):
    tools = setup(repository)
    task = request(session(1))
    task.capabilities = tools.capabilities()
    declaration = next(item for item in CodexWorkspaceTools.describe(task, tools)[0]["tools"]
                       if item["name"] == "learning")
    assert "trials" in declaration["inputSchema"]["properties"]["action"]["enum"]
    expected = tools.workspace.identity.to_dict()
    tools.workspace.closed = True
    assert tools.learning.invoke(tools.workspace, {
        "expected": expected, "action": "trials", "payload": {}})["disposition"] == "rejected"


@pytest.mark.parametrize("value", [None, []])
def test_malformed_read_result_does_not_break_task_or_invent_revision_evidence(repository, value):
    tools = setup(repository)
    ref = tools.learning.record_tool(request(session(1)), "read", "workspace.skills_read",
        {"disposition": "observed", "unresolved_effects": False, "result": value})
    receipt = invoke(tools, "status")["result"]["experiences"][0]
    assert receipt["ref"] == ref
    assert "skill_read" not in receipt["facts"]
