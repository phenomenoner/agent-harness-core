"""Discriminating active-loop, pinning, belief, evaluator, and rollback tests."""

from __future__ import annotations

import json

import pytest
from skill_test_support import goal, request, revision, session

from ahc.application.skill_learning import learn_episode
from ahc.application.skill_service import SkillTurnService
from ahc.domain import OutcomeAttribution
from ahc.domain.judgment_records import ATTRIBUTION_SCHEMA
from ahc.domain.sessions import ContractError
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.domain.skill_models import SelectionRequest
from ahc.domain.skill_selection import select_revisions
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.skill_doctor import doctor, inventory
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import CompareAndSetError, SkillRepository


def _lesson() -> dict[str, object]:
    return {
        "skill_name": "analyze-skill",
        "description": "Bounded analysis procedure",
        "cognitive_layer": "how-procedure",
        "roles": ["procedure"],
        "agent_allowlist": [],
        "required_tools": [],
        "triggers": ["analyze"],
        "expected_effects": ["bounded report"],
        "positive_examples": ["analyze task"],
        "negative_examples": ["summarize task"],
        "relationships": [],
        "body": revision().body,
    }


def test_direct_active_loop_pin_belief_replacement_and_exact_rollback(
    repository: SkillRepository,
) -> None:
    current_goal = goal()
    service = SkillTurnService(repository)
    first = service.select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-1", goal=current_goal
    )
    assert first["manifest"].entries == ()
    attribution = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "episode-1",
        "wrong-how",
        ("how-procedure",),
        ("evidence-lesson",),
        "verifier-test",
    )
    learned = learn_episode(
        repository,
        episode_id="episode-1",
        outcome="verified-failure",
        evidence_refs=("evidence-lesson",),
        attribution=attribution,
        lesson=_lesson(),
        reflect=True,
    )
    evidence = HeldOutTaskEvidence(
        learned["candidate"].revision_digest,
        digest_json({"task": "held-out"}),
        digest_json({"answer": 42}),
        digest_json({"answer": 42}),
        ("held-out-receipt",),
    )
    learned = learn_episode(
        repository,
        episode_id="episode-1",
        outcome="verified-failure",
        evidence_refs=("evidence-lesson",),
        attribution=attribution,
        lesson=_lesson(),
        evaluate=True,
        activate=True,
        held_out_evidence=(evidence,),
        operation_key="activate-1",
    )
    active_v1 = repository.get_active("analyze-skill")
    assert learned["evaluation"].verdict == "accept"
    assert active_v1 is not None

    pinned_empty = service.select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-1", goal=current_goal
    )
    assert pinned_empty["manifest"].manifest_digest == first["manifest"].manifest_digest
    assert pinned_empty["manifest"].entries == ()

    second = service.select_and_pin(
        request(2, current_goal.digest or ""), episode_id="episode-2", goal=current_goal
    )
    assert tuple(entry.revision_digest for entry in second["manifest"].entries) == (
        active_v1.revision_digest,
    )
    service.mark_used("episode-2", (active_v1.revision_digest or "",))
    learn_episode(
        repository,
        episode_id="episode-2",
        outcome="verified-success",
        evidence_refs=("evidence-success",),
    )
    learn_episode(
        repository,
        episode_id="episode-2",
        outcome="verified-success",
        evidence_refs=("evidence-success",),
    )
    belief = repository.beliefs()[0]
    assert (belief["successes"], belief["failures"]) == (1, 0)

    candidate = revision(version="1.0.1", predecessor=active_v1.revision_digest)
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    repository.save_evaluation(report)
    repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=active_v1.revision_digest,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        operation_key="activate-2",
    )
    assert repository.get_active(candidate.skill_name) == candidate

    rollback = repository.rollback(
        skill_name=candidate.skill_name,
        expected_active_digest=candidate.revision_digest or "",
        operation_key="rollback-1",
    )
    assert rollback["to_digest"] == active_v1.revision_digest
    assert repository.get_active(candidate.skill_name) == active_v1
    with pytest.raises((CompareAndSetError, ContractError)):
        repository.rollback(
            skill_name=candidate.skill_name,
            expected_active_digest=candidate.revision_digest or "",
            operation_key="rollback-2",
        )
    assert repository.get_revision(candidate.revision_digest or "") == candidate


def test_unknown_outcome_cannot_learn_or_change_beliefs(repository: SkillRepository) -> None:
    current_goal = goal()
    service = SkillTurnService(repository)
    service.select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-u", goal=current_goal
    )
    before = repository.status()["counts"]
    result = learn_episode(repository, episode_id="episode-u", outcome="unknown", evidence_refs=())
    assert result["episode"].outcome == "unknown"
    assert result["belief_updates"] == 0
    assert repository.beliefs() == ()
    with pytest.raises(ContractError, match="unknown outcome"):
        learn_episode(
            repository,
            episode_id="episode-u",
            outcome="unknown",
            evidence_refs=(),
            lesson=_lesson(),
            reflect=True,
        )
    assert repository.status()["counts"]["active_revisions"] == before["active_revisions"]


def test_evaluator_reparses_and_names_targeted_negative_check() -> None:
    valid = revision()
    tampered = valid.to_dict()
    tampered["body"] = valid.body.replace("Bounded analysis", "Changed analysis")
    report = evaluate_revision(tampered)
    assert report.verdict == "block"
    assert report.checks[0].check_id == "schema-and-digest"
    assert report.checks[0].passed is False


def test_role_agent_tool_trigger_eligibility_and_cjk_abstention() -> None:
    current_goal = goal()
    tool_skill = revision(required_tools=("reader",))
    no_tool = request(1, current_goal.digest or "")
    abstained = select_revisions(no_tool, (tool_skill,))
    assert abstained.manifest.entries == ()
    assert abstained.abstained_roles == ("procedure",)
    with_tool = SelectionRequest(
        session(1, "session-tool"),
        "turn-tool",
        ("procedure",),
        "agent-test",
        ("reader",),
        "analyze task",
        current_goal.digest,
    )
    selected = select_revisions(with_tool, (tool_skill,))
    assert len(selected.manifest.entries) == 1
    assert selected.abstained_roles == ()
    cjk = revision(name="cjk-skill", trigger="分析")
    cjk_request = SelectionRequest(
        session(1, "session-cjk"),
        "turn-cjk",
        ("procedure",),
        "agent-test",
        (),
        "請分析這份資料",
        current_goal.digest,
    )
    assert len(select_revisions(cjk_request, (cjk,)).manifest.entries) == 1


def test_doctor_is_read_only_and_detects_tampered_material(repository: SkillRepository) -> None:
    item = revision()
    repository.register_revision(item)
    before = inventory(repository.root)
    healthy = doctor(repository.root)
    assert healthy["ok"] is True
    assert inventory(repository.root) == before
    body = next(repository.root.glob("revisions/*/*/SKILL.md"))
    body.write_text("tampered\n", encoding="utf-8")
    broken_before = inventory(repository.root)
    broken = doctor(repository.root)
    assert broken["ok"] is False
    assert any(str(issue).startswith("revision:") for issue in broken["issues"])
    assert inventory(repository.root) == broken_before


def test_persisted_json_is_canonical(repository: SkillRepository) -> None:
    item = revision()
    repository.register_revision(item)
    manifest = json.loads(
        (
            repository.root
            / "revisions"
            / item.skill_name
            / (item.revision_digest or "")
            / "manifest.json"
        ).read_text()
    )
    assert manifest == item.to_dict()
