"""Regressions for the first formal ASI blocker batch."""

from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from dataclasses import replace

import pytest
from skill_test_support import goal, request, revision, session

from ahc.application.skill_service import SkillTurnService
from ahc.cli.skills import _request
from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SelectionRequest, SkillRelationship
from ahc.domain.skill_selection import select_revisions, trigger_evidence
from ahc.domain.turns import FAKE_TURN_REQUEST_SCHEMA_VERSION, FakeTurnRequest
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import (
    CompareAndSetError,
    OperationConflictError,
    SkillRepository,
)


def _register_accepted(repository: SkillRepository, item=None):
    candidate = revision() if item is None else item
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    repository.save_evaluation(report)
    return candidate, report


def _activate(repository: SkillRepository, operation_key: str = "activate-test"):
    candidate, report = _register_accepted(repository)
    receipt = repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=None,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        operation_key=operation_key,
    )
    return candidate, report, receipt


def test_trigger_matching_uses_token_sequences_without_ascii_substrings() -> None:
    assert trigger_evidence("analyze", "please reanalyze this") == ()
    assert trigger_evidence("analyze data", "please analyze data now") == ("analyze data",)
    assert trigger_evidence("分析", "請分析這份資料") == ("分析",)


def test_conflicts_are_symmetric_in_selection_and_evaluation() -> None:
    beta = revision(name="beta-skill", roles=("beta",))
    alpha = revision(
        name="alpha-skill",
        roles=("alpha",),
        relationships=(SkillRelationship("conflicts_with", beta.revision_digest or ""),),
    )
    current = SelectionRequest(
        session(1, "session-conflict"),
        "turn-conflict",
        ("alpha", "beta"),
        "agent-test",
        (),
        "analyze task",
        goal().digest,
    )
    selected = select_revisions(current, (alpha, beta))
    digests = {entry.revision_digest for entry in selected.manifest.entries}
    assert not {alpha.revision_digest, beta.revision_digest}.issubset(digests)
    report = evaluate_revision(
        beta,
        active_revisions=(alpha,),
        materialized_revisions=(alpha, beta),
    )
    assert (
        next(check for check in report.checks if check.check_id == "conflict-safety").passed
        is False
    )


def test_request_json_agent_is_bound_to_session_and_domain_rejects_mismatch() -> None:
    fake = FakeTurnRequest(
        FAKE_TURN_REQUEST_SCHEMA_VERSION,
        session(1, "session-json"),
        "turn-json",
        "analyze task",
    )
    args = Namespace(
        request_json=json.dumps(fake.to_dict()),
        generation=1,
        virtual_session_id=None,
        predecessor_json=None,
        agent_id=None,
        route_id=None,
        turn_id=None,
        input_text=None,
        roles=None,
        available_tools=[],
        task_text=None,
    )
    parsed = _request(args)
    assert parsed.agent_id == parsed.session.agent_id
    with pytest.raises(ContractError, match="session agent"):
        SelectionRequest(
            parsed.session, parsed.turn_id, parsed.roles, "other-agent", (), parsed.task_text
        )


def test_manifest_pin_rejects_same_ref_with_different_agent_and_route(
    repository: SkillRepository,
) -> None:
    _activate(repository)
    service = SkillTurnService(repository)
    current_goal = goal()
    original = request(1, current_goal.digest or "")
    service.select_and_pin(original, episode_id="episode-identity-original", goal=current_goal)
    original_episode = repository.get_episode("episode-identity-original")
    assert original_episode is not None
    foreign_session = replace(original.session, agent_id="agent-other", route_id="route-other")
    foreign = replace(
        original,
        session=foreign_session,
        agent_id="agent-other",
        turn_id="turn-identity-collision",
    )
    with pytest.raises(ContractError, match="identity"):
        service.select_and_pin(foreign, episode_id="episode-identity-foreign", goal=current_goal)
    assert repository.get_episode("episode-identity-foreign") is None
    forged_episode = replace(
        original_episode, episode_id="episode-identity-direct", session=foreign_session
    )
    with pytest.raises(ContractError, match="pinned manifest"):
        repository.save_episode(forged_episode)
    with pytest.raises(ContractError, match="immutable"):
        repository.update_episode(replace(original_episode, session=foreign_session))


def test_evaluator_checks_dependency_direction_against_inactive_materialized_graph(
    repository: SkillRepository,
) -> None:
    inactive_how = revision(name="inactive-how", trigger="inspect", layer="how-procedure")
    repository.register_revision(inactive_how)
    candidate = revision(
        name="what-candidate",
        trigger="choose",
        layer="what-strategy",
        roles=("strategy",),
        relationships=(SkillRelationship("depends_on", inactive_how.revision_digest or ""),),
    )
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "block"
    direction = next(check for check in report.checks if check.check_id == "relationship-direction")
    assert direction.passed is False


def test_failed_dependency_closure_does_not_leak_partial_selection() -> None:
    first = revision(name="dependency-first", roles=("dependency-first",))
    second = revision(
        name="dependency-second",
        roles=("dependency-second",),
        relationships=(SkillRelationship("conflicts_with", first.revision_digest or ""),),
    )
    parent = revision(
        name="parent-skill",
        roles=("parent",),
        relationships=(
            SkillRelationship("depends_on", first.revision_digest or ""),
            SkillRelationship("depends_on", second.revision_digest or ""),
        ),
    )
    current = SelectionRequest(
        session(1, "session-dependency-closure"),
        "turn-dependency-closure",
        ("parent",),
        "agent-test",
        (),
        "analyze task",
        goal().digest,
    )
    result = select_revisions(current, (first, second, parent))
    assert result.manifest.entries == ()
    assert result.selected == ()
    assert result.abstained_roles == ("parent",)


def test_activation_rereads_materialized_candidate_before_pointer_mutation(
    repository: SkillRepository,
) -> None:
    candidate, report = _register_accepted(repository)
    body = next(repository.root.glob("revisions/*/*/SKILL.md"))
    body.write_text("tampered\n", encoding="utf-8")
    with pytest.raises((CompareAndSetError, ContractError)):
        repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest or "",
            expected_active_digest=None,
            evaluation_digest=report.evaluation_digest or "",
            input_digest=report.input_digest,
            operation_key="activate-tampered",
        )
    assert repository.get_active(candidate.skill_name) is None


def test_activation_rejects_drifted_evaluation_report_bytes(
    repository: SkillRepository,
) -> None:
    candidate, report = _register_accepted(repository)
    with sqlite3.connect(repository.db_path) as connection:
        stored = json.loads(
            connection.execute(
                "SELECT report_json FROM evaluations WHERE evaluation_digest = ?",
                (report.evaluation_digest,),
            ).fetchone()[0]
        )
        stored["evaluator_id"] = "forged-evaluator"
        connection.execute(
            "UPDATE evaluations SET report_json = ? WHERE evaluation_digest = ?",
            (json.dumps(stored, sort_keys=True), report.evaluation_digest),
        )
    with pytest.raises(CompareAndSetError, match="drifted"):
        repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest or "",
            expected_active_digest=None,
            evaluation_digest=report.evaluation_digest or "",
            input_digest=report.input_digest,
            operation_key="activate-forged-report",
        )
    assert repository.get_active(candidate.skill_name) is None


def test_operation_key_binds_kind_skill_and_complete_activation_inputs(
    repository: SkillRepository,
) -> None:
    candidate, report, _ = _activate(repository, "shared-operation")
    with pytest.raises(OperationConflictError):
        repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest or "",
            expected_active_digest=None,
            evaluation_digest=report.evaluation_digest or "",
            input_digest="0" * 64,
            operation_key="shared-operation",
        )
    with pytest.raises(OperationConflictError):
        repository.rollback(
            skill_name=candidate.skill_name,
            expected_active_digest=candidate.revision_digest or "",
            operation_key="shared-operation",
        )


def test_event_key_and_metadata_are_bound_to_episode_manifest_and_revision(
    repository: SkillRepository,
) -> None:
    candidate, _, _ = _activate(repository)
    service = SkillTurnService(repository)
    current_goal = goal()
    first = service.select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-event-1", goal=current_goal
    )
    second_request = request(2, current_goal.digest or "")
    second = service.select_and_pin(second_request, episode_id="episode-event-2", goal=current_goal)
    entry = first["manifest"].entries[0]
    event_key = f"episode-event-1:surfaced:{entry.revision_digest}:{entry.role}"
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute(
            "UPDATE events SET manifest_digest = ? WHERE event_key = ?",
            (second["manifest"].manifest_digest, event_key),
        )
    with pytest.raises(ContractError, match="different evidence"):
        repository.append_event(
            event_key=event_key,
            episode_id="episode-event-1",
            event_kind="surfaced",
            revision_digest=candidate.revision_digest,
            manifest_digest=first["manifest"].manifest_digest,
            payload={"role": entry.role, "revision_digest": candidate.revision_digest},
        )


def test_belief_update_uses_exact_stored_episode_context(repository: SkillRepository) -> None:
    candidate, _, _ = _activate(repository)
    current_goal = goal()
    episode = SkillTurnService(repository).select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-belief", goal=current_goal
    )["episode"]
    terminal = replace(
        episode,
        used_revisions=(candidate.revision_digest or "",),
        outcome="verified-success",
        evidence_refs=("evidence-success",),
    )
    repository.update_episode(terminal)
    forged_session = replace(terminal.session, agent_id="forged-agent")
    forged = replace(terminal, session=forged_session)
    with pytest.raises(ContractError, match="stored episode"):
        repository.apply_belief_update(forged, outcome="verified-success")
    assert repository.beliefs() == ()
