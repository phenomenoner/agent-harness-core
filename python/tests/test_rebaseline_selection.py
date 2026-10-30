"""R4 rebaseline discriminator for per-turn skill selection."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
from skill_test_support import goal, revision, session

from ahc.application.skill_service import SkillTurnService
from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SelectionRequest, SkillRevision
from ahc.infrastructure import skill_root as skill_root_module
from ahc.infrastructure.skill_db import DatabaseError
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import SkillRepository


def _activate(repository: SkillRepository, candidate: SkillRevision, operation_key: str) -> None:
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    repository.save_evaluation(report)
    repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=None,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        graph_input_digest=report.graph_input_digest,
        operation_key=operation_key,
    )


def test_multiturn_changed_tools_reselects_within_revision_availability(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(skill_root_module, "WINDOWS_WORKSPACE_BASE", tmp_path)
    repository = SkillRepository.create(tmp_path / "skills")
    current_goal = goal()
    broad = revision(name="analyze-skill", trigger="analyze")
    reader = revision(
        name="inspect-skill",
        trigger="inspect",
        required_tools=("reader",),
    )
    _activate(repository, broad, "rebaseline-selection-broad")
    _activate(repository, reader, "rebaseline-selection-reader")

    service = SkillTurnService(repository)
    shared_session = session(1, "rebaseline-selection")
    first_request = SelectionRequest(
        shared_session,
        "turn-first",
        ("procedure",),
        "agent-test",
        (),
        "analyze task",
        current_goal.digest,
    )
    first = service.select_and_pin(
        first_request,
        episode_id="rebaseline-selection-first",
        goal=current_goal,
    )
    assert [entry.skill_name for entry in first["manifest"].entries] == ["analyze-skill"]

    second_request = SelectionRequest(
        shared_session,
        "turn-second",
        ("procedure",),
        "agent-test",
        ("reader",),
        "inspect task",
        current_goal.digest,
    )
    second = service.select_and_pin(
        second_request,
        episode_id="rebaseline-selection-second",
        goal=current_goal,
    )
    assert [entry.skill_name for entry in second["manifest"].entries] == ["inspect-skill"]


def test_restart_reselects_role_goal_and_tools_without_adopting_late_revision(repository):
    base = revision(roles=("procedure", "reviewer"), required_tools=("reader",))
    _activate(repository, base, "activate-base")
    owner = session(1, "persistent-selection")
    current_goal = goal()
    first = SelectionRequest(owner, "first", ("procedure",), "agent-test",
                             ("reader",), "analyze task", current_goal.digest)
    service = SkillTurnService(repository)
    original = service.select_and_pin(first, episode_id="first", goal=current_goal)
    late = revision(name="late-skill", trigger="late")
    _activate(repository, late, "activate-late")
    # Re-open the real SQLite repository: in-memory state cannot satisfy this check.
    service = SkillTurnService(SkillRepository(repository.root))
    next_goal = replace(current_goal, goal_id="goal-next", digest=None)
    second_request = replace(first, turn_id="second", roles=("reviewer",),
                             goal_digest=next_goal.digest)
    second = service.select_and_pin(second_request, episode_id="second", goal=next_goal)
    assert second["manifest"].turn_id == "second"
    assert second["manifest"].goal_digest == next_goal.digest
    assert second["manifest"].entries[0].role == "reviewer"
    assert second["manifest"].manifest_digest != original["manifest"].manifest_digest
    without_tools = replace(first, turn_id="no-tools", available_tools=())
    assert service.select_and_pin(without_tools, episode_id="no-tools", goal=current_goal)[
        "manifest"
    ].entries == ()
    late_request = replace(first, turn_id="late", task_text="late task")
    assert service.select_and_pin(late_request, episode_id="late", goal=current_goal)[
        "manifest"
    ].entries == ()
    rebound = replace(late_request, session=owner.successor(), turn_id="rebound")
    adopted = service.select_and_pin(rebound, episode_id="rebound", goal=current_goal)
    assert adopted["manifest"].entries[0].revision_digest == late.revision_digest
    assert current_goal.digest == first.goal_digest


def test_same_turn_changed_context_and_wrong_goal_are_rejected(repository):
    current_goal = goal()
    req = SelectionRequest(session(1), "turn", ("procedure",), "agent-test",
                           (), "analyze task", current_goal.digest)
    service = SkillTurnService(repository)
    original = service.select_and_pin(req, episode_id="episode", goal=current_goal)
    replay = service.select_and_pin(req, episode_id="episode", goal=current_goal)
    assert replay["manifest"] == original["manifest"]
    with pytest.raises(ContractError, match="context"):
        service.select_and_pin(replace(req, task_text="other task"),
                               episode_id="other", goal=current_goal)
    with pytest.raises(ContractError, match="goal digest"):
        service.select_and_pin(replace(req, goal_digest="a" * 64),
                               episode_id="wrong-goal", goal=current_goal)
    assert repository.get_episode("other") is None
    assert repository.get_episode("wrong-goal") is None


def test_legacy_store_is_readable_but_never_implicitly_upgraded(repository):
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute("PRAGMA user_version = 1")
    before = repository.db_path.read_bytes()
    assert repository.status()["schema_version"] == 1
    with pytest.raises(DatabaseError, match="no automatic upgrade"):
        repository.initialize()
    assert repository.db_path.read_bytes() == before


def test_availability_digest_drift_fails_closed(repository):
    repository.pin_revision_availability(session(1))
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute("UPDATE revision_availability SET pin_digest = ?", ("f" * 64,))
    with pytest.raises(ContractError, match="digest"):
        repository.pin_revision_availability(session(1))
