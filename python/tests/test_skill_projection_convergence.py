"""Wave-5 regressions for immutable relationship projection agreement."""

from __future__ import annotations

import sqlite3

import pytest
from skill_test_support import goal, request, revision

from ahc.application.skill_service import SkillTurnService
from ahc.domain.skill_models import SkillRelationship, SkillRevision
from ahc.infrastructure.skill_doctor import doctor, inventory
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import (
    CompareAndSetError,
    RepositoryError,
    SkillRepository,
)


def _prepare_evaluation(repository: SkillRepository, candidate: SkillRevision):
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    repository.save_evaluation(report)
    return report


def _activate(repository: SkillRepository, candidate: SkillRevision, operation_key: str):
    report = _prepare_evaluation(repository, candidate)
    current = repository.get_active(candidate.skill_name)
    return repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=None if current is None else current.revision_digest,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        graph_input_digest=report.graph_input_digest,
        operation_key=operation_key,
    )


def _change_target(
    repository: SkillRepository,
    owner: SkillRevision,
    old_target: SkillRevision,
    new_target: SkillRevision,
) -> None:
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute(
            "UPDATE relationships SET target_digest = ? "
            "WHERE revision_digest = ? AND target_digest = ?",
            (new_target.revision_digest, owner.revision_digest, old_target.revision_digest),
        )


def _change_kind(repository: SkillRepository, owner: SkillRevision, new_kind: str) -> None:
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute(
            "UPDATE relationships SET kind = ? WHERE revision_digest = ?",
            (new_kind, owner.revision_digest),
        )


def test_projection_target_drift_blocks_catalog_reads(repository: SkillRepository) -> None:
    target_a = revision(name="target-a", trigger="target-a")
    target_b = revision(name="target-b", trigger="target-b")
    repository.register_revision(target_a)
    repository.register_revision(target_b)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("complements", target_a.revision_digest or ""),),
    )
    repository.register_revision(candidate)
    _change_target(repository, candidate, target_a, target_b)

    with pytest.raises(RepositoryError, match="relationship projection"):
        repository.list_revisions()
    with pytest.raises(RepositoryError, match="relationship projection"):
        repository.active_revisions()


def test_projection_target_drift_blocks_activation_before_pointer(
    repository: SkillRepository,
) -> None:
    target_a = revision(name="target-a", trigger="target-a")
    target_b = revision(name="target-b", trigger="target-b")
    repository.register_revision(target_a)
    repository.register_revision(target_b)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("complements", target_a.revision_digest or ""),),
    )
    report = _prepare_evaluation(repository, candidate)
    _change_target(repository, candidate, target_a, target_b)

    with pytest.raises(CompareAndSetError, match="relationship projection"):
        repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest or "",
            expected_active_digest=None,
            evaluation_digest=report.evaluation_digest or "",
            input_digest=report.input_digest,
            graph_input_digest=report.graph_input_digest,
            operation_key="projection-target-activation",
        )
    assert repository.get_active(candidate.skill_name) is None
    with sqlite3.connect(repository.db_path) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM receipts WHERE operation_key = ?",
                ("projection-target-activation",),
            ).fetchone()
            is None
        )


def test_projection_kind_drift_blocks_selection_before_manifest(
    repository: SkillRepository,
) -> None:
    target = revision(name="target-skill", trigger="target")
    repository.register_revision(target)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("complements", target.revision_digest or ""),),
    )
    _activate(repository, candidate, "projection-selection-setup")
    _change_kind(repository, candidate, "variant_of")
    current_goal = goal()

    with pytest.raises(RepositoryError, match="relationship projection"):
        SkillTurnService(repository).select_and_pin(
            request(1, current_goal.digest or "", "candidate task"),
            episode_id="projection-selection",
            goal=current_goal,
        )
    with sqlite3.connect(repository.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM manifests").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_projection_kind_drift_blocks_rollback_before_pointer(
    repository: SkillRepository,
) -> None:
    target = revision(name="target-skill", trigger="target")
    repository.register_revision(target)
    alpha_v1 = revision(
        name="alpha-skill",
        trigger="alpha",
        relationships=(SkillRelationship("complements", target.revision_digest or ""),),
    )
    _activate(repository, alpha_v1, "projection-rollback-v1")
    alpha_v2 = revision(
        name="alpha-skill",
        trigger="alpha",
        version="1.0.1",
        predecessor=alpha_v1.revision_digest,
    )
    _activate(repository, alpha_v2, "projection-rollback-v2")
    _change_kind(repository, alpha_v1, "variant_of")

    with pytest.raises(CompareAndSetError, match="relationship projection"):
        repository.rollback(
            skill_name=alpha_v2.skill_name,
            expected_active_digest=alpha_v2.revision_digest or "",
            operation_key="projection-rollback-attempt",
        )
    assert repository.get_active(alpha_v2.skill_name) == alpha_v2
    with sqlite3.connect(repository.db_path) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM receipts WHERE operation_key = ?",
                ("projection-rollback-attempt",),
            ).fetchone()
            is None
        )


def test_doctor_reuses_projection_semantics_read_only(repository: SkillRepository) -> None:
    target = revision(name="target-skill", trigger="target")
    repository.register_revision(target)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("complements", target.revision_digest or ""),),
    )
    repository.register_revision(candidate)
    _change_kind(repository, candidate, "variant_of")

    before = inventory(repository.root)
    report = doctor(repository.root)
    issues = report["issues"]
    assert isinstance(issues, list)
    assert "relationship-projection" in issues
    assert inventory(repository.root) == before
