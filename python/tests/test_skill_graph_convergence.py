"""Focused regressions for canonical digest-bound skill graph convergence."""

from __future__ import annotations

import sqlite3

import pytest
from skill_test_support import goal, request, revision

from ahc.application.skill_learning import learn_episode
from ahc.application.skill_service import SkillTurnService
from ahc.domain import OutcomeAttribution
from ahc.domain.judgment_records import ATTRIBUTION_SCHEMA
from ahc.domain.sessions import ContractError
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.domain.skill_graph import validate_active_graph
from ahc.domain.skill_models import SkillRelationship
from ahc.domain.skill_selection import select_revisions
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.skill_doctor import doctor, inventory
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import CompareAndSetError, SkillRepository


def _attribution(episode_id: str) -> OutcomeAttribution:
    return OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        episode_id,
        "wrong-how",
        ("how-procedure",),
        ("graph-test-evidence",),
        "graph-test-verifier",
    )


def _lesson(
    name: str,
    trigger: str,
    *,
    relationships: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "skill_name": name,
        "description": "Bounded graph test procedure",
        "cognitive_layer": "how-procedure",
        "roles": ["procedure"],
        "agent_allowlist": [],
        "required_tools": [],
        "triggers": [trigger],
        "expected_effects": ["bounded report"],
        "positive_examples": [f"{trigger} task"],
        "negative_examples": ["summarize task"],
        "relationships": [] if relationships is None else relationships,
        "body": revision(name=name, trigger=trigger).body,
    }


def _activate_with_report(repository: SkillRepository, candidate, operation_key: str):
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    repository.save_evaluation(report)
    current = repository.get_active(candidate.skill_name)
    repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=None if current is None else current.revision_digest,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        operation_key=operation_key,
    )
    return report


def _learn(
    repository: SkillRepository,
    *,
    generation: int,
    episode_id: str,
    lesson: dict[str, object],
    evaluate: bool = True,
    activate: bool = True,
) -> dict[str, object]:
    current_goal = goal()
    SkillTurnService(repository).select_and_pin(
        request(generation, current_goal.digest or ""),
        episode_id=episode_id,
        goal=current_goal,
    )
    proposal = learn_episode(
        repository,
        episode_id=episode_id,
        outcome="verified-failure",
        evidence_refs=("graph-test-evidence",),
        attribution=_attribution(episode_id),
        lesson=lesson,
        reflect=True,
    )
    evidence = HeldOutTaskEvidence(
        proposal["candidate"].revision_digest,
        digest_json({"task": "held-out-graph"}),
        digest_json({"answer": 42}),
        digest_json({"answer": 42}),
        ("graph-held-out-receipt",),
    )
    return learn_episode(
        repository,
        episode_id=episode_id,
        outcome="verified-failure",
        evidence_refs=("graph-test-evidence",),
        attribution=_attribution(episode_id),
        lesson=lesson,
        reflect=True,
        evaluate=evaluate,
        activate=activate,
        held_out_evidence=(evidence,),
        operation_key=f"graph-{episode_id}",
    )


def test_direct_relationship_construction_and_persisted_parsing_reject_names() -> None:
    with pytest.raises(ContractError, match="digest"):
        SkillRelationship("depends_on", "alpha-skill")
    with pytest.raises(ContractError, match="digest"):
        SkillRelationship.from_dict({"kind": "depends_on", "target": "alpha-skill"})


def test_lesson_name_resolves_once_to_exact_digest_across_replacement(
    repository: SkillRepository,
) -> None:
    alpha_v1 = revision(name="alpha-skill", trigger="alpha")
    _activate_with_report(repository, alpha_v1, "graph-alpha-v1")
    learned = _learn(
        repository,
        generation=1,
        episode_id="graph-name-resolution",
        lesson=_lesson(
            "dependent-skill",
            "dependent",
            relationships=[{"kind": "depends_on", "target": "alpha-skill"}],
        ),
        evaluate=False,
        activate=False,
    )
    candidate = learned["candidate"]
    assert candidate.relationships[0].target == alpha_v1.revision_digest
    alpha_v2 = revision(
        name="alpha-skill",
        trigger="alpha",
        version="1.0.1",
        predecessor=alpha_v1.revision_digest,
    )
    _activate_with_report(repository, alpha_v2, "graph-alpha-v2")
    stored = repository.get_revision(candidate.revision_digest or "")
    assert stored.relationships[0].target == alpha_v1.revision_digest
    learned_after_replacement = _learn(
        repository,
        generation=2,
        episode_id="graph-name-resolution-after-replacement",
        lesson=_lesson(
            "dependent-after-replacement",
            "dependent-after",
            relationships=[{"kind": "depends_on", "target": "alpha-skill"}],
        ),
        evaluate=False,
        activate=False,
    )
    resolved_after_replacement = learned_after_replacement["candidate"]
    assert resolved_after_replacement.relationships[0].target == alpha_v2.revision_digest


def test_repository_real_wave_four_sequence_blocks_orphaning_replacement(
    repository: SkillRepository,
) -> None:
    alpha_v1 = revision(name="alpha-skill", trigger="alpha")
    _activate_with_report(repository, alpha_v1, "wave-alpha-v1")
    beta_result = _learn(
        repository,
        generation=1,
        episode_id="wave-beta",
        lesson=_lesson(
            "beta-skill",
            "beta",
            relationships=[{"kind": "depends_on", "target": "alpha-skill"}],
        ),
    )
    beta = beta_result["candidate"]
    assert repository.get_active("beta-skill") == beta
    alpha_v2_lesson = _lesson(
        "alpha-skill",
        "alpha",
        relationships=[{"kind": "depends_on", "target": "beta-skill"}],
    )
    with pytest.raises(ContractError, match="blocked activation"):
        _learn(
            repository,
            generation=2,
            episode_id="wave-alpha-v2",
            lesson=alpha_v2_lesson,
        )
    assert repository.get_active("alpha-skill") == alpha_v1
    assert repository.get_active("beta-skill") == beta


def test_materialized_but_inactive_exact_dependency_is_blocked(
    repository: SkillRepository,
) -> None:
    inactive = revision(name="inactive-skill", trigger="inactive")
    repository.register_revision(inactive)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("depends_on", inactive.revision_digest or ""),),
    )
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "block"
    assert not next(
        check for check in report.checks if check.check_id == "relationship-activation"
    ).passed
    assert repository.get_active("candidate-skill") is None


def test_activation_rejects_graph_snapshot_drift_without_pointer_mutation(
    repository: SkillRepository,
) -> None:
    other_v1 = revision(name="other-skill", trigger="other")
    _activate_with_report(repository, other_v1, "drift-other-v1")
    candidate = revision(name="candidate-skill", trigger="candidate")
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.verdict == "accept"
    repository.save_evaluation(report)
    other_v2 = revision(
        name="other-skill",
        trigger="other",
        version="1.0.1",
        predecessor=other_v1.revision_digest,
    )
    _activate_with_report(repository, other_v2, "drift-other-v2")
    with pytest.raises(CompareAndSetError, match="graph"):
        repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest or "",
            expected_active_digest=None,
            evaluation_digest=report.evaluation_digest or "",
            input_digest=report.input_digest,
            operation_key="drift-candidate",
        )
    assert repository.get_active(candidate.skill_name) is None


def test_complement_variant_and_replacement_edges_are_not_dependencies_or_conflicts(
    repository: SkillRepository,
) -> None:
    dormant = revision(name="dormant-skill", trigger="dormant")
    repository.register_revision(dormant)
    candidate = revision(
        name="edge-skill",
        trigger="edge",
        relationships=(
            SkillRelationship("complements", dormant.revision_digest or ""),
            SkillRelationship("replaces", dormant.revision_digest or ""),
            SkillRelationship("variant_of", dormant.revision_digest or ""),
        ),
    )
    report = _activate_with_report(repository, candidate, "edge-candidate")
    assert report.verdict == "accept"
    assert repository.get_active("edge-skill") == candidate
    selected = SkillTurnService(repository).select_and_pin(
        request(1, goal().digest or "", "edge task"),
        episode_id="edge-selection",
        goal=goal(),
    )
    assert [entry.revision_digest for entry in selected["manifest"].entries] == [
        candidate.revision_digest
    ]


def test_rollback_rejects_orphaning_an_active_exact_dependency(
    repository: SkillRepository,
) -> None:
    alpha_v1 = revision(name="alpha-skill", trigger="alpha")
    _activate_with_report(repository, alpha_v1, "rollback-alpha-v1")
    alpha_v2 = revision(
        name="alpha-skill",
        trigger="alpha",
        version="1.0.1",
        predecessor=alpha_v1.revision_digest,
    )
    _activate_with_report(repository, alpha_v2, "rollback-alpha-v2")
    beta = revision(
        name="beta-skill",
        trigger="beta",
        relationships=(SkillRelationship("depends_on", alpha_v2.revision_digest or ""),),
    )
    _activate_with_report(repository, beta, "rollback-beta")
    with pytest.raises(CompareAndSetError, match="invalid active graph"):
        repository.rollback(
            skill_name="alpha-skill",
            expected_active_digest=alpha_v2.revision_digest or "",
            operation_key="rollback-orphan-alpha",
        )
    assert repository.get_active("alpha-skill") == alpha_v2
    assert repository.get_active("beta-skill") == beta


def test_selection_fails_closed_for_an_invalid_active_graph(repository: SkillRepository) -> None:
    inactive = revision(name="inactive-skill", trigger="inactive")
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("depends_on", inactive.revision_digest or ""),),
    )
    result = select_revisions(
        request(1, goal().digest or "", "candidate task"),
        (candidate,),
        materialized_revisions=(inactive, candidate),
    )
    assert result.manifest.entries == ()
    assert result.selected == ()
    assert result.abstained_roles == ("procedure",)


def test_graph_snapshot_ignores_unrelated_dormant_materialized_revision() -> None:
    active = revision(name="active-skill", trigger="active")
    dormant = revision(name="dormant-skill", trigger="dormant")
    one = validate_active_graph((active,), (active,))
    two = validate_active_graph((active,), (active, dormant))
    assert one.valid is True
    assert two.valid is True
    assert one.graph_digest == two.graph_digest


def test_doctor_reports_graph_issue_read_only(repository: SkillRepository) -> None:
    inactive = revision(name="inactive-skill", trigger="inactive")
    repository.register_revision(inactive)
    candidate = revision(
        name="candidate-skill",
        trigger="candidate",
        relationships=(SkillRelationship("depends_on", inactive.revision_digest or ""),),
    )
    repository.register_revision(candidate)
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute(
            "INSERT INTO active_revisions(skill_name, revision_digest, "
            "predecessor_digest, updated_seq) VALUES(?,?,?,?)",
            (candidate.skill_name, candidate.revision_digest, None, 1),
        )
    before = inventory(repository.root)
    report = doctor(repository.root)
    assert "graph:inactive-dependency" in report["issues"]
    assert inventory(repository.root) == before
