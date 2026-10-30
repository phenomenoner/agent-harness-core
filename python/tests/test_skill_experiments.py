"""Trial mechanics only; fixture protection callbacks are not OS qualification."""

from dataclasses import replace

import pytest
from test_skill_graph_convergence import _activate_with_report
from test_skill_markdown_resources import modern, resource

from ahc.application.skill_experiments import SkillExperiments
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.skill_repository import CompareAndSetError


def learning(repository, *, protected=True, owner="agent-test"):
    return SkillExperiments(
        repository,
        owner=owner,
        granted_tools=("workspace.execute",),
        protected_names=("pmo-policy",),
        protection_check=lambda: protected,
    )


def experience(service, identity="input", *, facts=None):
    return service.record_experience(
        source_ref=identity,
        kind="user-input",
        source_digest=digest_json(identity),
        receipt={"accepted": True} if facts is None else facts,
    )


def candidate(old=None, **kwargs):
    item = (
        modern(agent_allowlist=("agent-test",))
        if old is None
        else replace(
            old,
            version="1.0.1",
            predecessor_digest=old.revision_digest,
            body="# Updated\nFollow the user's objective; check the artifact.",
            body_digest=None,
            revision_digest=None,
        )
    )
    return replace(item, body_digest=None, revision_digest=None, **kwargs)


def propose(service, item, ref, *, cause="correction", hypothesis="This may improve relevance"):
    return service.propose(candidate=item, source_ref=ref, cause=cause, hypothesis=hypothesis)


def test_trial_without_proven_benefit_and_later_observation(repository):
    service = learning(repository)
    before = experience(service)
    trial = propose(service, candidate(), before)
    assert service.get(trial)["benefit"] == "unmeasured"
    with pytest.raises(ContractError, match="disabled"):
        service.promote(trial)
    service.configure(enabled=True)
    result = service.promote(trial)
    assert result["benefit"] == "unmeasured"
    assert service.promote(trial) == result  # Lost reply does not reactivate.
    with pytest.raises(ContractError, match="follow activation"):
        service.observe(trial, source_ref=before, judgment="looks better")
    after = experience(service, "later-task")
    observed = service.observe(
        trial, source_ref=after, judgment="Output used requested terminology"
    )
    assert observed["benefit"] == "model-assessment"
    assert observed["state"] == "observed"
    assert observed["automatic_authority_expansion"] is False


def test_protection_required_and_policy_kill_is_in_activation_transaction(repository):
    service = learning(repository, protected=False)
    trial = propose(service, candidate(), experience(service))
    service.configure(enabled=True)
    with pytest.raises(ContractError, match="not protected"):
        service.promote(trial)
    assert repository.get_active("analyze-skill") is None
    assert service.get(trial)["state"] == "proposed"


def test_kill_between_evaluation_and_activation_blocks_only_effect(repository, monkeypatch):
    service = learning(repository)
    trial = propose(service, candidate(), experience(service))
    service.configure(enabled=True)
    save = repository.save_evaluation

    def stop(report):
        result = save(report)
        service.configure(enabled=False)
        return result

    monkeypatch.setattr(repository, "save_evaluation", stop)
    with pytest.raises(ContractError, match="disabled"):
        service.promote(trial)
    assert repository.get_active("analyze-skill") is None


def test_new_trial_rollback_retains_revision_and_allows_exact_retry(repository):
    service = learning(repository)
    item = candidate()
    trial = propose(service, item, experience(service))
    service.configure(enabled=True)
    service.promote(trial)
    service.configure(enabled=False)  # Recovery doesn't depend on enabling new promotion.
    result = service.rollback(trial)
    assert repository.get_active(item.skill_name) is None
    assert repository.get_revision(item.revision_digest) == item
    assert service.rollback(trial) == result
    assert result["external_effects_undone"] is False


def test_existing_trial_rollback_preserves_newer_revision(repository):
    service = learning(repository)
    old = candidate()
    _activate_with_report(repository, old, "base-activation")
    changed = candidate(old)
    trial = propose(service, changed, experience(service))
    service.configure(enabled=True)
    service.promote(trial)
    newer = replace(
        changed,
        version="1.0.2",
        predecessor_digest=changed.revision_digest,
        body_digest=None,
        revision_digest=None,
    )
    _activate_with_report(repository, newer, "newer-activation")
    with pytest.raises(CompareAndSetError):
        service.rollback(trial)
    assert repository.get_active(old.skill_name) == newer


def test_source_owner_and_immutable_receipt(repository):
    service = learning(repository)
    ref = experience(service)
    assert experience(service) == ref
    with pytest.raises(ContractError, match="receipt changed"):
        experience(service, facts={"accepted": False})
    foreign = experience(learning(repository, owner="foreign"), "their-input")
    with pytest.raises(ContractError, match="owned"):
        propose(service, candidate(), foreign)
    with pytest.raises(ContractError, match="owned"):
        propose(service, candidate(), "invented-evidence")


@pytest.mark.parametrize(
    "changes",
    [
        {"required_tools": ("new.tool",)},
        {"skill_name": "pmo-policy"},
        {"agent_allowlist": ()},
        {"assets": (resource(),)},
    ],
)
def test_candidate_cannot_expand_authority_or_mutate_executable_assets(repository, changes):
    service = learning(repository)
    with pytest.raises(ContractError):
        propose(service, candidate(**changes), experience(service))


def test_active_slot_daily_budget_and_same_source_cause_cannot_be_renamed(repository):
    service = learning(repository)
    source = experience(service, facts={"failure_signature": "timeout:read-resource"})
    first = propose(service, candidate(), source)
    with pytest.raises(ContractError, match="active trial"):
        propose(service, candidate(), source, hypothesis="different hypothesis")
    service.reject(first)
    second = propose(service, candidate(), source, cause="renamed cause", hypothesis="second idea")
    service.reject(second)
    with pytest.raises(ContractError, match="unchanged-cause"):
        propose(service, candidate(), source, cause="third label", hypothesis="third idea")
    service.configure(enabled=False, daily_limit=2)
    with pytest.raises(ContractError, match="daily"):
        propose(service, candidate(skill_name="other-skill"), source)
