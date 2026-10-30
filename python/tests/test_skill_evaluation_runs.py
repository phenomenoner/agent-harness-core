"""Evaluator custody and tool ownership; doubles do not qualify native isolation."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from skill_test_support import session
from test_skill_experiments import candidate, experience, learning, propose
from test_workspace_learning import invoke, request
from test_workspace_skills import tools_for

from ahc.application.skill_evaluation_runs import SkillEvaluationRuns
from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


class WorkspaceDouble:
    def __init__(self, factory, workspace_id):
        self.factory = factory
        self.identity = WorkspaceIdentity(factory.session, workspace_id)
        self.namespace = {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.factory.closed += 1
        if self.factory.failure == "cleanup":
            raise RuntimeError("cleanup unverified")

    def execute(self, expected, source):
        assert expected == self.identity
        assert len(source.encode()) <= 16384
        self.identity = replace(expected, revision=expected.revision + 1)
        if source.startswith(("_ahc_candidate_bytes", "candidate=")):
            exec(source, self.namespace)
        else:
            assert self.namespace["candidate"]["revision_digest"] == self.factory.candidate_digest
            self.factory.executed += 1
            if self.factory.failure == "transport":
                raise RuntimeError("execution unknown")
        return {
            "identity": self.identity.to_dict() if self.factory.failure != "identity" else {},
            "ok": self.factory.failure != "case",
            "output": "case observation",
            "output_truncated": False,
        }


class FactoryDouble:
    binding_digest = "a" * 64

    def __init__(self, repository, item):
        self.session, self.skill_root = session(1), repository.root
        self.candidate_digest = item.revision_digest
        self.opened = self.executed = self.closed = 0
        self.failure = None

    def open(self, workspace_id):
        self.opened += 1
        return WorkspaceDouble(self, workspace_id)


def setup(repository, *, body=None, limit=2):
    service = learning(repository)
    item = candidate(**({"body": body} if body else {}))
    trial = propose(service, item, experience(service))
    factory = FactoryDouble(repository, item)
    runs = SkillEvaluationRuns(service, factory, max_runs_per_trial=limit)
    return service, trial, factory, runs


def test_observed_run_survives_reopen_without_dispatch_or_evidence_rewrite(repository):
    service, trial, factory, runs = setup(repository, body="# Context\n" + "x" * 19000)
    value = runs.run(run_id="case-one", trial_id=trial, code="assert candidate")
    assert value["state"] == "observed"
    assert value["result"]["benefit"] == "unmeasured"
    assert value["result"]["output_authority"] == "untrusted-case-output"
    assert factory.opened == factory.closed == factory.executed == 1
    reopened = SkillEvaluationRuns(service, factory, max_runs_per_trial=2)
    assert reopened.run(run_id="case-one", trial_id=trial, code="assert candidate") == value
    assert factory.opened == 1
    assert repository.get_active("analyze-skill") is None
    with pytest.raises(ContractError, match="another request"):
        runs.run(run_id="case-one", trial_id=trial, code="different")


@pytest.mark.parametrize("failure", ["transport", "cleanup", "identity"])
def test_uncertain_execution_or_cleanup_stays_pending_and_cannot_be_renamed(repository, failure):
    _, trial, factory, runs = setup(repository)
    factory.failure = failure
    with pytest.raises((ContractError, RuntimeError)):
        runs.run(run_id="unknown", trial_id=trial, code="case")
    pending = runs.get("unknown")
    assert pending["state"] == "pending" and pending["result"] is None
    assert runs.run(run_id="unknown", trial_id=trial, code="case") == pending
    assert factory.opened == factory.closed == 1
    with pytest.raises(ContractError, match="unresolved"):
        runs.run(run_id="renamed", trial_id=trial, code="case")
    assert factory.opened == 1


def test_failed_case_is_an_observation_and_run_budget_is_durable(repository):
    _, trial, factory, runs = setup(repository, limit=1)
    factory.failure = "case"
    # Preparation must succeed; only the final case is deliberately unsuccessful.
    original = WorkspaceDouble.execute

    def execute(workspace, expected, code):
        failure = factory.failure
        if code != "case":
            factory.failure = None
        try:
            return original(workspace, expected, code)
        finally:
            factory.failure = failure

    from unittest.mock import patch

    with patch.object(WorkspaceDouble, "execute", execute):
        value = runs.run(run_id="first", trial_id=trial, code="case")
    assert value["state"] == "observed" and value["result"]["ok"] is False
    with pytest.raises(ContractError, match="budget"):
        SkillEvaluationRuns(runs.experiments, factory, max_runs_per_trial=1).run(
            run_id="second", trial_id=trial, code="case"
        )
    assert factory.opened == 1


@pytest.mark.parametrize("code", ["", "x" * 16385, "\ud800", 42])
def test_invalid_case_does_not_reserve_or_launch(repository, code):
    _, trial, factory, runs = setup(repository)
    with pytest.raises(ContractError):
        runs.run(run_id="invalid", trial_id=trial, code=code)
    with pytest.raises(ContractError, match="not found"):
        runs.get("invalid")
    assert factory.opened == 0


def test_optional_tool_exposes_real_receipt_only_to_its_conversation(repository):
    service = learning(repository)
    tools = tools_for(repository)
    item = candidate()
    factory = FactoryDouble(repository, item)
    runs = SkillEvaluationRuns(service, factory, max_runs_per_trial=2)
    tools.learning = WorkspaceLearningTools(service, session(1), evaluations=runs)
    ref = tools.learning.record_input(request(session(1)))
    trial = propose(service, item, ref)
    assert invoke(tools, "status")["result"]["isolated_evaluation_available"]
    payload = {"run_id": "case", "trial_id": trial, "code": "case"}
    outcome = invoke(tools, "evaluate", payload)
    assert outcome["disposition"] == "observed"
    value = outcome["result"]
    tools.learning._require_source(value["source_ref"])
    assert invoke(tools, "evaluation_get", {"run_id": "case"})["result"]["state"] == "observed"
    foreign_session = session(1, "another-conversation")
    foreign_factory = FactoryDouble(repository, item)
    foreign_factory.session = foreign_session
    foreign_runs = SkillEvaluationRuns(service, foreign_factory, max_runs_per_trial=2)
    foreign = WorkspaceLearningTools(service, foreign_session, evaluations=foreign_runs)
    workspace = SimpleNamespace(
        identity=WorkspaceIdentity(foreign_session, "foreign"), closed=False
    )
    for action, data in [("evaluate", payload), ("evaluation_get", {"run_id": "case"})]:
        result = foreign.invoke(
            workspace, {"expected": workspace.identity.to_dict(), "action": action, "payload": data}
        )
        assert result["disposition"] == "rejected"
    assert foreign_factory.opened == 0
    factory.failure = "identity"
    pending_payload = {"run_id": "pending", "trial_id": trial, "code": "case"}
    for action, data in [
        ("evaluate", pending_payload),
        ("evaluate", pending_payload),
        ("evaluation_get", {"run_id": "pending"}),
    ]:
        unresolved = invoke(tools, action, data)
        assert unresolved["disposition"] == "unknown"
        assert unresolved["unresolved_effects"] is True
    assert factory.opened == 2
    tools.learning = WorkspaceLearningTools(service, session(1))
    assert invoke(tools, "evaluate", payload)["disposition"] == "rejected"


def test_publish_failure_can_publish_once_on_retry_without_rerunning_case(repository, monkeypatch):
    service, trial, factory, runs = setup(repository)
    record = service.record_experience
    monkeypatch.setattr(service, "record_experience", lambda **_: (_ for _ in ()).throw(OSError()))
    with pytest.raises(RuntimeError, match="unverified"):
        runs.run(run_id="publish", trial_id=trial, code="case")
    assert runs.get("publish")["state"] == "observed"
    monkeypatch.setattr(service, "record_experience", record)
    assert "source_ref" in runs.run(run_id="publish", trial_id=trial, code="case")
    assert factory.opened == factory.closed == factory.executed == 1
