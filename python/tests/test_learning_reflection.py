"""Delivered-source reflection with real SQLite gates and controlled model calls."""
import json
from dataclasses import replace
from types import SimpleNamespace
import pytest

from test_runtime_learning_queue import ready, source, accepted, delivered, POLICY
from ahc.application.learning_reflection import LearningReflectionController, reflection_prompt
from ahc.domain.child_work import _task_text
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_learning_queue import RuntimeLearningQueue, ReflectionPolicy
from ahc.infrastructure.runtime_inbox import RuntimeInbox

NOOP = '{"action":"no-op","reason":"No reusable correction was observed."}'

class Calls:
    def __init__(self, effect=lambda *a, **k: NOOP):
        self.count, self.effect = 0, effect
    def run(self, *args, **kwargs):
        self.count += 1
        return self.effect(*args, **kwargs)

def controller(req, queue, calls):
    learning = SimpleNamespace(session=req.session, handoff=queue)
    return LearningReflectionController(learning, POLICY, calls)

def test_delivery_gate_then_once_only_noop(tmp_path):
    repo, snapshot, req, route = source(tmp_path)
    accepted(repo, snapshot, req)
    queue, calls = RuntimeLearningQueue(repo), Calls()
    control = controller(req, queue, calls)
    assert control.step() is None and calls.count == 0
    delivered(repo, req, route)
    result = control.step()
    assert result["state"] == "complete" and result["action"] == "no-op"
    assert result["automatic_activation"] is False
    assert calls.count == 1 and control.step() is None
    assert queue.get(req.session, req.operation_id)["state"] == "complete"

@pytest.mark.parametrize("effect", [
    lambda *a, **k: "bad JSON",
    lambda *a, **k: '{"action":"no-op","reason":""}',
    lambda *a, **k: (_ for _ in ()).throw(OSError("provider unavailable")),
], ids=["malformed", "invalid", "unavailable"])
def test_failure_is_unknown_and_never_replayed(tmp_path, effect):
    repo, req, queue, clock = ready(tmp_path)
    calls = Calls(effect)
    control = controller(req, queue, calls)
    assert control.step()["state"] == "unknown"
    assert control.step() is None and calls.count == 1
    assert controller(req, queue, calls).step() is None and calls.count == 1

@pytest.mark.parametrize("change", ["stop", "deadline", "input"])
def test_late_result_cannot_commit_after_stop_deadline_or_priority_input(tmp_path, change):
    repo, req, queue, clock = ready(tmp_path)
    def effect(*args, **kwargs):
        if change == "stop":
            queue.stop(req.session, req.operation_id)
        elif change == "deadline":
            clock[0] += POLICY.job_timeout_seconds * 1000
        else:
            current = repo.read_current(req.session.ref.virtual_session_id)
            RuntimeInbox(repo).submit(req.session, "next-input", current.goal.digest, "new work")
        assert kwargs["cancelled"]()
        return NOOP
    calls = Calls(effect)
    control = controller(req, queue, calls)
    result = control.step()
    assert calls.count == 1
    assert result["state"] in ("unknown", "cancelled")
    assert queue.get(req.session, req.operation_id)["decision_json"] != NOOP

def test_preexisting_input_and_closed_owner_do_not_claim(tmp_path):
    repo, req, queue, clock = ready(tmp_path)
    current = repo.read_current(req.session.ref.virtual_session_id)
    RuntimeInbox(repo).submit(req.session, "next-input", current.goal.digest, "new work")
    calls = Calls()
    control = controller(req, queue, calls)
    assert control.step() is None
    control.close()
    assert control.step() is None and calls.count == 0
    assert queue.get(req.session, req.operation_id)["state"] == "awaiting-delivery"

def test_lost_claim_is_unknown_after_reopen(tmp_path):
    repo, req, queue, clock = ready(tmp_path)
    queue.claim(req.session, req.operation_id, "old-owner", POLICY)
    calls = Calls()
    assert controller(req, queue, calls).step() is None and calls.count == 0
    assert queue.get(req.session, req.operation_id)["state"] == "unknown"

def test_custom_model_effort_is_bound_by_claim_and_finish(tmp_path):
    repo, req, queue, clock = ready(tmp_path)
    policy = ReflectionPolicy(60, 100, 200, 2, "gpt-6-sol", "high")
    binding = queue.claim(req.session, req.operation_id, "owner", policy)
    assert binding["model"] == "gpt-6-sol" and binding["effort"] == "high"
    result = queue.finish(req.session, req.operation_id, "owner",
                          json.loads(NOOP), model="gpt-6-luna", effort="max")
    assert result["state"] == "unknown"

def test_reflection_disclosure_handles_escaped_and_long_context():
    prompt = reflection_prompt({"source_operation": "source", "facts": {"accepted": True},
        "selected_revisions": ["a"*64]*64, "input": "\n"*4096, "output": '"'*10000})
    _task_text(prompt)
    assert len(prompt) <= 4096
    assert len(json.loads(prompt.split("\n", 1)[1])["selected_revisions"]) == 8

def test_idle_runtime_wires_reflection_and_close(tmp_path):
    from ahc.application.foreground_runtime import ForegroundRuntime
    repo, req, queue, clock = ready(tmp_path)
    calls = Calls()
    control = controller(req, queue, calls)
    learning = SimpleNamespace(controller=control)
    runtime = ForegroundRuntime(repo, req.session, lambda *args: None, None, learning=learning)
    runtime.start()
    try:
        assert runtime.step() is None
        assert calls.count == 1 and control.last["state"] == "complete"
    finally:
        runtime.close()
    assert control.closed

def test_model_proposal_uses_owned_harness_receipt_and_stays_inert(tmp_path, repository):
    from ahc.application.workspace_learning import WorkspaceLearningTools
    from ahc.application.skill_experiments import SkillExperiments
    repo, req, queue, clock = ready(tmp_path)
    experiments = SkillExperiments(repository, owner=req.session.agent_id, granted_tools=(),
                                   protected_names=(), protection_check=lambda: True)
    learning = WorkspaceLearningTools(experiments, req.session, handoff=queue)
    proposal = {"action": "propose", "reason": "The output omitted a required final result.",
                "proposal": {"skill_name": "result-check", "body": "Check that the final answer contains the requested result.",
                             "description": "Verify requested result", "cause": "result omission",
                             "hypothesis": "A final result check reduces omission."}}
    calls = Calls(lambda *a, **k: json.dumps(proposal))
    control = LearningReflectionController(learning, POLICY, calls)
    result = control.step()
    assert result["state"] == "complete" and "proposal" in result
    assert repository.get_active("result-check") is None
    assert result["automatic_activation"] is False
    assert not learning.capture_gap

def test_writer_contention_at_claim_is_contained_and_later_step_progresses(tmp_path, monkeypatch):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, req, queue, clock = ready(tmp_path)
    calls = Calls()
    control = controller(req, queue, calls)
    original = queue.claim
    def busy(*args, **kwargs):
        raise WorkingWriterBusy("controlled contention")
    monkeypatch.setattr(queue, "claim", busy)
    assert control.step() is None and calls.count == 0
    assert control.last["state"] == "deferred"
    monkeypatch.setattr(queue, "claim", original)
    assert control.step()["state"] == "complete" and calls.count == 1

def test_writer_contention_at_abandon_never_escapes_or_replays(tmp_path, monkeypatch):
    from ahc.infrastructure.working_repository import WorkingWriterBusy
    repo, req, queue, clock = ready(tmp_path)
    calls = Calls(lambda *a, **k: "bad JSON")
    control = controller(req, queue, calls)
    def busy(*args, **kwargs):
        raise WorkingWriterBusy("controlled contention")
    monkeypatch.setattr(queue, "abandon", busy)
    assert control.step() is None
    assert calls.count == 1 and control.last["state"] == "deferred"
    assert control.step() is None and calls.count == 1

def test_input_committed_at_final_finish_seam_defeats_completion(tmp_path, monkeypatch):
    repo, req, queue, clock = ready(tmp_path)
    calls = Calls()
    control = controller(req, queue, calls)
    original = queue.finish
    def interleave(*args, **kwargs):
        current = repo.read_current(req.session.ref.virtual_session_id)
        RuntimeInbox(repo).submit(req.session, "seam-input", current.goal.digest, "new user work")
        return original(*args, **kwargs)
    monkeypatch.setattr(queue, "finish", interleave)
    result = control.step()
    assert result["state"] == "unknown" and "proposal" not in result
    assert calls.count == 1
