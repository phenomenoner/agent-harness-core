"""Real isolated evaluator composition under the explicitly admitted native service."""

import sys
import time
from dataclasses import replace
from threading import Event, Thread

import pytest
from skill_test_support import goal, session
from test_linux_native import native_owner
from test_linux_workspace_boundary import boundary
from test_skill_experiments import candidate, experience, learning, propose

from ahc.adapters.native_skill_evaluation import NativeSkillEvaluationFactory
from ahc.application.runtime_service import RuntimeService
from ahc.application.skill_evaluation_runs import SkillEvaluationRuns
from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux evaluator composition")
__all__ = ["native_owner", "boundary"]


def factory_for(repository, boundary, tmp_path):
    sandbox, _ = boundary
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(session(1), goal()))
    sandbox = type(sandbox)(
        executable=str(sandbox.executable),
        sha256=sandbox.sha256,
        workspace=str(sandbox.workspace),
        readonly_roots=list(map(str, sandbox.readonly_roots)),
        protected_roots=[*map(str, sandbox.protected_roots), str(store.root), str(repository.root)],
        scratch_limits=NativeScratchLimits(2 * 1024**2, 2 * 1024**2),
    )
    return NativeSkillEvaluationFactory(
        store,
        current,
        repository.root,
        sandbox,
        NativeResourceLimits(128 * 1024**2, 16, 50000),
        EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30),
        "a" * 64,
        "/usr/bin/python3",
        30,
    )


def test_real_candidate_transfer_isolation_receipt_and_cleanup(
    repository, native_owner, boundary, tmp_path
):
    factory = factory_for(repository, boundary, tmp_path)
    service = learning(repository, protected=False)
    item = candidate(body="# Procedure\n" + "context " * 2200)
    trial = propose(service, item, experience(service))
    runs = SkillEvaluationRuns(service, factory, max_runs_per_trial=2)
    code = (
        "from pathlib import Path\n"
        f"assert candidate['revision_digest']=={item.revision_digest!r}\n"
        f"assert not Path({str(repository.root)!r}).exists()\n"
        f"assert not Path({str(factory.store.root)!r}).exists()\n"
        "Path('ephemeral').write_text('owned')\nprint('candidate read; stores hidden')"
    )
    outcome = runs.run(run_id="native-case", trial_id=trial, code=code)
    assert outcome["state"] == "observed" and outcome["result"]["ok"] is True
    assert "stores hidden" in outcome["result"]["output"]
    assert not (factory.boundary.workspace / "ephemeral").exists()
    assert runs.run(run_id="native-case", trial_id=trial, code=code) == outcome
    assert repository.get_active(item.skill_name) is None
    identity = outcome["result"]["workspace"]
    custody = BackendCustody(factory.store, resource_role="python-workspace")
    lease = custody.acquire(factory.session, identity["workspace_id"])
    try:
        assert not custody._held(lease)
    finally:
        lease.close()
    failed = runs.run(run_id="negative-case", trial_id=trial, code="raise ValueError('example')")
    assert failed["state"] == "observed" and failed["result"]["ok"] is False
    assert failed["result"]["benefit"] == "unmeasured"


def test_factory_refuses_unprotected_stores_and_unbounded_profile(repository, boundary, tmp_path):
    factory = factory_for(repository, boundary, tmp_path)
    with pytest.raises(ContractError, match="protected"):
        replace(factory, skill_root=tmp_path)
    with pytest.raises(ContractError, match="finite"):
        replace(factory, lifetime_seconds=0)
    with pytest.raises(ContractError, match="finite"):
        replace(factory, limits=replace(factory.limits, swap_bytes=1024))
    # Mutable boundary state must be readmitted even with a frozen factory.
    factory.boundary.scratch_limits = None
    with pytest.raises(ContractError, match="finite"):
        factory.open("refused")


def test_hung_case_expires_once_and_keeps_durable_pending_custody(
    repository, native_owner, boundary, tmp_path, monkeypatch
):
    from ahc.adapters import codex_transport, native_python_workspace, native_skill_evaluation

    factory = replace(factory_for(repository, boundary, tmp_path), lifetime_seconds=5)
    service = learning(repository, protected=False)
    trial = propose(service, candidate(), experience(service))
    runs = SkillEvaluationRuns(service, factory, max_runs_per_trial=1)
    run_id = "hung-case"
    code = (
        "from pathlib import Path\n"
        "Path('timeout-marker').write_text('entered')\n"
        "while True:\n"
        "    pass\n"
    )
    submitted, expiry_started = Event(), Event()
    state = {"dispatches": 0}

    real_runner = native_skill_evaluation.NativeProcessRunner

    def tracked_runner(*args, **kwargs):
        runner = real_runner(*args, **kwargs)
        expire = runner._expire

        def observe_expiry(owned):
            expiry_started.set()
            return expire(owned)

        runner._expire = observe_expiry
        state["runner"] = runner
        return runner

    real_execute = native_python_workspace.NativePythonWorkspace.execute

    def observe_case(workspace, expected, source):
        if source == code:
            state["port"] = workspace
        return real_execute(workspace, expected, source)

    real_send = codex_transport.CodexStdioTransport.send

    def observe_send(transport, message, *args, **kwargs):
        result = real_send(transport, message, *args, **kwargs)
        if message.get("operation") == "execute" and message.get("code") == code:
            state["dispatches"] += 1
            submitted.set()
        return result

    monkeypatch.setattr(native_skill_evaluation, "NativeProcessRunner", tracked_runner)
    monkeypatch.setattr(
        native_python_workspace.NativePythonWorkspace, "execute", observe_case
    )
    monkeypatch.setattr(codex_transport.CodexStdioTransport, "send", observe_send)
    result = {}

    def run_case():
        try:
            result["value"] = runs.run(run_id=run_id, trial_id=trial, code=code)
        except BaseException as error:
            result["error"] = error

    worker = Thread(target=run_case, daemon=True)
    started = time.monotonic()
    forced_stop = False
    worker.start()
    try:
        deadline = started + 12
        assert submitted.wait(max(0, deadline - time.monotonic())), (
            "the hung case was not submitted to the isolated evaluator"
        )
        assert expiry_started.wait(max(0, deadline - time.monotonic())), (
            "the native evaluator lifetime timer did not fire"
        )
        worker.join(max(0, deadline - time.monotonic()))
    finally:
        if worker.is_alive():
            forced_stop = True
            runner = state.get("runner")
            if runner is not None:
                try:
                    runner.close()
                except Exception:
                    pass
            worker.join(5)

    assert not worker.is_alive(), "timed-out evaluator did not terminate within its bound"
    assert not forced_stop, "test cleanup had to stop an evaluator past its lifetime"
    assert time.monotonic() - started < 17
    assert isinstance(result.get("error"), RuntimeError)
    assert "value" not in result

    pending = SkillEvaluationRuns(service, factory, max_runs_per_trial=1).get(run_id)
    assert pending["state"] == "pending" and pending["result"] is None
    port = state["port"]
    assert not (factory.boundary.workspace / "timeout-marker").exists()
    custody = BackendCustody(factory.store, resource_role="python-workspace")
    lease = custody.acquire(factory.session, port.identity.workspace_id)
    try:
        held = custody._held(lease)
        if held:
            assert pending["state"] == "pending"
            assert state["runner"]._expiry_error in (
                "cleanup-incomplete", "cleanup-unverified"
            ) or port.last_failure.get("cleanup") == "unverified"
        else:
            assert state["runner"]._group.complete
    finally:
        lease.close()

    reopened = SkillEvaluationRuns(service, factory, max_runs_per_trial=1)
    assert reopened.run(run_id=run_id, trial_id=trial, code=code) == pending
    assert state["dispatches"] == 1
    with pytest.raises(ContractError, match="unresolved"):
        reopened.run(run_id="hung-case-retry", trial_id=trial, code="print('retry')")
    assert state["dispatches"] == 1


def test_model_tool_evaluates_inside_admitted_turn_then_continues(
    repository, native_owner, boundary, tmp_path
):
    factory = factory_for(repository, boundary, tmp_path)
    service = learning(repository, protected=False)
    runs = SkillEvaluationRuns(service, factory, max_runs_per_trial=2)
    tools = WorkspaceLearningTools(service, factory.session, evaluations=runs)
    observed = {}
    request = AgentTurnRequest(factory.session, 'evaluate-in-turn', factory.expected.goal.digest,
        'Check a bounded skill improvement', (), 'a' * 64, 'b' * 64, 'parent',
        time.monotonic() + 60)
    code = ('from pathlib import Path\n'
            f'assert not Path({str(repository.root)!r}).exists()\n'
            f'assert not Path({str(factory.store.root)!r}).exists()\n'
            'assert candidate["skill_name"]=="in-turn-procedure"\n'
            'print("isolated candidate observed")')
    with factory.open('parent') as parent:
        def invoke(action, payload):
            return tools.invoke(parent, {'expected': parent.identity.to_dict(),
                                         'action': action, 'payload': payload})

        class Backend:
            def invoke(self, bound):
                tools.record_input(bound)
                status = invoke('status', {})
                proposed = invoke('propose', {'skill_name': 'in-turn-procedure',
                    'body': '# Procedure\nInspect evidence before deciding.',
                    'description': 'Inspect relevant evidence before deciding.',
                    'source_ref': status['result']['experiences'][0]['ref'],
                    'cause': 'observed evidence gap',
                    'hypothesis': 'Evidence may improve grounding'})
                assert proposed['disposition'] == 'observed', proposed
                observed['trial'] = proposed['result']['trial_id']
                payload = {'run_id': 'inside-parent', 'trial_id': observed['trial'], 'code': code}
                observed['evaluation'] = invoke('evaluate', payload)
                observed['replay'] = invoke('evaluate', payload)
                observed['promotion'] = invoke('promote', {'trial_id': observed['trial']})
                return AgentTurnObservation(bound, 'completed', 'fixture', 1, 1,
                                            output_text='parent completed')

        result = RuntimeService(factory.store, Backend()).execute(factory.expected, request)
        assert result.result['accepted'], result.result
        outcome = observed['evaluation']
        assert outcome['disposition'] == 'observed', outcome
        assert outcome['result']['result']['ok'] is True
        assert observed['replay'] == outcome
        assert observed['promotion']['disposition'] == 'rejected'
        assert repository.get_active('in-turn-procedure') is None
        # The borrowed scope is gone; the still-owned parent interpreter works.
        assert parent.execute(parent.identity, 'print("parent continues")')['ok']
    # The factory must use the fresh current snapshot, not its construction snapshot.
    after = runs.run(run_id='after-parent', trial_id=observed['trial'], code=code)
    assert after['state'] == 'observed' and after['result']['ok'] is True
    with factory.store._read() as db:
        held = db.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0]
        assert held == 0
