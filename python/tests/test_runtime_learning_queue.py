"""Real SQLite/channel seams for the learning handoff; no provider model calls."""
import json
import time
from dataclasses import replace

import pytest
from test_channel_outbox import state, accepted
from test_runtime_core import Backend

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_learning_queue import RuntimeLearningQueue, ReflectionPolicy, RETENTION_MS
from ahc.infrastructure import runtime_learning_queue as module
from ahc.infrastructure.working_repository import WorkingRepositoryError


POLICY=ReflectionPolicy(60,100,200,2)


def source(tmp_path):
    repo,snapshot,request,route=state(tmp_path)
    request=replace(request,capabilities=('workspace.learning',))
    return repo,snapshot,request,route


def delivered(repo,request,route,*,unknown=False):
    class Transport:
        def send(self,*args):
            if unknown: raise OSError('delivery receipt lost')
            return 'controlled-receipt'
    return ChannelDelivery(ChannelOutbox(repo),route,Transport()).deliver(request.operation_id,'delivery')


def ready(tmp_path):
    repo,snapshot,req,route=source(tmp_path);accepted(repo,snapshot,req);delivered(repo,req,route)
    clock=[int(time.time()*1000)]
    queue=RuntimeLearningQueue(repo,clock=lambda:clock[0])
    return repo,req,queue,clock


def unadmitted(repo, req, operation_id, *, held=False):
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    RuntimeInbox(repo).submit(req.session, operation_id, req.goal_digest, "new input")
    if held:
        def mark_held(db):
            db.execute("CREATE TABLE IF NOT EXISTS channel_held (operation_id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO channel_held VALUES(?)", (operation_id,))

        repo._mutate(mark_held)


@pytest.mark.parametrize(
    ("pending_inputs", "expected_state"),
    [
        ([('held', True)], 'complete'),
        ([('eligible', False)], 'unknown'),
        ([('held', True), ('eligible', False)], 'unknown'),
    ],
    ids=['held-only', 'eligible-input', 'mixed-held-and-eligible'],
)
def test_finish_fences_only_eligible_unadmitted_input(tmp_path, pending_inputs, expected_state):
    repo,req,queue,_=ready(tmp_path)
    queue.claim(req.session,req.operation_id,'owner',POLICY)
    for suffix, held in pending_inputs:
        unadmitted(repo, req, f"pending-{suffix}", held=held)

    result=queue.finish(req.session,req.operation_id,'owner',{'action':'no-op'},
                        model='gpt-6-luna',effort='max')

    if expected_state == 'complete':
        assert result == {'state':'complete','automatic_activation':False,'benefit':'unmeasured'}
    else:
        assert result == {'state':'unknown','replay_allowed':False}
    assert queue.get(req.session,req.operation_id)['state'] == expected_state


@pytest.mark.parametrize(
    ("pending_inputs", "allowed"),
    [
        ([('held', True)], True),
        ([('eligible', False)], False),
        ([('held', True), ('eligible', False)], False),
    ],
    ids=['held-only', 'eligible-input', 'mixed-held-and-eligible'],
)
def test_followup_effect_guard_fences_only_eligible_unadmitted_input(tmp_path, pending_inputs, allowed):
    repo,req,queue,_=ready(tmp_path)
    called=[]
    for suffix, held in pending_inputs:
        unadmitted(repo, req, f"effect-{suffix}", held=held)

    if allowed:
        queue.guard_effect(req.session, req.operation_id, 0, lambda: called.append('effect'))
        assert called == ['effect']
    else:
        with pytest.raises(ContractError, match='follow-up revoked'):
            queue.guard_effect(req.session, req.operation_id, 0, lambda: called.append('effect'))
        assert called == []


@pytest.mark.parametrize(('held', 'allowed'), [(True, True), (False, False)],
                         ids=['held-only', 'eligible-input'])
def test_followup_reservation_fences_only_eligible_unadmitted_input(tmp_path, held, allowed):
    repo,req,queue,clock=ready(tmp_path)
    unadmitted(repo, req, 'reserve-input', held=held)

    if allowed:
        binding=queue.reserve_followup(req.session, req.operation_id, 0, POLICY, 'followup',
                                       deadline_ms=clock[0]+60000)
        assert binding['job_id'] == 'followup'
    else:
        with pytest.raises(ContractError, match='follow-up revoked'):
            queue.reserve_followup(req.session, req.operation_id, 0, POLICY, 'followup',
                                   deadline_ms=clock[0]+60000)


@pytest.mark.parametrize(('held', 'allowed'), [(True, True), (False, False)],
                         ids=['held-only', 'eligible-input'])
def test_followup_completion_fences_only_eligible_unadmitted_input(tmp_path, held, allowed):
    repo,req,queue,clock=ready(tmp_path)
    binding=queue.reserve_followup(req.session, req.operation_id, 0, POLICY, 'followup',
                                   deadline_ms=clock[0]+60000)
    unadmitted(repo, req, 'finish-input', held=held)

    if allowed:
        queue.finish_followup(req.session, req.operation_id, binding, 'comparison result')
        with repo._read() as db:
            row=db.execute('SELECT state,output_text FROM runtime_learning_calls WHERE job_id=?',
                           ('followup',)).fetchone()
        assert tuple(row) == ('complete', 'comparison result')
    else:
        with pytest.raises(ContractError, match='follow-up revoked'):
            queue.finish_followup(req.session, req.operation_id, binding, 'comparison result')
        with repo._read() as db:
            row=db.execute('SELECT state FROM runtime_learning_calls WHERE job_id=?',
                           ('followup',)).fetchone()
        assert row['state'] == 'claimed'


def test_result_handoff_survives_reopen_and_duplicate_without_backend_replay(tmp_path):
    repo,snapshot,req,_=source(tmp_path);backend=Backend()
    RuntimeService(repo,backend).execute(snapshot,req)
    queue=RuntimeLearningQueue(RuntimeRepository(repo.root))
    row=queue.get(req.session,req.operation_id)
    assert row['state']=='awaiting-delivery' and row['result_digest']
    assert 'input_text' not in json.loads(row['source_json'])
    assert 'output_text' not in json.loads(row['facts_json'])
    before=backend.calls
    RuntimeService(repo,backend).execute(repo.read_current('session'),req)
    assert backend.calls==before
    assert sum(queue.summary(req.session)['counts'].values())==1
    assert queue.summary(req.session)['model_dispatch_configured'] is False


def test_handoff_rolls_back_with_terminal_result_commit(tmp_path):
    repo,snapshot,req,_=source(tmp_path);repo.begin(snapshot,req)
    result=accept_observation(req,AgentTurnObservation(req,'completed','fixture',1,1,output_text='answer'))
    repo.seal_observation(req,result)
    with pytest.raises(WorkingRepositoryError): repo.finish(req,result,fail_before_commit=True)
    queue=RuntimeLearningQueue(repo)
    assert repo.read_turn(req.session,req.operation_id).status=='submitted'
    row=queue.get(req.session,req.operation_id)
    assert row['state']=='awaiting-result' and row['result_digest'] is None
    repo.finish(req,result)
    assert queue.get(req.session,req.operation_id)['state']=='awaiting-delivery'


@pytest.mark.parametrize('delivery',['pending','unknown','sent'])
def test_only_confirmed_delivery_can_claim_background_work(tmp_path,delivery):
    repo,snapshot,req,route=source(tmp_path);accepted(repo,snapshot,req)
    if delivery!='pending': delivered(repo,req,route,unknown=delivery=='unknown')
    queue=RuntimeLearningQueue(repo)
    claim=queue.claim(req.session,req.operation_id,'owner',POLICY)
    assert (claim is not None)==(delivery=='sent')
    if claim:
        assert claim['evidence_scope']=='delivered-answer'
        assert claim['deadline_ms']>int(time.time()*1000)+50000
        assert claim['max_tokens']==100 and claim['model']=='gpt-6-luna' and claim['effort']=='max'
        assert claim['facts']['answer_adoption_verified'] is False


def test_missing_budget_or_foreign_scope_has_no_claim_effect(tmp_path):
    _,req,queue,_=ready(tmp_path)
    with pytest.raises(ContractError): queue.claim(req.session,req.operation_id,'owner',None)
    foreign=replace(req.session,route_id='foreign')
    with pytest.raises(ContractError): queue.get(foreign,req.operation_id)
    with pytest.raises(ContractError): queue.claim(foreign,req.operation_id,'owner',POLICY)
    assert queue.get(req.session,req.operation_id)['state']=='awaiting-delivery'


def test_claim_is_once_only_and_restart_never_replays_model(tmp_path):
    repo,req,queue,_=ready(tmp_path)
    queue.claim(req.session,req.operation_id,'owner',POLICY)
    with pytest.raises(ContractError): queue.claim(req.session,req.operation_id,'owner',POLICY)
    reopened=RuntimeLearningQueue(RuntimeRepository(repo.root))
    reopened.reconcile(req.session)
    assert reopened.get(req.session,req.operation_id)['state']=='unknown'
    with pytest.raises(ContractError): reopened.claim(req.session,req.operation_id,'new-owner',POLICY)


@pytest.mark.parametrize('failure',['late','wrong-model','wrong-effort'])
def test_late_or_wrong_route_receipt_remains_unknown(tmp_path,failure):
    _,req,queue,clock=ready(tmp_path)
    claim=queue.claim(req.session,req.operation_id,'owner',POLICY)
    if failure=='late': clock[0]=claim['deadline_ms']
    result=queue.finish(req.session,req.operation_id,'owner',{'action':'no-op'},
        model='wrong' if failure=='wrong-model' else 'gpt-6-luna',
        effort='low' if failure=='wrong-effort' else 'max')
    assert result=={'state':'unknown','replay_allowed':False}


def test_stop_generation_fences_inflight_result_and_future_claim(tmp_path):
    _,req,queue,_=ready(tmp_path)
    queue.claim(req.session,req.operation_id,'owner',POLICY)
    queue.stop(req.session,req.operation_id)
    assert queue.get(req.session,req.operation_id)['state']=='cancelled'
    with pytest.raises(ContractError):
        queue.finish(req.session,req.operation_id,'owner',{'action':'no-op'},model='gpt-6-luna',effort='max')
    with pytest.raises(ContractError): queue.claim(req.session,req.operation_id,'new-owner',POLICY)


def test_expired_backlog_has_an_explicit_receipt_without_claim(tmp_path):
    _,req,queue,clock=ready(tmp_path);clock[0]+=RETENTION_MS+1
    assert queue.claim(req.session,req.operation_id,'owner',POLICY) is None
    assert queue.get(req.session,req.operation_id)['state']=='expired'


def test_noop_receipt_does_not_claim_benefit_or_activate_skill(tmp_path):
    _,req,queue,_=ready(tmp_path);queue.claim(req.session,req.operation_id,'owner',POLICY)
    result=queue.finish(req.session,req.operation_id,'owner',{'action':'no-op','reason':'no supported lesson'},
                        model='gpt-6-luna',effort='max')
    assert result=={'state':'complete','automatic_activation':False,'benefit':'unmeasured'}
    with pytest.raises(ContractError):
        queue.finish(req.session,req.operation_id,'owner',{'action':'propose'},model='gpt-6-luna',effort='max')


def test_backlog_full_is_recorded_without_changing_user_result(tmp_path,monkeypatch):
    monkeypatch.setattr(module,'BACKLOG_LIMIT',0)
    repo,snapshot,req,_=source(tmp_path)
    result=accepted(repo,snapshot,req)
    assert result.result['accepted'] is True
    assert RuntimeLearningQueue(repo).get(req.session,req.operation_id)['state']=='dropped'


def test_no_learning_capability_creates_no_source(tmp_path):
    repo,snapshot,req,_=state(tmp_path);accepted(repo,snapshot,req)
    assert RuntimeLearningQueue(repo).get(req.session,req.operation_id) is None


def test_failed_execution_can_learn_only_its_delivered_failure_facts(tmp_path):
    repo,snapshot,req,route=source(tmp_path)
    class Failed:
        def invoke(self,request):
            return AgentTurnObservation(request,'unknown','transport-loss',1,0,unresolved_effects=True)
    RuntimeService(repo,Failed()).execute(snapshot,req);delivered(repo,req,route)
    claim=RuntimeLearningQueue(repo).claim(req.session,req.operation_id,'owner',POLICY)
    assert claim['evidence_scope']=='execution-failure-and-delivered-report'
    assert claim['facts']['accepted'] is False and claim['facts']['unresolved_effects'] is True


def test_daily_budget_and_serial_claims_are_shared_across_session_generations(tmp_path):
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.domain.channel_ingress import ChannelRoute, ChannelInboundEvent
    from ahc.domain.sessions import VirtualSessionIdentity
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.infrastructure.runtime_inbox import RuntimeInbox
    repo,snapshot,req,route=source(tmp_path);accepted(repo,snapshot,req);delivered(repo,req,route)
    queue=RuntimeLearningQueue(repo);policy=ReflectionPolicy(60,100,100,1)
    first=queue.claim(req.session,req.operation_id,'first-owner',policy)
    second_session=VirtualSessionIdentity.initial('new-session','agent','route')
    second_snapshot=WorkingSetSnapshot.initial(second_session,snapshot.goal);repo.create(second_snapshot)
    second_route=ChannelRoute('telegram','account','channel','user',second_session)
    event=ChannelInboundEvent(second_route,'event-two','next question')
    ChannelIngress(RuntimeInbox(repo),second_route).submit(event,snapshot.goal.digest)
    second_req=replace(req,session=second_session,operation_id=event.operation_id,input_text=event.input_text)
    accepted(repo,second_snapshot,second_req);delivered(repo,second_req,second_route)
    assert queue.get(req.session,req.operation_id)['scope_key']==queue.get(second_session,second_req.operation_id)['scope_key']
    assert queue.claim(second_session,second_req.operation_id,'second-owner',policy) is None
    queue.finish(req.session,req.operation_id,'first-owner',{'action':'no-op'},model='gpt-6-luna',effort='max')
    assert queue.claim(second_session,second_req.operation_id,'second-owner',policy) is None
    with repo._read() as db:
        usage=db.execute('SELECT tokens,calls FROM runtime_learning_usage').fetchall()
    assert [(r[0],r[1]) for r in usage]==[(100,1)]


def test_source_cancellation_is_durable_before_any_background_claim(tmp_path):
    repo,snapshot,req,_=source(tmp_path);repo.begin(snapshot,req)
    repo.finish(req,{'disposition':'not-submitted','accepted':False,'unresolved_effects':False,
                     'reason':'cancelled','output_text':''})
    queue=RuntimeLearningQueue(repo)
    assert queue.get(req.session,req.operation_id)['state']=='cancelled'
    with pytest.raises(ContractError): queue.claim(req.session,req.operation_id,'owner',POLICY)
