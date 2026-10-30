"""Regression evidence for the independently reviewed M1 effect boundaries."""
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_workspace_jobs import owned, call, start
from test_codex_workspace_tools import bridge, message
from test_wsl_python_workspace import IDENTITY

from ahc.adapters.workspace_batch_callback import receive_reply
from ahc.domain.workspace_job_lease import WorkspaceJobLease
from ahc.infrastructure import workspace_job_store as receipts


def test_cancel_receipt_failure_after_request_is_unknown(owned, monkeypatch):
    _, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    def lost_receipt(*args, **kwargs):
        raise OSError('receipt unavailable')
    monkeypatch.setattr(receipts, 'update_cancellation', lost_receipt)
    result = call(tools, 'cancel')
    assert result['disposition'] == 'unknown'
    assert result['unresolved_effects'] is True
    assert workspace.jobs._workers['work1']['lease'].cancel_requested
    assert workspace.port.closed


def callback_workspace(lease, run):
    replies = iter([{'id':1, 'batch':[{'call_id':'one', 'capability_id':'read',
                    'resource_id':'runtime', 'arguments':{}}], 'concurrency':1},
                    {'id':1, 'ok':True}])
    sends = []
    workspace = SimpleNamespace(_sequence=1, _job_lease=lease,
        _batch=SimpleNamespace(session=IDENTITY.session, run=run), identity=IDENTITY,
        _cancelled=lambda:False, _receive=lambda deadline:next(replies),
        _transport=SimpleNamespace(send=lambda payload, **kwargs:sends.append(payload)))
    return workspace, sends


def test_callback_dispatch_rejects_cancelled_job_before_batch():
    now=time.monotonic(); lease=WorkspaceJobLease(now,now+100,50)
    lease.request_cancel()
    calls=[]
    def run(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(summary=lambda:{}, values=[])
    workspace,sends=callback_workspace(lease,run)
    with pytest.raises(Exception):
        receive_reply(workspace, {'operation':'execute'}, now+100)
    assert calls == [] and sends == []


@pytest.mark.parametrize('stop', ['cancel', 'expire'])
def test_callback_inherits_job_deadline_and_suppresses_reply_after_stop(stop):
    now=time.monotonic(); lease=WorkspaceJobLease(now,now+100,50)
    seen=[]
    def run(*args, **kwargs):
        seen.append(kwargs)
        if stop == 'cancel': lease.request_cancel()
        else: lease.quiet_deadline=time.monotonic()-1
        return SimpleNamespace(summary=lambda:{}, values=[])
    workspace,sends=callback_workspace(lease,run)
    with pytest.raises(Exception):
        receive_reply(workspace, {'operation':'execute'}, now+100)
    assert seen[0]['deadline'] <= now+50
    if stop == 'cancel': assert seen[0]['cancelled']() is True
    assert sends == []


def test_native_cancel_keeps_only_exact_job_observation_available():
    adapter,tools=bridge()
    adapter.request=replace(adapter.request,capabilities=('workspace.job','workspace.execute'))
    identity=tools.workspace.identity.to_dict()
    invoked=[]
    def invoke(capability, arguments):
        invoked.append(arguments)
        return {'disposition':'observed','unresolved_effects':True,'result':{
            'identity':identity,'job_identity':identity,'job_id':'admitted',
            'status':'cancelled','cancellation':{'cancel_requested':True,
            'execution_stopped':False,'effects_unknown':True}}}
    tools.invoke=invoke
    def request(index,action,job_id='admitted'):
        value=message(); value['id']=index; value['params']['callId']=f'call{index}'
        value['params']['tool']='job'; value['params']['arguments']={
            'expected':identity,'action':action,'job_id':job_id}
        response=adapter.respond(value)
        return json.loads(response['result']['contentItems'][0]['text'])
    cancelled=request(11,'cancel')
    assert cancelled['result']['cancellation']['execution_stopped'] is False
    assert 'operation is stopped' not in cancelled['reporting']['instruction']
    assert request(12,'status')['disposition'] == 'observed'
    assert request(13,'wait')['disposition'] == 'observed'
    assert request(14,'start')['reason'] == 'problem-report-only'
    assert request(15,'status','other')['reason'] == 'problem-report-only'
    assert len(invoked)==3


def test_cancel_commit_then_receipt_loss_still_signals_exact_worker(owned,monkeypatch):
    _,workspace,tools=owned
    start(tools); assert workspace.port.entered.wait(1)
    original=receipts.finish
    def commit_then_lose(*args,**kwargs):
        value=original(*args,**kwargs)
        if kwargs.get('state')=='cancelled': raise OSError('reply lost after commit')
        return value
    monkeypatch.setattr(receipts,'finish',commit_then_lose)
    result=call(tools,'cancel')
    assert result['disposition']=='unknown' and result['unresolved_effects']
    slot=workspace.jobs._workers['work1']
    assert slot['lease'].cancel_requested
    assert slot['done'].wait(1)


def test_malformed_job_observation_never_opens_reporting_authority():
    adapter,_=bridge()
    adapter.reporting=True
    for arguments in [{'action':'status','job_id':[], 'expected':{}},
                      {'action':'status','job_id':'one','expected':{'bad':float('nan')}}]:
        assert adapter._is_job_observation('workspace.job',arguments) is False


def test_unknown_job_without_worker_cancel_keeps_unknown_effects(owned):
    _,workspace,tools=owned
    start(tools); assert workspace.port.entered.wait(1)
    old=workspace.jobs
    from ahc.application.workspace_jobs import WorkspaceJobs
    workspace.jobs=WorkspaceJobs(workspace)
    try:
        result=call(tools,'cancel')
        assert result['disposition']=='unknown' and result['unresolved_effects']
        assert not workspace.port.closed
    finally:
        workspace.jobs=old
        workspace.port.release.set()


def test_native_first_admission_observation_loss_allows_exact_status(owned,monkeypatch):
    _,workspace,tools=owned
    adapter,_=bridge()
    from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
    request=replace(adapter.request,session=workspace.identity.session,
                    capabilities=('workspace.job','workspace.execute'))
    native=CodexWorkspaceTools(request,tools,'thread','turn',clock=lambda:0)
    original=workspace.jobs.observe; count=[0]
    def first_loss(*args,**kwargs):
        count[0]+=1
        if count[0]==1: raise OSError('first result lost')
        return original(*args,**kwargs)
    monkeypatch.setattr(workspace.jobs,'observe',first_loss)
    def invoke(index,action,**fields):
        value=message();value['id']=index;value['params']['callId']=f'first{index}'
        value['params']['tool']='job';value['params']['arguments']={
            'expected':workspace.identity.to_dict(),'action':action,'job_id':'work1',**fields}
        value=native.respond(value)
        return json.loads(value['result']['contentItems'][0]['text'])
    started=invoke(31,'start',code='count-once',expected_seconds=10)
    assert started['disposition']=='unknown' and started['unresolved_effects']
    assert started['result']['job_id']=='work1'
    assert invoke(32,'status')['disposition']=='observed'
    assert invoke(33,'start',code='count-once',expected_seconds=10)['reason']=='problem-report-only'
