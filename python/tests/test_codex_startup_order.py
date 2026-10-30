"""Early thread-scoped status waits for authoritative thread/start binding."""
import pytest
from test_codex_client import setup


def mcp(thread='thread', **changes):
    return {'method':'mcpServer/startupStatus/updated','params':{
        'threadId':thread,'name':'fixture','status':'starting','error':None,
        'failureReason':None,**changes}}


@pytest.mark.parametrize('event', [
    mcp(),
    {'method':'thread/status/changed','params':{'threadId':'thread','status':{'type':'idle'}}},
])
@pytest.mark.parametrize('started_first', [False,True])
def test_early_status_binds_after_thread_response_before_any_turn(event, started_first):
    peer,client,request=setup()
    early=[event]
    if started_first:
        early.insert(0,{'method':'thread/started','params':{'thread':{'id':'thread'}}})
    peer.messages[1:1]=early
    result=client.run_admitted(request)
    assert result.disposition=='completed' and result.output_text=='answer'
    assert sum(m.get('method')=='turn/start' for m in peer.sent)==1


@pytest.mark.parametrize('event', [mcp('foreign'),mcp(status='approved'),
    mcp(thread=3),mcp(extra='unexpected'),
])
def test_unusable_early_mcp_advisory_does_not_prevent_model_submission(event):
    peer,client,request=setup()
    peer.messages.insert(1,event)
    assert client.run_admitted(request).disposition=='completed'
    assert sum(m.get('method')=='turn/start' for m in peer.sent)==1


def test_early_settings_cannot_bypass_permission_readback():
    peer,client,request=setup()
    settings=dict(peer.messages[1]['result'])
    settings.pop('thread')
    settings['sandboxPolicy']={'type':'dangerFullAccess'}
    settings.pop('sandbox')
    peer.messages.insert(1,{'method':'thread/settings/updated','params':{
        'threadId':'thread','threadSettings':settings}})
    assert client.run_admitted(request).disposition=='not-submitted'
    assert not any(m.get('method')=='turn/start' for m in peer.sent)


def test_discarded_startup_metadata_has_no_independent_eight_event_quota():
    peer,client,request=setup()
    peer.messages[1:1]=[mcp() for _ in range(9)]
    assert client.run_admitted(request).disposition=='completed'
    assert sum(m.get('method')=='turn/start' for m in peer.sent)==1


def test_global_mcp_status_before_thread_has_no_thread_authority():
    peer,client,request=setup()
    peer.messages.insert(1,mcp(None))
    result=client.run_admitted(request)
    assert result.disposition=='completed' and result.backend_thread=='thread'
