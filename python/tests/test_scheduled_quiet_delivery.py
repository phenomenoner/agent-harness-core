"""Scheduled silence is durable and cannot swallow an interactive reply."""
from dataclasses import replace
from types import SimpleNamespace
import threading

import pytest
from test_channel_background_presentation import scheduled
from test_runtime_core import Backend
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def answer(text, cleanup_pending=False):
    return SimpleNamespace(invoke=lambda request: AgentTurnObservation(
        request, 'completed', 'controlled', 1, 1, output_text=text,
        cleanup_pending=cleanup_pending))


@pytest.mark.parametrize('platform', ['telegram', 'discord'])
def test_exact_sentinel_is_quiet_durable_and_does_not_swallow_next_reply(tmp_path, platform):
    store, current, request, route = scheduled(tmp_path, platform)
    RuntimeService(store, answer('NO_REPLY')).execute(current, request)
    sent=[]
    delivery=ChannelDelivery(ChannelOutbox(store), route, SimpleNamespace(
        send=lambda actual_route, payload, operation: sent.append(payload) or 'wire-receipt'))
    result=delivery.deliver(request.operation_id, 'quiet-claim')
    assert sent == []
    assert result['state'] == 'sent'
    assert result['receipt'] == 'suppressed:scheduled:no-reply'
    reopened=RuntimeRepository(store.root)
    outbox=ChannelOutbox(reopened)
    row=outbox.read(route)[0]
    assert row['payload']['output_text'] == 'NO_REPLY'
    assert row['receipt'] == 'suppressed:scheduled:no-reply'
    assert reopened.read_turn(current.session, request.operation_id).result['output_text'] == 'NO_REPLY'
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, SimpleNamespace(send=lambda *args: pytest.fail('replayed'))).deliver(
            request.operation_id, 'never-replay')
    event=ChannelInboundEvent(route, 'following-user', 'What changed?')
    RuntimeInbox(reopened).submit(current.session, event.operation_id, current.goal.digest,
        event.input_text, channel_event=event)
    following=replace(request,operation_id=event.operation_id,input_text=event.input_text)
    RuntimeService(reopened, answer('NO_REPLY')).execute(reopened.read_current('session'), following)
    ChannelDelivery(outbox, route, SimpleNamespace(send=lambda actual_route, payload, operation:
        sent.append(payload) or 'wire-receipt')).deliver(following.operation_id, 'interactive-claim')
    assert sent == [{'output_text':'💬 **回覆**\n\nNO_REPLY'}]


@pytest.mark.parametrize('text', ['New version available', 'NO_REPLY\nmore', ' NO_REPLY', 'NO_REPLY\n'])
def test_scheduled_substantive_or_nonexact_text_is_delivered(tmp_path, text):
    store,current,request,route=scheduled(tmp_path)
    RuntimeService(store, answer(text)).execute(current,request)
    sent=[]
    result=ChannelDelivery(ChannelOutbox(store),route,SimpleNamespace(send=lambda actual_route,payload,operation:
        sent.append(payload) or 'wire-receipt')).deliver(request.operation_id,'notice-claim')
    assert result['receipt']=='wire-receipt'
    assert sent==[{'output_text':'⏰ **定時任務**\n\n'+text}]


def test_cleanup_notice_is_never_hidden_by_sentinel(tmp_path):
    store,current,request,route=scheduled(tmp_path)
    RuntimeService(store,answer('NO_REPLY',cleanup_pending=True)).execute(current,request)
    sent=[]
    ChannelDelivery(ChannelOutbox(store),route,SimpleNamespace(send=lambda actual_route,payload,operation:
        sent.append(payload) or 'wire-receipt')).deliver(request.operation_id,'cleanup-claim')
    assert len(sent)==1 and sent[0]['output_text'].startswith('⏰ **定時任務**\n\nNO_REPLY\n')


def test_native_background_owner_quiet_completion_allows_foreground_followup(tmp_path):
    from test_channel_controls import make
    from test_background_schedule_runtime import occurrence
    from ahc.application.background_schedule import BackgroundSchedule
    from ahc.application.foreground_runtime import ForegroundRuntime
    from ahc.application.local_runtime_ingress import LocalRuntimeIngress
    store,current,template,route,controls,_,submit=make(tmp_path)
    occurrence(store,current,submit)
    done=threading.Event()
    messages=[]
    receipts=[]
    def factory(binding):
        worker_route=replace(route,session=binding['worker_session'])
        ingress=LocalRuntimeIngress(RuntimeInbox(store),artifact_digest='a'*64,
            configuration_digest=lambda tools:'b'*64,workspace_scope=binding['workspace_id'],
            turn_timeout=30,effective_policy=template.effective_policy)
        outbox=ChannelOutbox(store)
        delivery=ChannelDelivery(outbox,worker_route,SimpleNamespace(send=lambda destination,payload,operation:
            messages.append(payload) or 'wire-receipt'))
        def drain(*,cancelled):
            for row in outbox.read(worker_route):
                if row['state']=='pending':
                    receipts.append(delivery.deliver(row['operation_id'],'background-claim'))
                    done.set()
                    return
        return ForegroundRuntime(store,binding['worker_session'],ingress,
            lambda *args,**kwargs:answer('NO_REPLY'),channel_delivery=drain)
    manager=BackgroundSchedule(store,current.session,factory,tick=lambda current:None,interval=0.01)
    manager.start()
    try:
        assert done.wait(10),manager.status()
        assert messages==[]
        assert receipts[0]['receipt']=='suppressed:scheduled:no-reply'
        event=submit('foreground after silent result','quiet-followup')
        request=replace(template,operation_id=event.operation_id,input_text=event.input_text)
        assert RuntimeService(store,Backend()).execute(current,request).result['accepted']
        ChannelDelivery(ChannelOutbox(store),route,SimpleNamespace(send=lambda destination,payload,operation:
            messages.append(payload) or 'wire-receipt')).deliver(request.operation_id,'followup-claim')
        assert messages==[{'output_text':'💬 **回覆**\n\nanswer'}]
    finally:
        manager.close()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("direct_first", [False, True])
def test_scheduled_artifact_silence_and_interactive_followup(
        tmp_path, monkeypatch, platform, direct_first):
    from test_workspace_tools import Port
    from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
    from ahc.cli import foreground_channels
    from ahc.domain.result_artifacts import ResultArtifact
    from ahc.infrastructure.artifact_repository import ArtifactRepository
    from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox

    store, current, request, route = scheduled(tmp_path, platform)
    workspace = RuntimeService(store, None).open_workspace(current, "quiet-artifact", Port)
    text_sends, file_sends = [], []
    try:
        manifest = ArtifactRepository(store).publish(workspace.identity, b"kept-report", "text/plain")
        selection = ResultArtifact(manifest.artifact_id, "report.txt")
        def invoke(actual):
            return AgentTurnObservation(actual, "completed", "controlled", 1, 1,
                                        output_text="NO_REPLY", artifacts=(selection,))
        backend = SimpleNamespace(invoke=invoke)
        assert RuntimeService(store, backend).execute(current, request).result["accepted"]
        monkeypatch.setenv("AHC_QUIET_FIXTURE", "fixture")
        text_transport = SimpleNamespace(send=lambda *args: text_sends.append(args) or "wire-text")
        file_transport = SimpleNamespace(send=lambda upload: file_sends.append(upload) or
                                         {"message_id": "1", "attachment_id": "2"})
        monkeypatch.setattr(foreground_channels, "BotTextTransport", lambda *a: text_transport)
        monkeypatch.setattr(foreground_channels, "BotAttachmentTransport", lambda *a: file_transport)
        config = SimpleNamespace(to_dict=lambda: {"channels": [
            {"route": route.to_dict(), "enabled": True, "token_env": "AHC_QUIET_FIXTURE"}]})
        if direct_first:
            ChannelAttachmentDelivery(ChannelAttachmentOutbox(store), route, file_transport).deliver(
                request.operation_id, 0, "direct-quiet")
        drain = foreground_channels.compose_channels(config, store)
        for _ in range(3):
            drain(cancelled=lambda: False)
        assert text_sends == [] and file_sends == []
        saved = ChannelAttachmentOutbox(store).read(route)[0]
        assert saved["state"] == "sent" and saved["receipt"] == "suppressed:scheduled:no-reply"
        assert ChannelOutbox(store).read(route)[0]["payload"]["output_text"] == "NO_REPLY"
        reopened = RuntimeRepository(store.root)
        foreground_channels.compose_channels(config, reopened)(cancelled=lambda: False)
        assert ChannelAttachmentOutbox(reopened).read(route)[0] == saved
        assert not file_sends

        event = ChannelInboundEvent(route, "ordinary-after-quiet", "report")
        RuntimeInbox(store).submit(current.session, event.operation_id, current.goal.digest,
                                   event.input_text, channel_event=event)
        next_request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
        actual = store.read_current(current.session.ref.virtual_session_id)
        assert RuntimeService(store, backend).execute(actual, next_request).result["accepted"]
        for _ in range(3):
            drain(cancelled=lambda: False)
        assert len(text_sends) == len(file_sends) == 1
        assert "NO_REPLY" in text_sends[0][1]["output_text"]
        assert file_sends[0].filename == "report.txt" and b"kept-report" in file_sends[0].body
    finally:
        workspace.close()


def test_silent_result_keeps_preexisting_unknown_attachment(tmp_path):
    from test_workspace_tools import Port
    from ahc.domain.result_artifacts import ResultArtifact
    from ahc.infrastructure.artifact_repository import ArtifactRepository
    from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox

    store, current, request, route = scheduled(tmp_path)
    workspace = RuntimeService(store, None).open_workspace(current, "unknown-artifact", Port)
    try:
        manifest = ArtifactRepository(store).publish(workspace.identity, b"report", "text/plain")
        backend = SimpleNamespace(invoke=lambda actual: AgentTurnObservation(
            actual, "completed", "controlled", 1, 1, output_text="NO_REPLY",
            artifacts=(ResultArtifact(manifest.artifact_id, "report.txt"),)))
        RuntimeService(store, backend).execute(current, request)
        outbox = ChannelAttachmentOutbox(store)
        outbox.claim(route, request.operation_id, 0, "lost-claim")
        unknown = outbox.mark_unknown(route, request.operation_id, 0, "lost-claim")
        ChannelDelivery(ChannelOutbox(store), route, SimpleNamespace(
            send=lambda *args: pytest.fail("silent result sent"))).deliver(request.operation_id, "quiet")
        reopened = ChannelAttachmentOutbox(RuntimeRepository(store.root))
        assert reopened.reconcile(route) == (unknown,)
        with pytest.raises(ContractError):
            reopened.claim(route, request.operation_id, 0, "no-replay")
    finally:
        workspace.close()
