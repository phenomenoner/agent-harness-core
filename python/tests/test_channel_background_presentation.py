"""Background completion stays quiet until one typed result, then chat resumes."""

import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_core import Backend, setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_progress import ChannelProgress
from ahc.application.pending_approval import PendingApprovalPort
from ahc.application.runtime_service import RuntimeService
from ahc.cli import foreground_typing
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_approval import ApprovalDecision
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.channel_schedule_commands import handle_schedule_command
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def scheduled(tmp_path, platform="telegram"):
    store, current, template = setup(tmp_path)
    route = ChannelRoute(platform, "11", "22", "33", current.session)
    reply = store._mutate(lambda c: handle_schedule_command(
        SimpleNamespace(store=store), c, route, "/cron add every 10m check progress",
        now_ms=600_000))
    schedule_id = reply.partition("排程 ")[2].partition("：")[0]
    value = ScheduleRepository(store).read(current.session, schedule_id)
    operation = ScheduleTick(store).tick(value, 1_200_000)["occurrences"][0]["operation_id"]
    request = replace(template, operation_id=operation, input_text="check progress",
                      deadline=time.monotonic() + 30)
    return store, current, request, route


def config(route):
    return SimpleNamespace(to_dict=lambda: {"channels": [{"route": route.to_dict(),
        "typing": True, "progress": True, "token_env": "AHC_BACKGROUND_FIXTURE"}]})


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_scheduled_turn_never_constructs_activity_or_sends_explicit_progress(
        tmp_path, monkeypatch, platform):
    store, current, request, route = scheduled(tmp_path, platform)
    monkeypatch.setenv("AHC_BACKGROUND_FIXTURE", "fixture")
    factory = foreground_typing.compose_typing(config(route), store)
    sent = []
    with store.execution_lock():
        store.begin(current, request)
        assert factory(request, lambda: False) is None
        with store.workspace_turn_scope(request):
            progress = ChannelProgress(store, request, route,
                SimpleNamespace(send=lambda *args: sent.append(args)))
            assert progress.send("progress", "intermediate commentary")["status"] == "suppressed"
        assert ChannelProgressStore(store).reserve(
            request, route, "activity-status", "Working", "claim", 1000) is False
    assert sent == [] and ChannelProgressStore(store).read(route) == ()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_background_result_then_user_reply_retains_types_and_raw_history(
        tmp_path, monkeypatch, platform):
    store, current, request, route = scheduled(tmp_path, platform)
    RuntimeService(store, Backend()).execute(current, request)
    # Presentation must survive reopen and removal of the originating schedule.
    with store._read() as c:
        name = c.execute("SELECT schedule_id FROM runtime_schedule_occurrences "
                         "WHERE operation_id=?", (request.operation_id,)).fetchone()[0]
    repo = ScheduleRepository(store)
    repo.set_state(repo.read(current.session, name), "removed")
    store = RuntimeRepository(store.root)
    sent, wire = [], []
    def post(actual_platform, token, channel, body):
        wire.append(body)
        if actual_platform == "telegram":
            return {"ok": True, "result": {"message_id": len(wire),
                "chat": {"id": 22}, "from": {"id": 11}}}
        return {"id": str(len(wire)), "channel_id": "22", "author": {"id": "11"}}
    platform_transport = BotTextTransport(route, "fixture", post=post)
    def send(actual_route, payload, operation):
        sent.append((payload, operation))
        return platform_transport.send(actual_route, payload, operation)
    transport = SimpleNamespace(send=send)
    outbox = ChannelOutbox(store)
    ChannelDelivery(outbox, route, transport).deliver(request.operation_id, "scheduled-claim")
    assert sent[0][0]["output_text"] == "⏰ **定時任務**\n\nanswer"
    if platform == "telegram":
        assert wire[0]["text"] == "⏰ <b>定時任務</b>\n\nanswer"
        assert wire[0]["parse_mode"] == "HTML"
    else:
        assert wire[0]["content"] == "⏰ **定時任務**\n\nanswer"
        assert wire[0]["allowed_mentions"] == {"parse": []}
    assert outbox.read(route)[0]["payload"] == {"output_text": "answer"}
    assert store.read_turn(current.session, request.operation_id).result["output_text"] == "answer"
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, transport).deliver(request.operation_id, "no-replay")
    event = ChannelInboundEvent(route, "update-next", "What remains?")
    RuntimeInbox(store).submit(current.session, event.operation_id, current.goal.digest,
                               event.input_text, channel_event=event)
    following = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    monkeypatch.setenv("AHC_BACKGROUND_FIXTURE", "fixture")
    # A long interactive request still receives its ordinary interactive worker.
    factory = foreground_typing.compose_typing(config(route), store)
    assert factory(following, lambda: False) is not None
    RuntimeService(store, Backend()).execute(store.read_current("session"), following)
    ChannelDelivery(outbox, route, transport).deliver(following.operation_id, "reply-claim")
    assert sent[-1][0]["output_text"] == "💬 **回覆**\n\nanswer" and len(sent) == 2


def test_notification_only_approval_does_not_publish_prompt_or_wait():
    def forbidden(*args, **kwargs):
        pytest.fail("background approval must not prompt or wait for chat interaction")
    port = PendingApprovalPort(SimpleNamespace(publish=forbidden), SimpleNamespace(),
                               notifier=forbidden, wait=forbidden, interactive=False)
    assert port.decide(SimpleNamespace(digest="a" * 64), cancelled=lambda: False) == (
        ApprovalDecision("a" * 64, "unavailable"))


def test_failed_background_notice_keeps_unknown_delivery_and_never_replays(tmp_path):
    store, current, request, route = scheduled(tmp_path)
    backend = SimpleNamespace(invoke=lambda bound: AgentTurnObservation(
        bound, "unknown", "controlled failure", 1, 0, unresolved_effects=True))
    RuntimeService(store, backend).execute(current, request)
    messages = []
    def send(route, payload, operation):
        messages.append(payload["output_text"])
        raise TimeoutError("unconfirmed send")
    delivery = ChannelDelivery(ChannelOutbox(store), route, SimpleNamespace(send=send))
    assert delivery.deliver(request.operation_id, "claim")["state"] == "unknown"
    assert messages[0].startswith("⏰ **定時任務提醒**\n")
    with pytest.raises(ContractError):
        delivery.deliver(request.operation_id, "never-retry")
    assert len(messages) == 1


def test_user_message_during_background_run_cannot_steer_the_scheduled_model(tmp_path):
    from test_channel_controls import make

    store, current, template, route, controls, _, submit = make(tmp_path)
    submit("/messages steer")
    submit("/cron add every 10m background check")
    with store._read() as c:
        name = c.execute("SELECT schedule_id FROM runtime_schedules").fetchone()[0]
    value = ScheduleRepository(store).read(current.session, name)
    due = value.start_ms + value.interval_ms
    operation = ScheduleTick(store).tick(value, due)["occurrences"][0]["operation_id"]
    request = replace(template, operation_id=operation, input_text=value.input_text)
    with store.execution_lock():
        store.begin(current, request)
        event = submit("Please answer my separate question")
    with store._read() as c:
        assert c.execute("SELECT 1 FROM channel_steering WHERE operation_id=?",
                         (event.operation_id,)).fetchone() is None
        text = c.execute("SELECT payload_json FROM runtime_submissions WHERE operation_id=?",
                         (event.operation_id,)).fetchone()[0]
        assert event.input_text in text
        assert c.execute("SELECT reply FROM channel_controls WHERE operation_id=?",
                         (event.operation_id,)).fetchone()[0].startswith("🔔 **任務提醒**")


def test_foreground_loop_runs_quiet_schedule_then_interactive_continuation(
        tmp_path, monkeypatch):
    from ahc.application.foreground_runtime import ForegroundRuntime
    from ahc.application.local_runtime_ingress import LocalRuntimeIngress
    from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository

    store, current, request, route = scheduled(tmp_path)
    store = ThreadedRuntimeRepository(store.root)
    created, finals = [], []
    ready = threading.Event()
    def create(text, operation):
        created.append((operation, text))
        ready.set()
        return "activity-receipt"
    monkeypatch.setenv("AHC_BACKGROUND_FIXTURE", "fixture")
    monkeypatch.setattr(foreground_typing, "BotTypingTransport", lambda *a:
                        SimpleNamespace(send=lambda *a: None))
    monkeypatch.setattr(foreground_typing, "BotActivityTransport", lambda *a:
                        SimpleNamespace(create=create, update=lambda text: None))
    outbox = ChannelOutbox(store)
    delivery = ChannelDelivery(outbox, route, SimpleNamespace(send=lambda r, payload, op:
        finals.append(payload["output_text"]) or "final-receipt"))
    def deliver(*, cancelled):
        for row in outbox.read(route):
            if row["state"] == "pending":
                delivery.deliver(row["operation_id"], "claim-" + str(len(finals)))
    class Backend:
        def invoke(self, bound):
            if bound.operation_id == request.operation_id:
                assert not created
            else:
                assert ready.wait(2)
            return AgentTurnObservation(bound, "completed", "controlled", 1, 1,
                                        "thread", "turn", "answer")
        def close(self):
            pass
    runtime = ForegroundRuntime(store, current.session,
        LocalRuntimeIngress(RuntimeInbox(store), artifact_digest="a" * 64,
                            configuration_digest=lambda _: "b" * 64, workspace_scope="w"),
        lambda *a, **kw: Backend(), channel_delivery=deliver,
        channel_typing=foreground_typing.compose_typing(config(route), store))
    runtime.start()
    try:
        first = runtime.step()
        assert first.result["accepted"], first.result
        event = ChannelInboundEvent(route, "next-user-request", "What remains?")
        RuntimeInbox(store).submit(current.session, event.operation_id, current.goal.digest,
                                   event.input_text, channel_event=event)
        following = runtime.step()
        assert following.result["accepted"], following.result
        assert finals == ["⏰ **定時任務**\n\nanswer", "💬 **回覆**\n\nanswer"]
        assert len(created) == 1 and created[0][0] == event.operation_id
        assert created[0][1].startswith("⏳ **互動進度**")
    finally:
        runtime.close()


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_long_background_conclusion_keeps_its_category_on_the_document(tmp_path, platform):
    from ahc.application.channel_artifact_upload import prepare_text_upload
    from ahc.domain.channel_presentation import typed_payload

    _, _, _, route = scheduled(tmp_path, platform)
    text = typed_payload({"output_text": "x" * 4096}, "scheduled")["output_text"]
    upload = prepare_text_upload(route, text)
    assert upload.caption == "⏰ 定時任務\n完整回覆"
    assert text.encode() in upload.body
    assert prepare_text_upload(route, "[定時任務]\nlegacy text").caption == "[定時任務]\n完整回覆"
    assert prepare_text_upload(route, "legacy reply").caption == "完整回覆"
