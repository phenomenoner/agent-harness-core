"""The foreground completes and resets while a real scheduled turn is still blocked."""

import threading
from dataclasses import replace
from types import SimpleNamespace

from test_channel_controls import make, reply
from test_runtime_core import Backend

from ahc.application.background_schedule import BackgroundSchedule
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def occurrence(store, current, submit):
    event = submit("/cron add every 10m background fixture")
    schedule_id = reply(store, event).partition("排程 ")[2].partition("：")[0]
    value = ScheduleRepository(store).read(current.session, schedule_id)
    return ScheduleTick(store).tick(value, value.start_ms + value.interval_ms,
                                    limit=1)["occurrences"][0]["operation_id"]


def test_blocked_background_does_not_block_foreground_new_or_result_delivery(tmp_path):
    store, current, template, route, controls, _, submit = make(tmp_path)
    source = occurrence(store, current, submit)
    entered, release, delivered = threading.Event(), threading.Event(), threading.Event()
    messages, calls = [], []

    class Blocked(Backend):
        def invoke(self, request):
            calls.append(request.operation_id)
            entered.set()
            assert release.wait(20)
            return super().invoke(request)

    def factory(binding):
        worker_route = replace(route, session=binding["worker_session"])
        inbox = RuntimeInbox(store)
        ingress = LocalRuntimeIngress(inbox, artifact_digest="a" * 64,
            configuration_digest=lambda tools: "b" * 64,
            workspace_scope=binding["workspace_id"], turn_timeout=30,
            effective_policy=template.effective_policy)
        outbox = ChannelOutbox(store)
        def send(destination, payload, operation):
            assert destination == worker_route
            messages.append(payload["output_text"])
            delivered.set()
            return "fixture-receipt"
        delivery = ChannelDelivery(outbox, worker_route, SimpleNamespace(send=send))
        def drain(*, cancelled):
            for row in outbox.read(worker_route):
                if row["state"] == "pending":
                    delivery.deliver(row["operation_id"], "background-claim")
                    return
        return ForegroundRuntime(store, binding["worker_session"], ingress,
            lambda *args, **kwargs: Blocked(), channel_delivery=drain)

    manager = BackgroundSchedule(store, current.session, factory, tick=lambda current: None,
                                 interval=0.01)
    controls.bind_background_status(manager.status)
    controls.prepare_background_restart = manager.prepare_restart
    manager.start()
    try:
        assert entered.wait(10), manager.status()
        assert manager.status()["running"] == 1
        event = submit("foreground question", "front-one")
        selected = RuntimeInbox(store).next_pending(current.session, exclude_scheduled=True)
        assert selected["operation_id"] == event.operation_id
        request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
        assert RuntimeService(store, Backend()).execute(current, request).result["accepted"]
        assert not release.is_set()
        assert "已開啟新的對話" in reply(store, submit("/new", "new-while-background"))
        assert "本聊天室沒有執行中的工作" in reply(store, submit("/stop", "stop-foreground"))
        assert manager.status()["running"] == 1
        release.set()
        assert delivered.wait(10), manager.status()
        assert messages == ["⏰ **定時任務**\n\nanswer"]
        follow = submit("foreground after result", "front-two")
        request = replace(template, operation_id=follow.operation_id, input_text=follow.input_text)
        assert RuntimeService(store, Backend()).execute(
            store.read_current(current.session.ref.virtual_session_id), request).result["accepted"]
        assert len(calls) == 1
        assert RuntimeInbox(store).query(current.session, source)["status"] != "terminal"
    finally:
        release.set()
        manager.close()
    assert manager.status()["state"] == "closed"
    restart = submit("/restart", "restart-after-background")
    store._mutate(lambda c: c.execute("UPDATE channel_controls SET state='sent' "
        "WHERE operation_id=?", (restart.operation_id,)))
    assert controls.restart_ready() is True
    assert manager._paused is True


def test_unfinished_background_model_is_not_replayed_or_a_foreground_admission_gate(tmp_path):
    store, current, template, route, controls, _, submit = make(tmp_path)
    occurrence(store, current, submit)
    manager = BackgroundSchedule(store, current.session,
        lambda binding: (_ for _ in ()).throw(AssertionError("must not replay")),
        tick=lambda current: None)
    binding = manager.dispatch.claim_one(current.session)
    request = replace(template, session=binding["worker_session"],
        operation_id=binding["worker_operation_id"], workspace_scope=binding["workspace_id"],
        input_text=binding["source_payload"]["input_text"])
    store.begin(store.read_current(request.session.ref.virtual_session_id), request)
    manager.step()
    assert manager.status()["unresolved"] == 1
    assert manager.status()["unavailable_workbenches"] == 1
    event = submit("new distinct foreground work")
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    assert RuntimeService(store, Backend()).execute(current, request).result["accepted"]
