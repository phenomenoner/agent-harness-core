"""Full entrypoint regressions from the daily-use release review."""

from dataclasses import replace

from test_channel_controls import make, reply
from test_channel_schedule_commands import scheduled_id
from test_discord_controls_ui import fixture, frame
from test_runtime_core import Backend

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.cli.foreground_commands import compose_commands
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def test_scheduled_slash_reaches_foreground_backend(tmp_path):
    store, current, _, _, controls, _, submit = make(tmp_path)
    event = submit("/cron add every 1m /new")
    value = ScheduleRepository(store).read(current.session, scheduled_id(reply(store, event)))
    ScheduleTick(store).tick(value, value.start_ms)
    backend = Backend()
    runtime = ForegroundRuntime(
        store, current.session,
        LocalRuntimeIngress(RuntimeInbox(store), artifact_digest="a" * 64,
                            configuration_digest=lambda _: "b" * 64, workspace_scope="w"),
        lambda *a, **kw: backend, local_reply=compose_commands(controls.config, store))
    runtime.start()
    try:
        result = runtime.step()
        assert backend.calls == 1
        assert result.result["output_text"] == "answer"
    finally:
        runtime.close()


def test_discord_native_idle_steer_enters_inbox_once_with_complete_text(tmp_path):
    store, current, _, route, _, gateway = fixture(tmp_path)
    text = '請先讀 "D:\\project\\task.py"\n然後說明目前進度。' + "詳細內容" * 40
    event = frame(route, 1, {"type": 1, "name": "steer", "options": [
        {"name": "value", "type": 3, "value": text}]})
    assert gateway.admit(event)
    pending = RuntimeInbox(store).next_pending(current.session)
    assert pending is not None and pending["input_text"] == text
    gateway.admit({**event, "s": 2})
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
        rows = [tuple(r) for r in c.execute("SELECT * FROM channel_controls")]
        assert "PRIVATE_TOKEN" not in str(rows)


def test_discord_native_cron_can_create_multiword_unicode_task(tmp_path):
    store, _, _, route, _, gateway = fixture(tmp_path)
    task = '讀取 "D:\\project\\task.py" 並整理'
    event = frame(route, 1, {"type": 1, "name": "cron", "options": [
        {"name": "value", "type": 3, "value": "add every 10m " + task}]})
    answer = gateway.admit(event)
    assert "已建立" in str(answer)
    with store._read() as c:
        value = ScheduleRepository._decode(c.execute("SELECT * FROM runtime_schedules").fetchone())
    assert value.input_text == task


def test_discord_native_active_steer_retains_exact_target_and_owner(tmp_path):
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.domain.channel_ingress import ChannelInboundEvent
    store, current, request, route, _, gateway = fixture(tmp_path)
    event = ChannelInboundEvent(route, "active", "original task")
    ChannelIngress(RuntimeInbox(store), route).submit(event, current.goal.digest)
    store.begin(current, replace(request, operation_id=event.operation_id,
                                 input_text=event.input_text))
    update = frame(route, 1, {"type": 1, "name": "steer", "options": [
        {"name": "value", "type": 3, "value": "先確認這個"}]})
    gateway.admit(update)
    with store._read() as c:
        row = c.execute("SELECT * FROM channel_steering").fetchone()
        assert row is not None and row["target_operation_id"] == event.operation_id
        assert row["goal_digest"] == current.goal.digest
        assert row["event_id"] == "interaction-101"


def test_queued_steering_slash_is_not_reparsed_as_command(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    event = submit("/steer /new")
    pending = RuntimeInbox(store).next_pending(current.session)
    request = replace(request, operation_id=event.operation_id, input_text=pending["input_text"])
    assert request.input_text == "/new"
    assert compose_commands(controls.config, store)(request) is None
