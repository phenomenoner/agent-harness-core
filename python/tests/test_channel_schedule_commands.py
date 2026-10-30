from dataclasses import replace
from types import SimpleNamespace

from test_runtime_core import Backend, setup

from ahc.application.runtime_service import RuntimeService
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.daily_schedules import DailySchedule
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_schedule_commands import handle_schedule_command
from ahc.infrastructure.channel_schedule_routes import ChannelScheduleRoutes
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick


def fixture(tmp_path):
    store, current, request = setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", current.session)
    controls = SimpleNamespace(store=store)

    def command(text, now=600_000):
        return store._mutate(
            lambda connection: handle_schedule_command(
                controls, connection, route, text, now_ms=now
            )
        )

    return store, current, request, route, command


def scheduled_id(reply):
    return reply.partition("排程 ")[2].partition("：")[0]


def test_cron_create_list_state_and_reopen_keep_exact_route(tmp_path):
    store, current, _, route, command = fixture(tmp_path)
    assert handle_schedule_command(SimpleNamespace(store=store), None, route, "/model") is None
    assert "語法" in command("/cron add every 0m task")
    assert command("/cron list") == "本聊天室沒有排程。"

    reply = command('/cron add every 10m "write daily note"')
    schedule_id = scheduled_id(reply)
    assert "將回覆至本聊天室" in reply
    assert schedule_id in command("/cron list")
    value = ScheduleRepository(store).read(current.session, schedule_id)
    assert type(value) is IntervalSchedule and value.interval_ms == 600_000
    with store._read() as connection:
        assert ChannelScheduleRoutes.route_for(connection, value) == route

    task = 'powershell -File "C:\\Users\\operator\\daily task.ps1"\nWrite-Output "done"'
    exact = command("/cron add every 10m " + task)
    preserved = ScheduleRepository(store).read(current.session, scheduled_id(exact))
    assert preserved.input_text == task

    assert "已暫停" in command("/cron pause " + schedule_id)
    assert "已啟用" in command("/cron resume " + schedule_id)
    assert "已刪除" in command("/cron delete " + schedule_id)
    assert "不能變更" in command("/cron resume " + schedule_id)
    reopened = ScheduleRepository(type(store)(store.root))
    assert reopened.read(current.session, schedule_id).state == "removed"


def test_daily_route_due_admission_has_one_channel_outbox_delivery(tmp_path):
    store, current, request, route, command = fixture(tmp_path)
    daily_reply = command('/cron add daily 08:30 Asia/Taipei "prepare report"', now=1_000)
    daily = ScheduleRepository(store).read(current.session, scheduled_id(daily_reply))
    assert type(daily) is DailySchedule and daily.timezone == "Asia/Taipei"

    reply = command("/cron add every 10m scheduled task")
    value = ScheduleRepository(store).read(current.session, scheduled_id(reply))
    assert ScheduleTick(store).tick(value, 600_000)["occurrences"] == []
    first = ScheduleTick(store).tick(value, 1_200_000)
    second = ScheduleTick(store).tick(value, 1_200_000)
    assert len(first["occurrences"]) == 1 and second["occurrences"] == []
    operation_id = first["occurrences"][0]["operation_id"]
    pending = RuntimeInbox(store).next_pending(current.session)
    assert pending["operation_id"] == operation_id and pending["input_text"] == "scheduled task"
    with store._read() as connection:
        row = connection.execute(
            "SELECT route_json FROM runtime_channel_routes WHERE operation_id=?", (operation_id,)
        ).fetchone()
        assert row is not None

    result = RuntimeService(store, Backend()).execute(
        current,
        replace(request, operation_id=operation_id, input_text="scheduled task", deadline=700_000),
    )
    assert result.result["accepted"] is True
    outbox = ChannelOutbox(store).read(route)
    assert len(outbox) == 1 and outbox[0]["operation_id"] == operation_id
    assert outbox[0]["state"] == "pending"


def test_sibling_route_cannot_list_or_mutate_schedule(tmp_path):
    store, current, _, route, command = fixture(tmp_path)
    value = ScheduleRepository(store).read(
        current.session, scheduled_id(command("/cron add every 10m private task")))
    other = replace(route, user_id="another-user")
    def foreign(text):
        return store._mutate(lambda c: handle_schedule_command(
            SimpleNamespace(store=store), c, other, text))
    assert foreign("/cron list") == "本聊天室沒有排程。"
    assert "不屬於" in foreign("/cron delete " + value.schedule_id)
    assert ScheduleRepository(store).read(current.session, value.schedule_id) == value


def test_failed_route_binding_leaves_no_orphan_schedule(tmp_path, monkeypatch):
    from ahc.domain.sessions import ContractError
    store, _, _, _, command = fixture(tmp_path)
    def fail(*args):
        raise ContractError("injected binding failure")
    monkeypatch.setattr(ChannelScheduleRoutes, "bind", fail)
    assert "未變更" in command("/cron add every 10m task")
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM runtime_schedules").fetchone()[0] == 0


def test_scheduled_slash_text_is_task_input_not_control(tmp_path):
    from test_channel_controls import make, reply
    store, current, _, _, _, _, submit = make(tmp_path)
    event = submit("/cron add every 1m /new")
    schedule_id = scheduled_id(reply(store, event))
    value = ScheduleRepository(store).read(current.session, schedule_id)
    result = ScheduleTick(store).tick(value, value.start_ms)
    operation_id = result["occurrences"][0]["operation_id"]
    pending = RuntimeInbox(store).next_pending(current.session)
    assert pending["operation_id"] == operation_id and pending["input_text"] == "/new"
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM channel_controls WHERE operation_id=?",
                         (operation_id,)).fetchone()[0] == 0
