from test_runtime_core import Backend, setup

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.domain.schedules import IntervalSchedule
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.schedule_repository import ScheduleRepository


def test_idle_tick_then_normal_execution_and_no_replay(tmp_path):
    store, current, _ = setup(tmp_path)
    schedules = ScheduleRepository(store)
    schedules.create(IntervalSchedule("s", current.session, current.goal.digest, 100, 100, "hi"))
    tick = ForegroundSchedule(store, clock_ms=lambda: 100)
    backend = Backend()
    runtime = ForegroundRuntime(
        store, current.session,
        LocalRuntimeIngress(RuntimeInbox(store), artifact_digest="a" * 64,
                            configuration_digest=lambda _: "b" * 64, workspace_scope="w"),
        lambda *a, **kw: backend, schedule=tick,
    )
    runtime.start()
    try:
        assert runtime.step(cancelled=lambda: True) is None
        assert RuntimeInbox(store).next_pending(current.session) is None
        assert runtime.step() is None and tick.status == "submitted"
        assert runtime.schedule_status() == {
            "status": "submitted", "observation_scope": "last-idle-step",
            "execution_verified": False,
        }
        assert runtime.step().result["accepted"]
        assert runtime.step() is None and tick.status == "not-due"
        assert backend.calls == 1
        assert runtime.schedule_status()["status"] == "not-due"
        tick.status = "private diagnostic must not escape"
        assert runtime.schedule_status()["status"] == "observation-unavailable"
    finally:
        runtime.close()


def test_rotation_skips_future_and_paused_schedules(tmp_path):
    store, current, _ = setup(tmp_path)
    schedules = ScheduleRepository(store)
    for name, start in (("a", 1000), ("b", 100), ("c", 100)):
        value = IntervalSchedule(name, current.session, current.goal.digest, start, 100, "hi")
        schedules.create(value)
        if name == "c":
            schedules.set_state(value, "paused")
    tick = ForegroundSchedule(store, clock_ms=lambda: 100)
    tick(current)
    assert tick.status == "not-due"
    tick(current)
    assert tick.status == "submitted"
    tick(current)
    assert tick.status == "not-due"
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
