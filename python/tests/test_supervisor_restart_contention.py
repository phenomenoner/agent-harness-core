import json

from ahc import AppConfig, Supervisor
from ahc.infrastructure.persistence import StateStore
from ahc.infrastructure.working_repository import WorkingWriterBusy


class RestartRuntime:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.checks = 0
        self.closed = False

    def start(self):
        pass

    def step(self, **kwargs):
        pass

    def restart_requested(self):
        self.checks += 1
        result = next(self.outcomes)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


def test_restart_check_recovers_once_without_stopping_service(tmp_path):
    runtime = RestartRuntime([WorkingWriterBusy('private lock details'), True])
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 75
    assert runtime.checks == 2 and runtime.closed
    assert StateStore(config.state_dir).read()['status'] == 'stopped'


def test_deferred_restart_probe_precedes_any_next_runtime_step(tmp_path):
    runtime = RestartRuntime([WorkingWriterBusy('busy'), True])
    steps = []
    runtime.step = lambda **kwargs: steps.append('work')
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 75
    assert steps == ['work']


def test_repeated_restart_contention_defers_work_then_restarts_once(tmp_path):
    runtime = RestartRuntime([WorkingWriterBusy('PRIVATE') for _ in range(4)] + [True])
    steps = []
    runtime.step = lambda **kwargs: steps.append('work')
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 75
    assert runtime.checks == 5 and runtime.closed
    assert steps == ['work']
    state = StateStore(config.state_dir).read()
    assert state['status'] == 'stopped' and 'error' not in state
    assert not (config.state_dir/'runtime-failure.json').exists()


def test_contention_waits_on_normal_cadence_and_remains_stoppable(tmp_path):
    runtime = RestartRuntime([WorkingWriterBusy('PRIVATE') for _ in range(100)])
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.02)
    supervisor = Supervisor(config, runtime=runtime)
    waits = []
    def wait(delay):
        waits.append(delay)
        if len(waits) == 4:
            supervisor._stop_event.set()
    supervisor._stop_event.wait = wait
    steps = []
    runtime.step = lambda **kwargs: steps.append('work')
    assert supervisor.run(max_runtime_seconds=2) == 0
    assert runtime.checks == 4 and runtime.closed
    assert waits == [0.02] * 4 and steps == ['work']


def test_restart_check_other_error_fails_once_with_sanitized_diagnostic(tmp_path):
    runtime = RestartRuntime([RuntimeError('PRIVATE')])
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 4
    assert runtime.checks == 1 and runtime.closed
    diagnostic = json.loads((config.state_dir/'runtime-failure.json').read_text())
    assert diagnostic['runtime_failures'][0]['phase'] == 'restart-check'
    assert 'PRIVATE' not in json.dumps([StateStore(config.state_dir).read(), diagnostic])


def test_successful_probe_resets_consecutive_contention_budget(tmp_path):
    runtime = RestartRuntime([WorkingWriterBusy('busy'), False, WorkingWriterBusy('busy'), True])
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 75
    assert runtime.checks == 4 and runtime.closed


def test_restart_transaction_rolls_back_under_reader_then_commits_once(tmp_path):
    import sqlite3
    from types import SimpleNamespace

    from test_channel_controls import make

    store, _, _, route, controls, _, submit = make(tmp_path)
    event = submit('/restart')
    controls.deliver_one({route: SimpleNamespace(send=lambda *args: 'sent-once')})
    reader = sqlite3.connect(store.db_path, isolation_level=None)
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM channel_controls').fetchall()

    class Runtime(RestartRuntime):
        def restart_requested(self):
            self.checks += 1
            try:
                return controls.restart_ready()
            finally:
                if self.checks == 1:
                    reader.rollback()

    runtime = Runtime([])
    config = AppConfig(state_dir=tmp_path/'supervisor', poll_interval_seconds=0.01)
    try:
        assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=2) == 75
    finally:
        reader.close()
    assert runtime.checks == 2 and runtime.closed
    with store._read() as connection:
        row = connection.execute('SELECT restart,state,receipt FROM channel_controls '
                                 'WHERE operation_id=?', (event.operation_id,)).fetchone()
        assert tuple(row) == (2, 'sent', 'sent-once')
        assert connection.execute('SELECT count(*) FROM runtime_turns').fetchone()[0] == 0
    assert not controls.restart_ready()
