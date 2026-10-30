"""Lost create replies must not poison the next independent task."""

import json
from dataclasses import asdict, replace

import pytest
from test_backend_custody import Runner, held_count, ready_to_create

from ahc.adapters.codex_transport import CodexTransportError
from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.adapters.wsl_unit_runner import WslCreationIndeterminate, WslUnitRunner
from ahc.application.failure_trace import failure_trace
from ahc.domain.backend_creation import BackendCreationAttempt, BackendCreationCancelled
from ahc.infrastructure.backend_custody import BackendCustody, BackendCustodyUnavailable
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("born", [False, True])
@pytest.mark.parametrize("role", ["backend", "python-workspace"])
def test_lost_create_reopen_reconciles_then_accepts_new_task(tmp_path, born, role):
    repo, original, lease, workspace, unit = ready_to_create(tmp_path)
    lease.close()
    custody = BackendCustody(repo, resource_role=role)
    lease = custody.acquire(workspace.session, workspace.workspace_id)
    attempt = BackendCreationAttempt(unit.distribution, unit.unit)

    class Lost(Runner):
        def prepare_creation(self, *a, **kw):
            return attempt

        def create_attempt(self, selected, *a, **kw):
            assert selected == attempt
            with repo._read() as c:
                row = c.execute("SELECT * FROM backend_creation_attempts WHERE owner_key=?",
                                (lease.key,)).fetchone()
                assert json.loads(row["attempt_json"]) == asdict(attempt)
                assert c.execute("SELECT state FROM backend_creation_intents WHERE owner_key=?",
                                 (lease.key,)).fetchone()[0] == "pending"
            raise WslCreationIndeterminate(unit.distribution, unit.unit)

        def reconcile_creation(self, selected):
            assert selected == attempt
            return unit if born else BackendCreationCancelled(attempt)

    runner = Lost(unit)
    with pytest.raises(WslCreationIndeterminate):
        custody.create(lease, workspace, runner, ["/bin/sleep", "1"], lifetime_seconds=1)
    lease.close()
    custody = BackendCustody(RuntimeRepository(repo.root), resource_role=role)
    lease = custody.acquire(workspace.session, workspace.workspace_id)
    try:
        assert custody.recover(lease, runner) == ((unit,) if born else ())
        assert held_count(repo) == 0
        assert runner.calls == (["observe", "stop", "observe"] if born else [])
        next_unit = replace(unit, unit="ahc-backend-" + "d" * 32 + ".service")

        class New(Runner):
            def create(self, *a, **kw):
                return next_unit

        successor = New(next_unit)
        assert custody.create(lease, workspace, successor, ["/bin/sleep", "1"],
                              lifetime_seconds=1) == next_unit
        custody.recover(lease, successor)
        assert held_count(repo) == 0
    finally:
        lease.close()


@pytest.mark.parametrize("fault", ["unavailable", "foreign", "corrupt", "unbound-success"])
def test_uncertain_reconciliation_never_clears_pending_or_stops(tmp_path, fault):
    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    attempt = BackendCreationAttempt(unit.distribution, unit.unit)

    class Lost(Runner):
        def prepare_creation(self, *a, **kw):
            return attempt

        def create_attempt(self, *a, **kw):
            if fault == "unbound-success":
                return replace(unit, unit="ahc-backend-" + "d" * 32 + ".service")
            raise OSError("lost")

        def reconcile_creation(self, selected):
            if fault in ("unavailable", "unbound-success"):
                raise BackendCustodyUnavailable("busy")
            return BackendCreationCancelled(replace(attempt, distribution="Foreign"))

    runner = Lost(unit)
    try:
        with pytest.raises((OSError, ValueError)):
            custody.create(lease, workspace, runner, ["/bin/true"], lifetime_seconds=1)
        if fault == "corrupt":
            repo._mutate(lambda c: c.execute("UPDATE backend_creation_attempts SET record_digest='x'"))
        with pytest.raises(BackendCustodyUnavailable):
            custody.recover(lease, runner)
        with repo._read() as c:
            assert c.execute("SELECT state FROM backend_creation_intents").fetchone()[0] == "pending"
        assert runner.calls == [] and held_count(repo) == 0
    finally:
        lease.close()


@pytest.mark.parametrize("category", ["eof", "reader-io", "deadline", "malformed-json"])
def test_stream_transport_category_reaches_durable_failure_trace(monkeypatch, category):
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", "AHC-Test")

    class Transport:
        def send(self, *a, **kw):
            pass

        def receive(self, **kw):
            raise CodexTransportError("private diagnostic", transport_error_type=category)

    monkeypatch.setattr(runner, "_controller_source", lambda: "fixture")
    monkeypatch.setattr(runner, "_open_transport", lambda _: Transport())
    with pytest.raises(WslCreationIndeterminate) as raised:
        runner.create(["/bin/true"], lifetime_seconds=1)
    trace = failure_trace(raised.value)
    assert trace["control_phase"] == "receive" and trace["control_error_type"] == category
    assert "private diagnostic" not in json.dumps(trace)


@pytest.mark.parametrize("fault", [None, "unit", "distribution", "boolean", "extra"])
def test_reconciliation_reply_is_exactly_bound(fault):
    runner = WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    attempt = runner.prepare_creation(["/bin/true"], lifetime_seconds=1)
    result = {"unit": attempt.unit, "cancelled": True, "identity": None}
    if fault == "unit":
        result["unit"] = "ahc-backend-" + "a" * 32 + ".service"
    if fault == "distribution":
        result.update(cancelled=False, identity={"distribution": "foreign", "unit": attempt.unit,
                      "boot_id": "12345678-1234-1234-1234-123456789abc", "invocation_id": "b" * 32})
    if fault == "boolean":
        result["cancelled"] = 1
    if fault == "extra":
        result["extra"] = True
    request = {"action": "reconcile-create", "unit": attempt.unit}
    if fault is None:
        assert runner._decode({"ok": True, "result": result}, request) == BackendCreationCancelled(attempt)
    else:
        with pytest.raises(BackendCustodyUnavailable):
            runner._decode({"ok": True, "result": result}, request)


def test_database_commit_loss_retains_attempt_for_exact_cleanup(tmp_path):
    from ahc.infrastructure.working_repository import WorkingRepositoryError

    repo, custody, lease, workspace, unit = ready_to_create(tmp_path)
    attempt = BackendCreationAttempt(unit.distribution, unit.unit)

    class Born(Runner):
        def prepare_creation(self, *a, **kw):
            return attempt

        def create_attempt(self, *a, **kw):
            return unit

        def reconcile_creation(self, selected):
            assert selected == attempt
            return unit

    runner = Born(unit)
    repo._mutate(lambda c: c.execute(
        "CREATE TRIGGER fail_resolution BEFORE UPDATE ON backend_creation_intents "
        "WHEN NEW.state='recorded' BEGIN SELECT RAISE(ABORT,'injected'); END"))
    try:
        with pytest.raises(WorkingRepositoryError):
            custody.create(lease, workspace, runner, ["/bin/true"], lifetime_seconds=1)
        assert held_count(repo) == 0
        with repo._read() as c:
            assert c.execute("SELECT count(*) FROM backend_creation_attempts").fetchone()[0] == 1
            assert c.execute("SELECT state FROM backend_creation_intents").fetchone()[0] == "pending"
        repo._mutate(lambda c: c.execute("DROP TRIGGER fail_resolution"))
        assert custody.recover(lease, runner) == (unit,)
        assert runner.calls == ["observe", "stop", "observe"]
    finally:
        lease.close()
