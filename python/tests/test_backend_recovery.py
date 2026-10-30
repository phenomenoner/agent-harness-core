from ahc.application.backend_recovery import BackendRecovery


TRACE = {"exception_type": "WslControlTransportUnavailable"}


def test_transport_recovery_needs_new_probe_then_one_exact_owner_cleanup():
    now = [0]
    events = []
    class Owner:
        def recovery_ready(self):
            events.append("probe")
            return True
    owner = Owner()
    recovery = BackendRecovery(clock=lambda: now[0])
    cleanup = lambda: events.append("cleanup")
    recovery.step(owner, cleanup, TRACE)
    assert events == []
    now[0] = 15
    recovery.step(object(), cleanup, TRACE)
    assert events == []
    recovery.step(owner, cleanup, TRACE)
    assert events == ["probe", "cleanup"]
    recovery.step(owner, cleanup, TRACE)
    assert events == ["probe", "cleanup"]
    assert recovery.observe()["state"] == "recovered"


def test_failed_probes_are_finite_and_do_not_cleanup():
    now = [0]
    probes, effects = [], []
    class Owner:
        def recovery_ready(self):
            probes.append(True)
            return False
    owner = Owner()
    recovery = BackendRecovery(clock=lambda: now[0])
    for tick in (0, 14, 15, 59, 60, 600, 6000):
        now[0] = tick
        recovery.step(owner, lambda: effects.append(True), TRACE)
    assert len(probes) == 2 and effects == []
    assert recovery.state == "exhausted"


def test_protocol_errors_do_not_trigger_and_failed_reconciliation_is_not_replayed():
    now = [0]
    calls = []
    class Owner:
        def recovery_ready(self):
            calls.append("probe")
            return True
    def fail():
        calls.append("cleanup")
        raise RuntimeError("unknown owner state")
    owner = Owner()
    recovery = BackendRecovery(clock=lambda: now[0])
    recovery.step(owner, fail, {"exception_type": "BackendCustodyUnavailable"})
    assert recovery.state == "idle"
    recovery.step(owner, fail, TRACE)
    now[0] = 15
    recovery.step(owner, fail, TRACE)
    now[0] = 600
    recovery.step(owner, fail, TRACE)
    assert calls == ["probe", "cleanup"] and recovery.state == "failed"


def test_probe_is_hidden_read_only_and_targets_only_configured_distro(monkeypatch):
    import subprocess
    from types import SimpleNamespace
    from ahc.adapters.wsl_unit_runner import WslUnitRunner
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(subprocess, "run", run)
    runner = WslUnitRunner("C:/Windows/System32/wsl.exe", "Test-Distro")
    assert runner.control_ready()
    args, options = calls[0]
    assert args == [runner.host_executable, "--distribution", "Test-Distro",
                    "--user", "ahc", "--exec", "/usr/bin/true"]
    assert options["timeout"] == 3 and options["stdin"] is subprocess.DEVNULL
    assert options["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)
