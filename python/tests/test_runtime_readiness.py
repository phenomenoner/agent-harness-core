import time

import pytest

from ahc import AppConfig, Supervisor, inspect_status, request_stop
from ahc.infrastructure.persistence import AtomicJsonStore, StateStore
from ahc.infrastructure.process import current_identity


def publish(tmp_path):
    config = AppConfig(state_dir=tmp_path)
    owner = current_identity().to_dict()
    StateStore(tmp_path).write({
        "schema_version": 1, "status": "running", "supervisor": owner,
        "children": {}, "started_at": time.time(), "updated_at": time.time(),
    })
    return config, owner


@pytest.mark.parametrize("variant", ["fresh", "foreign", "stale", "future", "malformed", "missing"])
def test_status_only_merges_fresh_same_generation_readiness(tmp_path, variant):
    config, owner = publish(tmp_path)
    sidecar = AtomicJsonStore(tmp_path / "runtime-readiness.json")
    snapshot = {"supervisor": owner, "updated_at": time.time(),
                "admission": {"ready": False, "reason": "backend-cleanup-exhausted"}}
    if variant == "foreign":
        snapshot["supervisor"] = {**owner, "start_time": owner["start_time"] + 1}
    elif variant == "stale":
        snapshot["updated_at"] -= 60
    elif variant == "future":
        snapshot["updated_at"] += 60
    elif variant == "malformed":
        snapshot["admission"]["ready"] = "false"
    if variant != "missing":
        sidecar.write(snapshot)
    before = config.state_path.read_bytes()
    code, status = inspect_status(config)
    assert code == 0 and status["status"] == "running"
    assert ("admission" in status) == (variant == "fresh")
    assert config.state_path.read_bytes() == before
    # Optional observation never changes the legacy identity-bound stop protocol.
    code, request = request_stop(config, 0)
    assert code == 0 and request["target"] == owner


def test_optional_readiness_write_failure_does_not_fail_lifecycle_publish(tmp_path, monkeypatch):
    config, _ = publish(tmp_path)
    supervisor = Supervisor(config)
    supervisor._identity = current_identity()
    supervisor._started_at = time.time()
    class Runtime:
        def admission_status(self):
            return {"ready": True, "reason": "ready"}
    supervisor.runtime = Runtime()
    original = AtomicJsonStore.write
    def write(self, value):
        if self.path.name == "runtime-readiness.json":
            raise PermissionError("optional diagnostics unavailable")
        return original(self, value)
    monkeypatch.setattr(AtomicJsonStore, "write", write)
    assert supervisor._safe_publish("running")
    assert StateStore(tmp_path).read()["status"] == "running"


@pytest.mark.parametrize("state", ["ready", "paused", "failed", "closed", "close-unverified",
                                  "backoff", "degraded"])
def test_nested_discord_receiver_terminal_state_is_visible_and_not_ready(tmp_path, state):
    from types import SimpleNamespace
    from test_channel_controls import make
    from ahc.application.foreground_runtime import ForegroundRuntime

    store, current, *_ = make(tmp_path)
    runtime = ForegroundRuntime(store, current.session, lambda *_: None, lambda *_: None,
        channel_receiver=SimpleNamespace(failed=False, status=lambda: {
            "state": "running", "discord": {"state": state, "private": "never publish"}}))
    runtime.state = "running"
    result = runtime.admission_status()
    assert result["ready"] is (state == "ready")
    assert result["reason"] == ("ready" if state == "ready" else "receiver-unavailable")
    assert result["receiver"] == {"state": "running", "discord": state}
    assert "private" not in str(result)


@pytest.mark.parametrize("value", [None, {"state": "private-state"}, {"state": ["secret"]}])
def test_receiver_observation_never_publishes_unknown_payload(tmp_path, value):
    from types import SimpleNamespace
    from test_channel_controls import make
    from ahc.application.foreground_runtime import ForegroundRuntime
    store, current, *_ = make(tmp_path)
    runtime = ForegroundRuntime(store, current.session, lambda *_: None, lambda *_: None,
        channel_receiver=SimpleNamespace(status=lambda: value))
    runtime.state = "running"
    result = runtime.admission_status()
    assert result["ready"] is False
    assert result["receiver"] == {"state": "observation-unavailable"}


@pytest.mark.parametrize("unsafe", [False, True])
def test_discord_budget_diagnostics_are_bounded_and_sanitized(unsafe):
    from types import SimpleNamespace
    from ahc.application.foreground_runtime import ForegroundRuntime
    runtime = object.__new__(ForegroundRuntime)
    runtime.channel_receiver = SimpleNamespace(status=lambda: {
        "state": "running", "discord": {"state": "paused",
            "reason": "secret payload" if unsafe else "connection-budget-exhausted",
            "connection_attempts": True if unsafe else 2,
            "connection_limit": -1 if unsafe else 2,
            "total_connection_attempts": 2**40 if unsafe else 5,
            "token": "secret payload"}})
    value = runtime._receiver_observation()
    if unsafe:
        assert value == {"state": "running", "discord": "paused"}
    else:
        assert value["discord_reason"] == "connection-budget-exhausted"
        assert value["discord_connection_attempts"] == 2
        assert value["discord_total_connection_attempts"] == 5
        assert value["discord_connection_limit"] == 2
    assert "secret" not in str(value)


@pytest.mark.parametrize("unsafe", [False, True])
def test_telegram_transport_probe_diagnostics_are_bounded_and_sanitized(unsafe):
    from types import SimpleNamespace
    from ahc.application.foreground_runtime import ForegroundRuntime
    runtime = object.__new__(ForegroundRuntime)
    runtime.channel_receiver = SimpleNamespace(status=lambda: {
        "state": "running", "telegram": {"state": "paused",
            "transport_probe_attempts": True if unsafe else 2,
            "transport_probe_limit": -1 if unsafe else 2016, "token": "secret"}})
    value = runtime._receiver_observation()
    assert value == ({"state": "running", "telegram": "paused"} if unsafe else {
        "state": "running", "telegram": "paused", "telegram_transport_probe_attempts": 2,
        "telegram_transport_probe_limit": 2016})
