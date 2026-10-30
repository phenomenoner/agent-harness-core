"""Configured scheduled ingress inherits the ordinary profile and policy binding."""
import pytest
from profile_test_support import profile_registry_payload
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.cli import foreground_trusted as composition
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_profiles import bind_profile_configuration
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.schedule_repository import ScheduleRepository


@pytest.mark.parametrize("owned", [True, False])
def test_scheduled_foreground_uses_selected_profile_policy_and_capabilities(
        tmp_path, monkeypatch, owned):
    original, request, raw, _ = config_file(tmp_path, enabled=True)
    registry = AgentProfileRegistry.from_dict(profile_registry_payload(
        (request.session.agent_id,) if owned else ("foreign",)))
    created = []
    admitted = []

    class Factory:
        def __init__(self, store, config):
            created.append(config)
            self.config = config

        def recover(self, *args):
            pass

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            assert bound.session == request.session
            assert bound.goal_digest == request.goal_digest
            assert bound.effective_policy == self.config.effective_policy
            assert bound.configuration_digest == self.config.configuration_digest
            assert bound.capabilities == tuple(tools.capabilities())
            assert "workspace.memory_native" not in bound.capabilities
            admitted.append(bound)

            class Backend:
                def invoke(self, exact):
                    return AgentTurnObservation(exact, "completed", "fixture", 1, 1,
                                                "thread", "turn", "done")

                def close(self):
                    pass

            return Backend()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, kw["checkpoint"]))
    if not owned:
        with pytest.raises(ContractError):
            compose_foreground_document(raw, profile_registry=registry)
        assert created == admitted == []
        return
    runtime = compose_foreground_document(raw, profile_registry=registry)
    base = parse_foreground_config_v2(raw)
    expected = bind_profile_configuration(base, registry)
    assert expected.configuration_digest != base.configuration_digest
    assert created[0].configuration_digest == expected.configuration_digest
    ScheduleRepository(original.store).create(IntervalSchedule(
        "scheduled", request.session, request.goal_digest, 100, 100, "ordinary request"))
    runtime.schedule = ForegroundSchedule(runtime.store, clock_ms=lambda: 100)
    runtime.start()
    try:
        assert runtime.step() is None
        assert runtime.step().result["accepted"]
        assert len(admitted) == 1
        assert runtime.step() is None
    finally:
        runtime.close()
