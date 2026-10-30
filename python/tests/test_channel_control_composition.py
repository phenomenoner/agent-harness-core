import json

from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.application.channel_ingress import ChannelIngress
from ahc.cli import foreground_trusted as composition
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_chat_preferences_reach_factory_and_children_without_changing_inflight_policy(
        tmp_path, monkeypatch):
    original, request, value, _ = config_file(tmp_path, enabled=True)
    route = ChannelRoute("telegram", "1", "2", "3", request.session)
    value["channels"] = [{"enabled": True, "route": route.to_dict(), "token_env": "TEST_TOKEN"}]
    value["channel_control"] = {
        "enabled": True, "models": {"gpt-test": {"efforts": ["high", "max"], "priority": True},
                                    "other": {"efforts": ["high", "max"], "priority": True}},
        "aliases": {}}
    from ahc.adapters import wsl_python_workspace, wsl_stream_runner
    from ahc.cli import foreground_control_pump

    factories, observations = [], []
    monkeypatch.setenv("TEST_TOKEN", "test")
    monkeypatch.setattr(composition, "compose_channels", lambda *args: None)
    monkeypatch.setattr(foreground_control_pump.ChannelControlPump, "start", lambda self: None)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *args, **kwargs: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kwargs: Port(identity, None))

    class Factory:
        def __init__(self, store, config, **hooks):
            self.store, self.config, self.hooks = store, config, hooks
            factories.append(self)

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def recover(self, *args):
            pass

        def __call__(self, bound, tools, *, cancelled):
            owner = self
            class Backend:
                def invoke(self, admitted):
                    assert tools.children.owner is owner
                    assert tools.children.children is not None
                    assert admitted.effective_policy == owner.config.effective_policy
                    assert owner.hooks["tool_gate"] is not None
                    assert owner.hooks["approval_port_factory"](admitted).notifier is not None
                    observations.append(owner.config.to_dict())
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "done")
                def close(self):
                    pass
            return Backend()
    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    runtime = composition.compose_trusted_foreground(value)
    ingress = ChannelIngress(RuntimeInbox(runtime.store), route)
    runtime.start()
    try:
        for ordinal, text in enumerate(("/model other", "/think max", "/fast on",
                                        "/approvals always-approve", "hello")):
            ingress.submit(ChannelInboundEvent(route, str(ordinal), text), request.goal_digest)
        assert runtime.step().result["accepted"]
        selected = observations[0]
        assert selected["backend"]["model"] == "other"
        assert selected["backend"]["reasoning_effort"] == "max"
        assert selected["backend"]["service_tier"] == "priority"
        assert selected["policy"]["approval_mode"] == "always-approve"
        assert factories[0].config.effective_policy.approval_mode == "manual"
        assert json.loads(json.dumps(value))["backend"]["model"] == "gpt-test"
    finally:
        runtime.close()
