"""Supervisor keeps local work available while the optional platform receiver pauses."""

import json

from test_discord_identify import LIMITS
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.discord_identify import DiscordIdentifyAdmission
from ahc.cli import foreground_discord, main
from ahc.cli import foreground_trusted as composition
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_paused_gateway_does_not_stop_local_foreground_work(tmp_path, monkeypatch, capsys):
    original, request, config, path = config_file(tmp_path, enabled=True)
    route = ChannelRoute("discord", "123", "456", "789", request.session)
    config["channels"] = [
        {"route": route.to_dict(), "token_env": "AHC_POLL_TEST", "enabled": False}
    ]
    config["discord_gateway"] = {
        "account_id": "123",
        "token_env": "AHC_POLL_TEST",
        "enabled": True,
        "intents": 0,
        "connection_limit": 1,
    }
    path.write_text(json.dumps(config), encoding="utf-8")
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    calls, invoked, workers = [], [], []
    RuntimeInbox(original.store).submit(
        request.session, "local-1", request.goal_digest, "local input"
    )
    before = original.store.read_current(request.session.ref.virtual_session_id)
    compose = foreground_discord.compose_discord

    def http(token):
        calls.append(True)
        return {**LIMITS, "session_start_limit": {**LIMITS["session_start_limit"], "remaining": 0}}

    def compose_receiver(*args):
        worker = compose(*args)
        workers.append(worker)
        return worker

    class Runner:
        def __init__(self, *args, **kwargs):
            pass

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    workers[0]._thread.join(timeout=3)
                    assert workers[0].status()["state"] == "paused"
                    invoked.append(admitted.operation_id)
                    return AgentTurnObservation(
                        admitted, "completed", "fixture", 1, 1, "thread", "turn", "local answer"
                    )

            return Backend()

    monkeypatch.setenv("AHC_POLL_TEST", "fixture-token")
    # This scenario tests admission exhaustion, not an unauthenticated live socket.
    from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver

    class Socket:
        def __init__(self, *_):
            self.frames = [{"op": 10, "d": {"heartbeat_interval": 1000}}]

        def receive(self, **_):
            return self.frames.pop(0) if self.frames else None

        def send(self, frame):
            raise AssertionError("Identify must be rejected before socket send")

        def close(self):
            pass

    monkeypatch.setattr(
        foreground_discord,
        "DiscordGatewayDriver",
        lambda *args, **kwargs: DiscordGatewayDriver(*args, connect=Socket, **kwargs),
    )
    monkeypatch.setattr(
        foreground_discord,
        "DiscordIdentifyAdmission",
        lambda token: DiscordIdentifyAdmission(token, request=http),
    )
    monkeypatch.setattr(composition, "compose_discord", compose_receiver)
    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(
        wsl_python_workspace,
        "WslPythonWorkspace",
        lambda store, runner, identity, **kw: Port(identity, None),
    )
    assert (
        main(["--config", str(app), "run", "--runtime-config", str(path), "--max-runtime", "3"])
        == 0
    )
    output = capsys.readouterr().out
    assert "fixture-token" not in output and "private-provider-error" not in output
    report = json.loads(output)
    assert report["runtime"]["state"] == "closed"
    assert len(workers) == 1 and not workers[0]._thread.is_alive()
    assert calls == [True]
    assert invoked == ["local-1"]
    after = original.store.read_current(request.session.ref.virtual_session_id)
    assert after.session == before.session and after.goal == before.goal
    inbox = RuntimeInbox(RuntimeRepository(original.store.root))
    assert inbox.next_pending(request.session) is None
    for operation in invoked:
        assert inbox.query(request.session, operation)["status"] == "terminal"
