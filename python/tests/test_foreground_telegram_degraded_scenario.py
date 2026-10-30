"""Local work and the sole Telegram entrance survive receiver outages."""

import json
import pytest

from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.telegram_poll import TelegramPoll, TelegramPollingUnavailable
from ahc.cli import foreground_telegram, main
from ahc.cli import foreground_trusted as composition
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_paused_polling_does_not_stop_local_foreground_work(tmp_path, monkeypatch, capsys):
    original, request, config, path = config_file(tmp_path, enabled=True)
    route = ChannelRoute("telegram", "123", "456", "789", request.session)
    config["channels"] = [{"route": route.to_dict(), "token_env": "AHC_POLL_TEST",
                           "enabled": False}]
    config["telegram_polling"] = {"account_id": "123", "token_env": "AHC_POLL_TEST",
        "enabled": True, "exclusive_consumer": True, "interval_seconds": 1}
    path.write_text(json.dumps(config), encoding="utf-8")
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    polls, invoked = [], []

    class Poll(TelegramPoll):
        def poll(self, offset=None):
            operation = "local-" + str(len(polls) + 1)
            polls.append(offset)
            RuntimeInbox(original.store).submit(request.session, operation,
                                                request.goal_digest, "local input")
            raise TelegramPollingUnavailable("private-provider-error")

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
                    invoked.append(admitted.operation_id)
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "local answer")
            return Backend()

    monkeypatch.setenv("AHC_POLL_TEST", "fixture-token")
    monkeypatch.setattr(foreground_telegram, "TelegramPoll", Poll)
    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, None))
    assert main(["--config", str(app), "run", "--runtime-config", str(path),
                 "--max-runtime", "3"]) == 0
    output = capsys.readouterr().out
    assert "fixture-token" not in output and "private-provider-error" not in output
    report = json.loads(output)
    assert report["runtime"]["state"] == "closed"
    ingress = report["runtime"]["channels"]["ingress"]
    assert ingress["state"] == "backoff" and ingress["automatic_recovery"] is True
    assert ingress["consecutive_failures"] == 2
    assert ingress["failure_category"] == "unverified-response"
    assert ingress["next_retry_seconds"] > 0
    assert polls == [None, None]
    assert invoked == ["local-1", "local-2"]
    inbox = RuntimeInbox(RuntimeRepository(original.store.root))
    assert inbox.next_pending(request.session) is None
    for operation in invoked:
        assert inbox.query(request.session, operation)["status"] == "terminal"


@pytest.mark.parametrize("malformed", [False, True])
def test_only_telegram_entrance_recovers_twice_and_delivers_distinct_tasks(tmp_path, monkeypatch,
                                                                       malformed):
    import sys
    from test_telegram_inbox import update
    from ahc.adapters.channel_text import BotTextTransport
    from ahc.cli import foreground_channels
    from ahc.cli.foreground import compose_foreground
    from ahc.domain.channel_ingress import channel_operation_id
    from ahc.infrastructure.channel_outbox import ChannelOutbox
    from ahc.infrastructure.telegram_inbox import TelegramInbox
    from ahc.infrastructure.telegram_hold import inspect_held_updates

    original, request, config, path = config_file(tmp_path, enabled=True)
    if sys.platform == "linux":
        from test_foreground_config_native import native_config
        config = native_config()
        config.update(runtime_root=str(original.store.root), session=request.session.to_dict())
        config["backend"]["enabled"] = True
        config["workspace"]["interpreter"] = sys.executable
    route = ChannelRoute("telegram", "123", "456", "789", request.session)
    config["channels"] = [{"route": route.to_dict(), "token_env": "AHC_POLL_TEST", "enabled": True}]
    config["telegram_polling"] = {"account_id": "123", "token_env": "AHC_POLL_TEST",
        "enabled": True, "exclusive_consumer": True, "interval_seconds": 1,
        "transport_recovery": {"interval_seconds": 30, "probe_limit": 1}}
    path.write_text(json.dumps(config), encoding="utf-8")
    clock, polls, probes, invoked, sent = [0.0], [], [], [], []
    failure = TelegramPollingUnavailable("unverified")
    first_batch = [update(6, text=123), update(7)] if malformed else [update(7)]
    outcomes = iter([failure, failure, first_batch, failure, failure, [update(8)], []])
    probe_outcomes = iter([None, failure, None])

    class Poll:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, offset=None):
            polls.append(offset)
            result = next(outcomes)
            if isinstance(result, Exception):
                raise result
            return result

        def probe(self):
            probes.append(clock[0])
            result = next(probe_outcomes)
            if isinstance(result, Exception):
                raise result

    class Runner:
        def __init__(self, *_args, **_kwargs):
            pass

    class Factory:
        def __init__(self, *_args):
            pass

        def recover(self, *_args):
            return ()

        def binding(self, _tools):
            return "a" * 64, "b" * 64

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    invoked.append(admitted.operation_id)
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "channel answer")
            return Backend()

    def post(platform, token, channel, body):
        sent.append(body)
        return {"ok": True, "result": {"message_id": len(sent), "chat": {"id": 456},
                                        "from": {"id": 123}}}

    monkeypatch.setenv("AHC_POLL_TEST", "fixture-token")
    monkeypatch.setattr(foreground_telegram, "TelegramPoll", Poll)
    monkeypatch.setattr(foreground_telegram.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    if sys.platform == "linux":
        from ahc.adapters import native_process_runner, native_python_workspace
        monkeypatch.setattr(native_process_runner, "NativeProcessRunner", Runner)
        monkeypatch.setattr(native_python_workspace, "NativePythonWorkspace",
                            lambda store, runner, identity, **kw: Port(identity, None))
    else:
        monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
        monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                            lambda store, runner, identity, **kw: Port(identity, None))
    monkeypatch.setattr(foreground_channels, "BotTextTransport",
                        lambda route, token: BotTextTransport(route, token, post=post))
    runtime = compose_foreground(path)
    runtime.start()
    try:
        for stamp in (0, 1, 31, 32, 33, 34, 64, 94, 95, 96):
            clock[0] = stamp
            runtime.step()
        operations = [channel_operation_id(route, "update-" + str(i)) for i in (7, 8)]
        assert invoked == operations and len(sent) == 2
        assert probes == [31, 64, 94] and polls[-1] == 9
        assert TelegramInbox(runtime.store, [route]).offset() == 9
        assert inspect_held_updates(TelegramInbox(runtime.store, [route])) == {
            "held_count": int(malformed), "update_ids": [6] if malformed else []}
        assert all(row["state"] == "sent" for row in ChannelOutbox(runtime.store).read(route))
        assert all(RuntimeInbox(runtime.store).query(request.session, op)["status"] == "terminal"
                   for op in operations)
        assert runtime.channel_status()["ingress"]["state"] == "ready"
        assert runtime.channel_status()["ingress"]["held_updates"] == int(malformed)
    finally:
        runtime.close()
