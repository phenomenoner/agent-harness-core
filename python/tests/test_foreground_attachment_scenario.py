"""CLI supervisor attachment delivery with controlled backend/platform peers."""

import json
import threading

import pytest
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.adapters.channel_text import BotTextTransport
from ahc.cli import (
    foreground_channels,
    foreground_progress,
    foreground_trusted,
    foreground_typing,
    main,
)
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.result_artifacts import ResultArtifact
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("typing", [False, True])
@pytest.mark.parametrize("progress_mode", [None, "sent", "unknown"])
def test_run_sends_selected_attachment_once_and_closes(
    tmp_path, monkeypatch, capsys, platform, typing, progress_mode
):
    original, request, config, path = config_file(tmp_path, enabled=True)
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    route = ChannelRoute(platform, "123", "456", "789", request.session)
    config["channels"] = [{"route": route.to_dict(), "token_env": "AHC_ATTACHMENT_SCENARIO",
                           "enabled": True}]
    if typing:
        config["channels"][0]["typing"] = True
    if progress_mode:
        config["channels"][0]["progress"] = True
    path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setenv("AHC_ATTACHMENT_SCENARIO", "fixture")
    events, model_calls = [], []
    typing_seen = threading.Event()

    class Typing:
        def __init__(self, bound, token):
            assert bound == route and token == "fixture"

        def send(self):
            events.append("typing")
            typing_seen.set()

    monkeypatch.setattr(foreground_typing, "BotTypingTransport", Typing)

    def text_post(platform, token, channel, body):
        events.append("text")
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": 456},
                                           "from": {"id": 123}}}
        return {"id": "1", "channel_id": "456", "author": {"id": "123"}}

    def attachment_post(bound, token, upload):
        events.append("attachment")
        assert bound == route and token == "fixture"
        assert upload.byte_count == 6 and b"report" in upload.body
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 2, "chat": {"id": 456},
                "from": {"id": 123}, "document": {"file_id": "file-2",
                    "file_name": "report.txt", "file_size": 6}}}
        return {"id": "2", "channel_id": "456", "author": {"id": "123"},
                "attachments": [{"id": "3", "filename": "report.txt", "size": 6}]}

    def progress_post(platform, token, channel, body):
        events.append("progress")
        assert token == "fixture" and channel == "456"
        if progress_mode == "unknown":
            raise TimeoutError("private-progress")
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 10, "chat": {"id": 456},
                                           "from": {"id": 123}}}
        return {"id": "10", "channel_id": "456", "author": {"id": "123"}}

    monkeypatch.setattr(foreground_progress, "BotTextTransport",
                        lambda r, t: BotTextTransport(r, t, post=progress_post))

    class Activity:
        def __init__(self, bound, token):
            self.text = BotTextTransport(bound, token, post=progress_post)

        def create(self, text, operation):
            return self.text.send(route, {"output_text": text}, operation)

        def update(self, text):
            pass

    monkeypatch.setattr(foreground_typing, "BotActivityTransport", Activity)

    monkeypatch.setattr(foreground_channels, "BotTextTransport",
                        lambda r, t: BotTextTransport(r, t, post=text_post))
    monkeypatch.setattr(foreground_channels, "BotAttachmentTransport",
                        lambda r, t: BotAttachmentTransport(r, t, post=attachment_post))

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
                    if typing:
                        assert typing_seen.wait(2)
                    model_calls.append(admitted.operation_id)
                    if progress_mode:
                        status = tools.invoke("workspace.progress", {
                            "expected": tools.workspace.identity.to_dict(),
                            "call_id": "progress-one", "text": "Preparing report"})
                        # The initial working card already reserved the shared
                        # progress budget. Throttling must not abort the answer.
                        assert status["result"]["status"] == "limited"
                    artifact = tools.artifacts.publish(
                        tools.workspace.identity, b"report", "text/plain")
                    selected = tools.invoke("workspace.attach", {
                        "expected": tools.workspace.identity.to_dict(),
                        "artifact_id": artifact.artifact_id, "filename": "report.txt"})
                    assert selected["disposition"] == "observed"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                        output_text="report attached",
                        artifacts=(ResultArtifact.from_dict(selected["result"]),))
            return Backend()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, None))
    assert main(["runtime-channel-submit", "--runtime-root", str(original.store.root),
        "--route-json", json.dumps(route.to_dict()), "--event-id", "attachment-event",
        "--goal-digest", request.goal_digest, "--input-text", "send report"]) == 0
    capsys.readouterr()
    for _ in range(2):
        assert main(["--config", str(app), "run", "--runtime-config", str(path),
                     "--max-runtime", "1"]) == 0
        assert json.loads(capsys.readouterr().out)["runtime"]["state"] == "closed"
        rows = ChannelAttachmentOutbox(RuntimeRepository(original.store.root)).read(route)
        assert len(rows) == 1 and rows[0]["state"] == "sent", events
        with RuntimeRepository(original.store.root)._read() as connection:
            progress_rows = connection.execute(
                "SELECT state FROM runtime_channel_progress").fetchall()
            assert len(progress_rows) == int(progress_mode is not None)
            if progress_mode:
                assert progress_rows[0]["state"] == progress_mode
    assert events == ((["typing"] if typing else []) +
                      (["progress"] if progress_mode else []) + ["text", "attachment"])
    assert len(model_calls) == 1
