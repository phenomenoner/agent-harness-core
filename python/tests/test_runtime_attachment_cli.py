"""Local attachment operations use the same durable claims as foreground delivery."""

import json

import pytest
from test_channel_attachment_outbox import pending

from ahc.cli import main, runtime_channels
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox


@pytest.mark.parametrize("failure", [False, True])
def test_cli_inspect_explicit_send_and_no_retry(tmp_path, monkeypatch, capsys, failure):
    repo, request, route, workspace = pending(tmp_path)
    common = ["--runtime-root", str(repo.root), "--route-json", json.dumps(route.to_dict())]
    calls = []
    try:
        send = ["runtime-channel-attachment-send", *common, "--operation-id", request.operation_id,
                "--ordinal", "0", "--token-env", "AHC_ATTACHMENT_CLI_FIXTURE"]
        monkeypatch.delenv("AHC_ATTACHMENT_CLI_FIXTURE", raising=False)
        assert main(send) == 2
        capsys.readouterr()
        assert main(["runtime-channel-attachments", *common]) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["attachments"][0]["state"] == "pending"
        assert report["platform_readback"] == "not-performed"

        class Transport:
            def __init__(self, bound, token):
                assert bound == route and token == "fixture-private"

            def send(self, upload):
                calls.append(upload)
                if failure:
                    raise TimeoutError("fixture-private")
                return {"message_id": "1", "attachment_id": "2"}

        monkeypatch.setattr(runtime_channels, "BotAttachmentTransport", Transport)
        monkeypatch.setenv("AHC_ATTACHMENT_CLI_FIXTURE", "fixture-private")
        assert main(send) == (2 if failure else 0)
        output = capsys.readouterr().out
        assert "fixture-private" not in output
        assert json.loads(output)["delivery"]["state"] == ("unknown" if failure else "sent")
        assert main(send) == 2
        capsys.readouterr()
        assert len(calls) == 1
    finally:
        workspace.close()


def test_cli_reconcile_does_not_read_credentials_or_retry(tmp_path, monkeypatch, capsys):
    repo, request, route, workspace = pending(tmp_path)
    try:
        outbox = ChannelAttachmentOutbox(repo)
        outbox.claim(route, request.operation_id, 0, "abandoned")
        monkeypatch.delenv("AHC_ATTACHMENT_CLI_FIXTURE", raising=False)
        common = ["--runtime-root", str(repo.root), "--route-json", json.dumps(route.to_dict())]
        assert main(["runtime-channel-attachments-reconcile", *common]) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["attachments"][0]["state"] == "unknown"
        assert report["automatic_retry"] is False
        assert report["platform_readback"] == "not-performed"
        assert main(["runtime-channel-attachments", *common]) == 0
        assert json.loads(capsys.readouterr().out)["attachments"] == report["attachments"]
    finally:
        workspace.close()
