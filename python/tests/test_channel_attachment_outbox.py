"""Real database attachment delivery, ambiguity and owner fencing."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_result_artifact_commit import prepared

from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy


def pending(tmp_path):
    repo, snapshot, request, route, workspace, observation = prepared(tmp_path)
    repo.begin(snapshot, request)
    result = accept_observation(request, observation)
    repo.seal_observation(request, result)
    repo.finish(request, result)
    return repo, request, route, workspace


@pytest.mark.parametrize("failure", [False, True])
def test_one_upload_reopen_and_no_replay(tmp_path, failure):
    repo, request, route, workspace = pending(tmp_path)
    calls = []
    try:
        class Transport:
            def send(self, upload):
                calls.append(upload)
                assert upload.route == route and b"report" in upload.body
                assert upload.filename == "report.txt" and upload.byte_count == 6
                with pytest.raises(WorkingWriterBusy):
                    ChannelAttachmentOutbox(RuntimeRepository(repo.root)).reconcile(route)
                if failure:
                    raise TimeoutError("PRIVATE-FIXTURE-ERROR")
                return {"message_id": "1", "attachment_id": "2"}

        delivery = ChannelAttachmentDelivery(ChannelAttachmentOutbox(repo), route, Transport())
        record = delivery.deliver(request.operation_id, 0, "claim")
        assert record["state"] == ("unknown" if failure else "sent")
        assert "PRIVATE" not in str(record)
        reopened = ChannelAttachmentOutbox(RuntimeRepository(repo.root))
        assert reopened.read(route) == (record,)
        assert reopened.reconcile(route) == (record,)
        with pytest.raises(ContractError):
            ChannelAttachmentDelivery(reopened, route, Transport()).deliver(
                request.operation_id, 0, "new-claim")
        assert len(calls) == 1
    finally:
        workspace.close()


def test_lost_claim_becomes_unknown_and_old_token_cannot_ack(tmp_path):
    repo, request, route, workspace = pending(tmp_path)
    try:
        outbox = ChannelAttachmentOutbox(repo)
        outbox.claim(route, request.operation_id, 0, "lost")
        reopened = ChannelAttachmentOutbox(RuntimeRepository(repo.root))
        assert reopened.reconcile(route)[0]["state"] == "unknown"
        with pytest.raises(ContractError):
            reopened.acknowledge(route, request.operation_id, 0, "lost", "late-receipt")
        with pytest.raises(ContractError):
            reopened.claim(route, request.operation_id, 0, "new")
    finally:
        workspace.close()


def test_wrong_route_and_stale_generation_rejected(tmp_path):
    repo, request, route, workspace = pending(tmp_path)
    try:
        outbox = ChannelAttachmentOutbox(repo)
        with pytest.raises(ContractError):
            outbox.claim(replace(route, user_id="other"), request.operation_id, 0, "claim")
        assert outbox.read(route)[0]["state"] == "pending"
        WorkingContinuityService(repo).continue_generation(
            repo.read_current("session"), route.session.successor(), "compaction")
        with pytest.raises(WorkingSetConflict):
            outbox.claim(route, request.operation_id, 0, "claim")
        assert outbox.read(route)[0]["state"] == "pending"
    finally:
        workspace.close()


@pytest.mark.parametrize("text_failure", [False, True])
def test_configured_idle_delivery_orders_text_before_attachment(
    tmp_path, monkeypatch, text_failure
):
    from ahc.cli import foreground_channels

    repo, request, route, workspace = pending(tmp_path)
    calls = []
    try:
        class Text:
            def __init__(self, *args):
                pass

            def send(self, *args):
                calls.append("text")
                if text_failure:
                    raise TimeoutError()
                return "text-receipt"

        class Attachment:
            def __init__(self, *args):
                pass

            def send(self, upload):
                calls.append("attachment")
                assert upload.route == route
                return {"message_id": "2"}

        monkeypatch.setattr(foreground_channels, "BotTextTransport", Text)
        monkeypatch.setattr(foreground_channels, "BotAttachmentTransport", Attachment)
        monkeypatch.setenv("AHC_ATTACHMENT_FIXTURE", "fixture")
        config = SimpleNamespace(to_dict=lambda: {"channels": [{"route": route.to_dict(),
            "enabled": True, "token_env": "AHC_ATTACHMENT_FIXTURE"}]})
        callback = foreground_channels.compose_channels(config, repo)
        callback(cancelled=lambda: True)
        assert calls == []
        callback(cancelled=lambda: False)
        assert calls == ["text"]
        callback(cancelled=lambda: False)
        assert calls == (["text"] if text_failure else ["text", "attachment"])
        reopened = RuntimeRepository(repo.root)
        foreground_channels.compose_channels(config, reopened)(cancelled=lambda: False)
        assert len(calls) == (1 if text_failure else 2)
        assert ChannelAttachmentOutbox(reopened).read(route)[0]["state"] == (
            "pending" if text_failure else "sent")
    finally:
        workspace.close()
