from dataclasses import replace

import pytest
from test_workspace_tools import setup

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.application.channel_artifact_upload import prepare_artifact_upload
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", [None, "size", "channel", "author", "filename", "id_type"])
def test_artifact_response_correlation_without_retry(tmp_path, platform, fault):
    store, workspace, _ = setup(tmp_path)
    try:
        manifest = ArtifactRepository(store).publish(workspace.identity, b"report", "text/plain")
        route = ChannelRoute(platform, "123", "456", "789", workspace.identity.session)
        upload = prepare_artifact_upload(store, route, manifest.artifact_id, "report.txt")
        calls = []

        def post(bound, token, body):
            calls.append(body)
            assert bound == route and token == "fixture"
            size = 99 if fault == "size" else 6
            filename = "other.txt" if fault == "filename" else "report.txt"
            if platform == "telegram":
                channel = 999 if fault == "channel" else "456" if fault == "id_type" else 456
                return {"ok": True, "result": {"message_id": 1, "chat": {"id": channel},
                    "from": {"id": 999 if fault == "author" else 123},
                    "document": {"file_id": "file-1", "file_name": filename, "file_size": size}}}
            return {"id": "1", "channel_id": "999" if fault == "channel" else "456",
                    "author": {"id": "999" if fault == "author" else "123"},
                    "attachments": [{"id": "file" if fault == "id_type" else "2",
                                     "filename": filename, "size": size}]}

        transport = BotAttachmentTransport(route, "fixture", post=post)
        if fault:
            with pytest.raises(ContractError, match="response unverified"):
                transport.send(upload)
        else:
            receipt = transport.send(upload)
            assert receipt["artifact_id"] == manifest.artifact_id and receipt["byte_count"] == 6
        assert len(calls) == 1
        with pytest.raises(ContractError):
            transport.send(replace(upload, route=replace(route, channel_id="999")))
        assert len(calls) == 1
    finally:
        workspace.close()
