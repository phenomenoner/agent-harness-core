import json
from dataclasses import replace
from email import policy
from email.parser import BytesParser

import pytest
from test_workspace_tools import setup

from ahc.application.channel_artifact_upload import prepare_artifact_upload
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_artifact_scope_and_multipart_bytes(tmp_path, platform):
    store, workspace, _ = setup(tmp_path)
    try:
        content = b"fixture-private\x00\xff\r\n"
        manifest = ArtifactRepository(store).publish(workspace.identity, content,
                                                     "application/octet-stream")
        route = ChannelRoute(platform, "123", "456", "789", workspace.identity.session)
        upload = prepare_artifact_upload(store, route, manifest.artifact_id, "報告.bin")
        assert "fixture-private" not in repr(upload)
        message = BytesParser(policy=policy.default).parsebytes(
            f"Content-Type: {upload.content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()
            + upload.body)
        parts = list(message.iter_parts())
        file = parts[-1]
        assert file.get_filename() == "報告.bin" and file.get_payload(decode=True) == content
        if platform == "discord":
            metadata = json.loads(parts[0].get_payload(decode=True))
            assert metadata["attachments"] == [{"id": 0, "filename": "報告.bin"}]
            assert metadata["allowed_mentions"] == {"parse": []}
        else:
            assert parts[0].get_payload(decode=True) == b"456"
        for name in ("../file", "bad\r\nheader", 'bad"name', "C:\\secret"):
            with pytest.raises(ContractError):
                prepare_artifact_upload(store, route, manifest.artifact_id, name)
        with pytest.raises(ContractError):
            prepare_artifact_upload(store, replace(route, session=replace(route.session,
                agent_id="other")), manifest.artifact_id, "file.bin")
    finally:
        workspace.close()
