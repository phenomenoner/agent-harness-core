import base64
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_client import setup as codex_setup
from test_codex_incoming_files import setup
from test_workspace_tools import Port

from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli.foreground_media import (
    MAX_NATIVE_IMAGE_BYTES,
    compose_foreground_media,
)
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_execution_scope import workspace_turn
from ahc.infrastructure.working_repository import WorkingSetCorrupt


def test_foreground_media_projects_verified_png_as_native_image(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
        content = b"\x89PNG\r\n\x1a\nfixture"
        event = ChannelInboundEvent(
            route,
            "update-image",
            "inspect image",
            (ChannelFile("telegram", "456", "2", "image.png", len(content), "image/png"),),
        )
        ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
        importer = ChannelFileImport(repo)
        importer.run(event, snapshot.goal.digest, workspace.identity,
                     SimpleNamespace(fetch=lambda _reference: content))
        importer.submit(event, snapshot.goal.digest)
        tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
        callback = compose_foreground_media(repo, tools)
        bound = replace(
            request,
            operation_id=event.operation_id,
            input_text=event.input_text,
            workspace_scope=workspace.identity.workspace_id,
        )
        with repo.execution_lock(), workspace_turn(repo, bound):
            projected = callback(bound)
        assert len(projected) == 1
        assert projected[0]["type"] == "image"
        assert projected[0]["url"].startswith("data:image/png;base64,iVBORw0KGgp")
    finally:
        workspace.close()


def _event(snapshot, event_id, files, *, context=None):
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    return ChannelInboundEvent(route, event_id, "inspect attachments", tuple(files), context)


def _import(repo, snapshot, workspace, event, content_by_file_id):
    ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
    ChannelFileImport(repo).run(
        event,
        snapshot.goal.digest,
        workspace.identity,
        SimpleNamespace(fetch=lambda reference: content_by_file_id[reference.file_id]),
    )
    ChannelFileImport(repo).submit(event, snapshot.goal.digest)


def _request_for(request, workspace, event):
    return replace(
        request,
        operation_id=event.operation_id,
        input_text=event.input_text,
        workspace_scope=workspace.identity.workspace_id,
    )


def _project(callback, repo, request):
    with repo.execution_lock(), workspace_turn(repo, request):
        return callback(request)


def test_foreground_media_accepts_imported_bytes_from_prior_workspace_generation(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    first = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    successor = None
    try:
        content = b"\x89PNG\r\n\x1a\nprior-generation"
        event = _event(snapshot, "restart-image", (
            ChannelFile("telegram", "456", "2", "prior.png", len(content), "image/png"),
        ))
        _import(repo, snapshot, first, event, {"2": content})
        first.close()
        successor = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
        callback = compose_foreground_media(repo, WorkspaceTools(
            successor, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, successor, event)
        projected = _project(callback, repo, bound)
        assert projected[0]["type"] == "image"
    finally:
        first.close()
        if successor is not None:
            successor.close()


def test_foreground_media_keeps_legacy_absent_inbox_empty(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = replace(request, workspace_scope=workspace.identity.workspace_id)
        assert _project(callback, repo, bound) == []
    finally:
        workspace.close()


def test_foreground_media_accepts_only_signed_native_image_types(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        entries = (
            ("2", "png", "image/png", b"\x89PNG\r\n\x1a\nvalid"),
            ("3", "jpeg", "image/jpeg", b"\xff\xd8\xffvalid"),
            ("4", "gif", "image/gif", b"GIF89avalid"),
            ("5", "webp", "image/webp", b"RIFFxxxxWEBPvalid"),
            ("6", "bad", "image/png", b"not-a-png"),
            ("7", "plain", "text/plain", b"ordinary text"),
        )
        files = tuple(
            ChannelFile("telegram", "456", file_id, f"{name}.bin", len(content), media_type)
            for file_id, name, media_type, content in entries
        )
        event = _event(snapshot, "signed-images", files, context={
            "attachments": [
                {"filename": file.filename, "media_type": file.media_type, "status": "ready"}
                for file in files
            ],
        })
        _import(repo, snapshot, workspace, event,
                {file_id: content for file_id, _, _, content in entries})
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, workspace, event)
        projected = _project(callback, repo, bound)
        images = [item for item in projected if item["type"] == "image"]
        assert [item["url"].split(";", 1)[0] for item in images] == [
            "data:image/png", "data:image/jpeg", "data:image/gif", "data:image/webp"
        ]
        assert len(images) == 4
        notice = projected[-1]
        assert notice["type"] == "text"
        assert "unsupported image or non-image byte set" in notice["text"]
        assert "ordinary text" not in notice["text"]
    finally:
        workspace.close()


def test_foreground_media_emits_notice_when_partial_image_budget_is_exceeded(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        header = b"\x89PNG\r\n\x1a\n"
        first = header + b"a" * (MAX_NATIVE_IMAGE_BYTES // 2 - len(header))
        second = header + b"b" * (MAX_NATIVE_IMAGE_BYTES // 2)
        files = (
            ChannelFile("telegram", "456", "2", "first.png", len(first), "image/png"),
            ChannelFile("telegram", "456", "3", "second.png", len(second), "image/png"),
        )
        event = _event(snapshot, "budget-images", files)
        _import(repo, snapshot, workspace, event, {"2": first, "3": second})
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, workspace, event)
        projected = _project(callback, repo, bound)
        images = [item for item in projected if item["type"] == "image"]
        decoded = [base64.b64decode(item["url"].split(",", 1)[1]) for item in images]
        assert len(images) == 1
        assert sum(map(len, decoded)) <= MAX_NATIVE_IMAGE_BYTES
        assert "partial image budget" in projected[-1]["text"]
    finally:
        workspace.close()


def test_foreground_media_rejects_cross_operation_and_session_projection(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        content = b"\x89PNG\r\n\x1a\nowned"
        event = _event(snapshot, "bound-image", (
            ChannelFile("telegram", "456", "2", "owned.png", len(content), "image/png"),
        ))
        _import(repo, snapshot, workspace, event, {"2": content})
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, workspace, event)
        with repo.execution_lock(), workspace_turn(repo, replace(bound, operation_id="foreign")):
            with pytest.raises(ContractError, match="operation binding"):
                callback(bound)
        with repo.execution_lock(), workspace_turn(repo, bound):
            with pytest.raises(ContractError, match="workspace binding"):
                callback(replace(bound, session=bound.session.successor()))
    finally:
        workspace.close()


def test_foreground_media_rejects_corrupt_import_binding(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        content = b"\x89PNG\r\n\x1a\ncorrupt-binding"
        event = _event(snapshot, "corrupt-import", (
            ChannelFile("telegram", "456", "2", "corrupt.png", len(content), "image/png"),
        ))
        _import(repo, snapshot, workspace, event, {"2": content})
        repo._mutate(lambda connection: connection.execute(
            "UPDATE runtime_channel_file_imports SET event_digest=? WHERE operation_id=?",
            ("b" * 64, event.operation_id),
        ))
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, workspace, event)
        with pytest.raises(WorkingSetCorrupt, match="file import binding corrupted"):
            _project(callback, repo, bound)
    finally:
        workspace.close()


def test_foreground_media_rejects_corrupt_event_context_binding(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        content = b"\x89PNG\r\n\x1a\ncorrupt-context"
        file = ChannelFile("telegram", "456", "2", "context.png", len(content), "image/png")
        event = _event(snapshot, "corrupt-context", (file,), context={
            "attachments": [{"filename": file.filename, "media_type": file.media_type,
                              "status": "ready"}],
        })
        _import(repo, snapshot, workspace, event, {"2": content})
        repo._mutate(lambda connection: connection.execute(
            "UPDATE runtime_channel_context SET context_digest=? WHERE operation_id=?",
            ("b" * 64, event.operation_id),
        ))
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        bound = _request_for(request, workspace, event)
        with pytest.raises(WorkingSetCorrupt, match="channel context binding corrupted"):
            _project(callback, repo, bound)
    finally:
        workspace.close()


def test_codex_client_appends_native_projection_before_submitted(tmp_path):
    peer, client, request = codex_setup()
    seen = []
    payload = base64.b64encode(b"\x89PNG\r\n\x1a\nfixture").decode("ascii")

    def media(bound):
        assert bound is request
        seen.append(client.submitted)
        return [{"type": "image", "url": f"data:image/png;base64,{payload}"}]

    client.turn_media = media
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert seen == [False]
    turn = next(item for item in peer.sent if item.get("method") == "turn/start")
    assert turn["params"]["input"][-1]["type"] == "image"


def test_codex_client_rejects_callback_error_before_any_submission(tmp_path):
    peer, client, request = codex_setup()

    def media(_request):
        raise ContractError("projection unavailable")

    client.turn_media = media
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted"
    assert client.submitted is False
    assert peer.sent == []


def test_codex_client_checks_full_frame_before_submitted(tmp_path):
    peer, client, request = codex_setup()
    content = b"\x89PNG\r\n\x1a\n" + b"x" * 800_000
    payload = base64.b64encode(content).decode("ascii")
    client.turn_media = lambda _request: [{
        "type": "image", "url": f"data:image/png;base64,{payload}"
    }]
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted"
    assert client.submitted is False
    assert [item.get("method") for item in peer.sent] == [
        "initialize", "initialized", "thread/start"
    ]
