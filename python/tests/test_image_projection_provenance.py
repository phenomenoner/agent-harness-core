import pytest
from test_foreground_media import (
    ArtifactRepository,
    ChannelFile,
    Port,
    RuntimeService,
    WorkspaceTools,
    _event,
    _import,
    _project,
    _request_for,
    compose_foreground_media,
    setup,
)

from ahc.infrastructure.working_repository import WorkingSetCorrupt


def image_event(snapshot, name):
    return _event(snapshot, name, [
        ChannelFile("telegram", "456", name, "same.png", 12, "image/png")])


def test_same_metadata_image_swap_is_rejected(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        first, second = image_event(snapshot, "first"), image_event(snapshot, "second")
        _import(repo, snapshot, workspace, first, {"first": b"\x89PNG\r\n\x1a\nAAAA"})
        _import(repo, snapshot, workspace, second, {"second": b"\x89PNG\r\n\x1a\nBBBB"})
        def swap(c):
            selected = c.execute(
                "SELECT selections_json FROM runtime_channel_file_imports WHERE operation_id=?",
                (second.operation_id,)).fetchone()[0]
            c.execute("UPDATE runtime_channel_file_imports SET selections_json=? "
                      "WHERE operation_id=?",
                      (selected, first.operation_id))
        repo._mutate(swap)
        callback = compose_foreground_media(
            repo, WorkspaceTools(workspace, artifacts=ArtifactRepository(repo)))
        with pytest.raises(WorkingSetCorrupt, match="file import binding corrupted"):
            _project(callback, repo, _request_for(request, workspace, first))
    finally:
        workspace.close()


@pytest.mark.parametrize("old_schema", [False, True])
def test_legacy_import_is_readable_but_not_native_and_new_import_gets_seal(tmp_path, old_schema):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        event = image_event(snapshot, "old")
        _import(repo, snapshot, workspace, event, {"old": b"\x89PNG\r\n\x1a\nAAAA"})
        def make_legacy(c):
            if old_schema:
                c.execute("ALTER TABLE runtime_channel_file_imports DROP COLUMN selection_digest")
            else:
                c.execute("UPDATE runtime_channel_file_imports SET selection_digest=NULL")
        repo._mutate(make_legacy)
        artifacts = ArtifactRepository(repo)
        callback = compose_foreground_media(repo, WorkspaceTools(workspace, artifacts=artifacts))
        projected = _project(callback, repo, _request_for(request, workspace, event))
        assert len(projected) == 1 and projected[0]["type"] == "text"
        assert "upload again" in projected[0]["text"]
        new = image_event(snapshot, "new")
        _import(repo, snapshot, workspace, new, {"new": b"\x89PNG\r\n\x1a\nBBBB"})
        with repo._read() as c:
            rows = {row[0]: row[1] for row in c.execute(
                "SELECT operation_id,selection_digest FROM runtime_channel_file_imports")}
        assert rows[event.operation_id] is None  # Never fabricate provenance for retained data.
        assert len(rows[new.operation_id]) == 64
        assert _project(callback, repo, _request_for(request, workspace, new))[0]["type"] == "image"
    finally:
        workspace.close()
