"""Explicit artifact commands publish bytes, not paths, and read exact identities."""

import base64
import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.artifacts import ArtifactManifest
from ahc.domain.sessions import ContractError


def test_unhashable_lineage_is_a_contract_rejection(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    producer = store.reserve_workspace_generation(snapshot, "work")
    with pytest.raises(ContractError):
        ArtifactManifest(producer, "a" * 64, "text/plain", 0, ([],))


def test_publish_and_reopen_read_default_to_manifest_only(tmp_path, capsys):
    store, snapshot, _ = setup(tmp_path)
    producer = store.reserve_workspace_generation(snapshot, "work")
    content = tmp_path / "report.txt"
    content.write_bytes(b"workspace result\n")
    args = [
        "artifact-publish",
        "--runtime-root",
        str(store.root),
        "--producer-json",
        json.dumps(producer.to_dict()),
        "--content-file",
        str(content),
        "--media-type",
        "text/plain",
    ]
    assert main(args) == 0
    published = json.loads(capsys.readouterr().out)
    assert str(content) not in json.dumps(published)
    assert published["manifest"]["byte_count"] == len(content.read_bytes())
    read = [
        "artifact-read",
        "--runtime-root",
        str(store.root),
        "--session-json",
        json.dumps(producer.session.to_dict()),
        "--artifact-id",
        published["artifact_id"],
    ]
    assert main(read) == 0
    metadata = json.loads(capsys.readouterr().out)
    assert "content_base64" not in metadata
    assert metadata["manifest"] == published["manifest"]
    assert main(read + ["--include-content"]) == 0
    full = json.loads(capsys.readouterr().out)
    assert base64.b64decode(full["content_base64"]) == content.read_bytes()


def test_cli_rejects_oversized_content_without_publication(tmp_path, capsys):
    store, snapshot, _ = setup(tmp_path)
    producer = store.reserve_workspace_generation(snapshot, "work")
    content = tmp_path / "oversized.bin"
    content.write_bytes(b"x" * 1048577)
    assert (
        main(
            [
                "artifact-publish",
                "--runtime-root",
                str(store.root),
                "--producer-json",
                json.dumps(producer.to_dict()),
                "--content-file",
                str(content),
                "--media-type",
                "application/octet-stream",
            ]
        )
        == 2
    )
    output = capsys.readouterr().out
    assert str(content) not in output
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 0
