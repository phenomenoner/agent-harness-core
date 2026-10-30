import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.infrastructure.artifact_repository import ArtifactRepository


@pytest.mark.parametrize("concurrency", [1, 4])
def test_real_store_batch_manifest_missing_result_and_rejection(tmp_path, capsys, concurrency):
    store, snapshot, _ = setup(tmp_path)
    producer = store.reserve_workspace_generation(snapshot, "work")
    artifact = ArtifactRepository(store).publish(producer, b"private content", "text/plain")
    rows = [
        {"call_id": "artifact", "capability_id": "runtime.artifact.manifest",
         "arguments": {"artifact_id": artifact.artifact_id}},
        {"call_id": "missing", "capability_id": "runtime.result.read",
         "arguments": {"operation_id": "missing"}},
        {"call_id": "bad", "capability_id": "runtime.result.read", "arguments": {}},
        {"call_id": "write", "capability_id": "runtime.submit", "arguments": {}},
    ]
    args = ["runtime-read-batch", "--runtime-root", str(store.root),
            "--session-json", json.dumps(producer.session.to_dict()),
            "--calls-json", json.dumps(rows), "--concurrency", str(concurrency)]
    assert main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert [row["state"] for row in output["receipts"]] == [
        "success", "success", "rejected", "rejected"]
    assert "values" not in output
    assert "private content" not in str(output)
    assert main(args + ["--include-values"]) == 0
    full = json.loads(capsys.readouterr().out)
    assert full["values"][0] == artifact.to_dict()
    assert full["values"][1:] == [None, None, None]
    assert "private content" not in str(full)


def test_absent_root_is_not_initialized(tmp_path, capsys):
    from ahc.domain.sessions import VirtualSessionIdentity

    root = tmp_path / "absent"
    session = VirtualSessionIdentity.initial("s", "a", "r")
    rows = [{"call_id": "one", "capability_id": "runtime.result.read",
             "arguments": {"operation_id": "missing"}}]
    assert main(["runtime-read-batch", "--runtime-root", str(root),
                 "--session-json", json.dumps(session.to_dict()),
                 "--calls-json", json.dumps(rows)]) == 1
    assert json.loads(capsys.readouterr().out)["ok"] is False
    assert not root.exists()


def test_other_session_cannot_read_artifact_manifest(tmp_path, capsys):
    from ahc.domain.sessions import VirtualSessionIdentity

    store, snapshot, _ = setup(tmp_path)
    producer = store.reserve_workspace_generation(snapshot, "work")
    artifact = ArtifactRepository(store).publish(producer, b"private", "text/plain")
    other = VirtualSessionIdentity.initial("other", "agent", "route")
    rows = [{"call_id": "one", "capability_id": "runtime.artifact.manifest",
             "arguments": {"artifact_id": artifact.artifact_id}}]
    assert main(["runtime-read-batch", "--runtime-root", str(store.root),
                 "--session-json", json.dumps(other.to_dict()), "--calls-json", json.dumps(rows),
                 "--include-values"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["receipts"][0]["state"] == "unknown"
    assert output["values"] == [None]
