"""Candidate readback requires a pinned backup identity and recomputed assessment."""

import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import inspect_runtime_restore, prepare_runtime_restore


@pytest.mark.parametrize("fault", ["digest", "bytes", "assessment", "activation"])
def test_candidate_inspection_rejects_changed_identity_or_receipt(tmp_path, capsys, fault):
    repo, _, _ = setup(tmp_path)
    backup, candidate = tmp_path / "backup", tmp_path / "candidate"
    manifest = backup_runtime_database(repo, backup)
    receipt = prepare_runtime_restore(backup, candidate)
    digest = manifest["sha256"]
    assert main(["runtime-restore-inspect", "--candidate-root", str(candidate),
                 "--expected-backup-digest", digest]) == 0
    assert json.loads(capsys.readouterr().out)["restore"] == receipt
    assert receipt["assessment"]["counts"]["undelivered_attachments"] == 0
    assert receipt["assessment"]["counts"]["staged_file_events"] is None
    if fault == "digest":
        digest = "f" * 64
    elif fault == "bytes":
        with (candidate / "restore-candidate.sqlite").open("ab") as output:
            output.write(b"changed")
    else:
        if fault == "activation":
            receipt["activation_allowed"] = True
        else:
            receipt["assessment"]["counts"]["undelivered_attachments"] = 9
        (candidate / "restore.json").write_text(json.dumps(receipt), encoding="utf-8")
    before = {path.name: path.read_bytes() for path in candidate.iterdir()}
    with pytest.raises(ContractError):
        inspect_runtime_restore(candidate, digest)
    assert {path.name: path.read_bytes() for path in candidate.iterdir()} == before
