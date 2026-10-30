"""Prepared restore preserves bytes without creating an executable runtime root."""

import json

import pytest
from test_runtime_core import setup

from ahc.cli import main
from ahc.infrastructure.runtime_backup import backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


def test_restore_prepare_cli_preserves_backup_and_excludes_activation(tmp_path, capsys):
    repo, snapshot, _ = setup(tmp_path)
    backup = tmp_path / "backup"
    manifest = backup_runtime_database(repo, backup)
    before = (backup / "runtime.sqlite").read_bytes()
    destination = tmp_path / "restore"
    assert main(["runtime-restore-prepare", "--backup-root", str(backup),
                 "--destination", str(destination)]) == 0
    receipt = json.loads(capsys.readouterr().out)["restore"]
    assert receipt["state"] == "prepared" and receipt["activation_allowed"] is False
    assert receipt["sha256"] == manifest["sha256"]
    assert receipt["assessment"]["sessions"][0]["session"] == snapshot.session.to_dict()
    assert (destination / "restore-candidate.sqlite").read_bytes() == before
    assert not (destination / WORKING_DB_FILENAME).exists()
    assert (backup / "runtime.sqlite").read_bytes() == before
    assert json.loads((destination / "restore.json").read_text()) == receipt
    with pytest.raises(FileExistsError):
        prepare_runtime_restore(backup, destination)
    assert (destination / "restore-candidate.sqlite").read_bytes() == before


def test_invalid_backup_never_creates_candidate(tmp_path):
    repo, _, _ = setup(tmp_path)
    backup = tmp_path / "backup"
    backup_runtime_database(repo, backup)
    with (backup / "runtime.sqlite").open("ab") as output:
        output.write(b"modified")
    from ahc.domain.sessions import ContractError

    with pytest.raises(ContractError):
        prepare_runtime_restore(backup, tmp_path / "restore")
    assert not (tmp_path / "restore").exists()
