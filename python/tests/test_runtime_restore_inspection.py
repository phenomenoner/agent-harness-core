"""Caller-pinned transformed-candidate checks reject receipt and database drift."""

import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest
from test_runtime_core import setup

from ahc.cli.runtime_backup import run_backup_command
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_inspection import inspect_transformed_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore


@pytest.mark.parametrize("damage", [None, "receipt", "omit", "database", "mapping", "pin"])
def test_transformed_inspection(tmp_path, damage):
    repo, saved, _ = setup(tmp_path)
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    backup_receipt = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, backup_receipt["sha256"], (saved,),
                                        "b" * 64, transformed)
    database = transformed / receipt["database"]
    before = database.read_bytes()
    expected = receipt["sha256"]
    if damage == "receipt":
        receipt["activation_allowed"] = True
    elif damage == "omit":
        receipt["transitions"] = []
    elif damage in {"database", "mapping"}:
        with closing(sqlite3.connect(database)) as connection:
            connection.execute("DELETE FROM runtime_recovery_transitions")
            connection.commit()
        if damage == "mapping":
            expected = receipt["sha256"] = _digest(database)
    elif damage == "pin":
        expected = "c" * 64
    if damage:
        (transformed / "transformation.json").write_text(json.dumps(receipt), encoding="utf-8")
        with pytest.raises(ContractError):
            inspect_transformed_restore(transformed, expected, backup_receipt["sha256"], "b" * 64)
        return
    code, result = run_backup_command(SimpleNamespace(command="runtime-recovery-inspect",
        candidate_root=transformed, expected_digest=expected,
        expected_backup_digest=backup_receipt["sha256"], expected_source_digest="b" * 64))
    assert code == 0 and result["recovery"]["recovered_sessions"] == 1
    assert result["recovery"]["activation_allowed"] is False
    assert "source_snapshot" not in json.dumps(result)
    assert database.read_bytes() == before
