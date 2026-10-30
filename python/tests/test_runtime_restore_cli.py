"""Real CLI process flow for disposable offline restore and pending resolution."""

import json
import subprocess
import sys

import pytest
from test_runtime_core import setup

from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_restore_activation import (
    RestoreIndeterminate,
    activate_runtime_restore,
)
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


def cli(*args, code=0):
    result = subprocess.run([sys.executable, "-m", "ahc", *map(str, args)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == code, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("pending", [False, True])
def test_operator_restore_command_flow(tmp_path, pending):
    repo, _, _ = setup(tmp_path)
    source = _digest(repo.root / WORKING_DB_FILENAME)
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = cli("runtime-backup", "--runtime-root", repo.root,
                   "--destination", backup)["backup"]
    cli("runtime-restore-prepare", "--backup-root", backup, "--destination", prepared)
    common = ["--runtime-root", repo.root, "--supervisor-lock", tmp_path / "supervisor.lock",
              "--expected-backup-digest", manifest["sha256"], "--expected-source-digest", source]
    receipt = cli("runtime-restore-transform", *common, "--candidate-root", prepared,
                  "--destination", transformed)["restore"]
    assert "source_snapshot" not in json.dumps(receipt)
    if pending:
        with pytest.raises(RestoreIndeterminate) as error:
            activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                receipt["sha256"], manifest["sha256"], source, fail_after_replace=True)
        attempt = error.value.attempt
        report = cli("runtime-restore-pending", "--runtime-root", repo.root,
                     "--expected-attempt", attempt)["pending"]
        resolution_args = ["--runtime-root", repo.root, "--supervisor-lock",
                           tmp_path / "supervisor.lock", "--expected-attempt", attempt,
                           "--expected-digest", report["observed_digest"]]
        cli("runtime-restore-reconcile", *resolution_args, "--decision", "retain-source", code=2)
        assert (repo.root / "restore-pending.json").exists()
        result = cli("runtime-restore-reconcile", *resolution_args,
                     "--decision", "accept-candidate")["restore"]
        assert result["state"] == "reconciled"
    else:
        cli("runtime-restore-activate", *common, "--candidate-root", transformed,
            "--expected-digest", "f" * 64, code=2)
        assert _digest(repo.root / WORKING_DB_FILENAME) == source
        result = cli("runtime-restore-activate", *common, "--candidate-root", transformed,
                     "--expected-digest", receipt["sha256"])["restore"]
        assert result["state"] == "activated"
    assert not (repo.root / "restore-pending.json").exists()
    assert repo.read_current("session").session.ref.generation == 2
    assert _digest(repo.root / WORKING_DB_FILENAME) == receipt["sha256"]
