"""CLI normalization/resume and refusal to overwrite a post-crash foreign file."""

import json
import subprocess
import sys

import pytest
from test_runtime_core import setup
from test_runtime_restore_cli import cli
from test_runtime_sidecar_capture import CHILD
from test_runtime_sidecar_transition import TRANSITION

from ahc.infrastructure.runtime_sidecar_capture import capture_runtime_sidecars
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("mode", ["normalize", "resume", "installed", "foreign"])
def test_sidecar_operator_commands_preserve_conflicting_bytes(tmp_path, mode):
    repo, saved, _ = setup(tmp_path)
    active = repo.root / WORKING_DB_FILENAME
    crashed = subprocess.run([sys.executable, "-c", CHILD, str(active), "WAL"],
                             capture_output=True, timeout=15)
    assert crashed.returncode == 23
    capture = tmp_path / "capture"
    receipt = capture_runtime_sidecars(repo, repo.root / "supervisor.lock", capture)
    common = ["--runtime-root", repo.root, "--supervisor-lock", repo.root / "supervisor.lock"]
    if mode == "normalize":
        result = cli("runtime-sidecar-normalize", *common, "--capture-root", capture,
                     "--expected-capture-digest", receipt["capture_digest"],
                     "--expected-normalized-digest", receipt["sha256"])["restore"]
    else:
        crashed = subprocess.run([sys.executable, "-c", TRANSITION, str(repo.root), str(capture),
            receipt["capture_digest"], receipt["sha256"],
            "installed" if mode == "installed" else "retired-db"],
            capture_output=True, timeout=15)
        assert crashed.returncode == 23, crashed.stderr
        marker = repo.root / "sidecar-pending.json"
        pending = marker.read_bytes()
        attempt = json.loads(pending)["attempt"]
        status = cli("runtime-sidecar-status", *common)["restore"]
        assert status["attempt"] == attempt and status["state"] == "pending"
        assert status["normalized_installed"] is (mode == "installed")
        assert not status["resumed"] and marker.read_bytes() == pending
        cli("runtime-sidecar-status", *common, "--expected-attempt", "f" * 32, code=2)
        if mode == "foreign":
            active.write_bytes(b"new external file")
            retained = repo.root / "sidecar-attempts" / attempt / "original"
            before = {path.name: path.read_bytes() for path in retained.iterdir()}
            status = cli("runtime-sidecar-status", *common,
                         "--expected-attempt", attempt)["restore"]
            assert status["state"] == "conflict" and status["next_command"] is None
            cli("runtime-sidecar-resume", *common, "--expected-attempt", attempt, code=2)
            assert active.read_bytes() == b"new external file"
            assert marker.read_bytes() == pending
            assert {path.name: path.read_bytes() for path in retained.iterdir()} == before
            return
        cli("runtime-sidecar-resume", *common, "--expected-attempt", "f" * 32, code=2)
        assert marker.read_bytes() == pending and active.exists() is (mode == "installed")
        result = cli("runtime-sidecar-resume", *common, "--expected-attempt", attempt)["restore"]
    assert result["state"] == "normalized"
    assert repo.read_current("session") == saved
    assert not (repo.root / "sidecar-pending.json").exists()
    assert cli("runtime-sidecar-status", *common)["restore"]["state"] == "no-pending-transition"
