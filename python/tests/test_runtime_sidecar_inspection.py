"""Sidecar capture verification rejects mutated output and inventory drift."""

import pytest
from test_runtime_core import setup
from test_runtime_restore_cli import cli

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_sidecar_capture import capture_runtime_sidecars
from ahc.infrastructure.runtime_sidecar_inspection import inspect_sidecar_capture
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("damage", [None, "raw", "normalized", "extra", "sidecar", "pin"])
def test_sidecar_inspection(tmp_path, damage):
    repo, _, _ = setup(tmp_path)
    root = tmp_path / "capture"
    result = capture_runtime_sidecars(repo, tmp_path / "supervisor.lock", root)
    before = (root / "raw" / WORKING_DB_FILENAME).read_bytes()
    if damage in {"raw", "normalized"}:
        path = root / "raw" / WORKING_DB_FILENAME if damage == "raw" else root / "normalized.sqlite"
        path.write_bytes(b"changed")
    elif damage == "extra":
        (root / "raw" / "unexpected").write_bytes(b"unexpected")
    elif damage == "sidecar":
        (root / "normalized.sqlite-wal").write_bytes(b"unresolved")
    digest = "c" * 64 if damage == "pin" else result["capture_digest"]
    if damage:
        with pytest.raises(ContractError):
            inspect_sidecar_capture(root, digest, result["sha256"])
        return
    report = inspect_sidecar_capture(root, digest, result["sha256"])
    assert report["state"] == "verified" and report["activation_allowed"] is False
    assert (root / "raw" / WORKING_DB_FILENAME).read_bytes() == before


def test_sidecar_capture_and_inspection_cli(tmp_path):
    repo, _, _ = setup(tmp_path)
    root = tmp_path / "capture"
    before = (repo.root / WORKING_DB_FILENAME).read_bytes()
    receipt = cli("runtime-sidecar-capture", "--runtime-root", repo.root,
                  "--supervisor-lock", tmp_path / "supervisor.lock",
                  "--destination", root)["capture"]
    report = cli("runtime-sidecar-inspect", "--capture-root", root,
                 "--expected-capture-digest", receipt["capture_digest"],
                 "--expected-normalized-digest", receipt["sha256"])["capture"]
    assert report["state"] == "verified"
    assert (repo.root / WORKING_DB_FILENAME).read_bytes() == before
