"""Extracted staging drift checks; no native Linux execution claim on Windows."""

import hashlib
import os
import sys

import pytest

from ahc.adapters import codex_linux_bundle as bundle
from ahc.domain.sessions import ContractError


def fixture(tmp_path, monkeypatch):
    root = tmp_path / "tree"
    (root / "bin").mkdir(parents=True)
    binary = root / "bin/codex"
    binary.write_bytes(b"inert-not-executed")
    binary.chmod(0o755)
    (root / "bin").chmod(0o755)
    monkeypatch.setattr(bundle, "LINUX_CODEX_PIN", {
        "target": "x86_64-unknown-linux-musl",
        "directories": [{"path": "bin", "mode": 0o755}],
        "members": [{"path": "bin/codex", "size": binary.stat().st_size,
                     "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "mode": 0o755}],
    })
    return root, binary


def test_exact_tree_reports_permission_evidence_honestly(tmp_path, monkeypatch):
    root, _ = fixture(tmp_path, monkeypatch)
    result = bundle.verify_linux_tree(root, target="x86_64-unknown-linux-musl")
    assert result.root == root
    assert result.permissions_checked == (sys.platform == "linux")


@pytest.mark.parametrize("mutation", ["content", "missing", "extra-file", "extra-dir", "hardlink"])
def test_tree_drift_rejected(tmp_path, monkeypatch, mutation):
    root, binary = fixture(tmp_path, monkeypatch)
    if mutation == "content":
        binary.write_bytes(b"x" * binary.stat().st_size)
    elif mutation == "missing":
        binary.unlink()
    elif mutation == "extra-file":
        (root / "unknown").write_text("extra")
    elif mutation == "extra-dir":
        (root / "unknown").mkdir()
    else:
        os.link(binary, tmp_path / "outside-hardlink")
    with pytest.raises(ContractError):
        bundle.verify_linux_tree(root, target="x86_64-unknown-linux-musl")


def test_redirect_rejected_before_reading_target(tmp_path, monkeypatch):
    root, binary = fixture(tmp_path, monkeypatch)
    original = binary.read_bytes()
    binary.unlink()
    outside = tmp_path / "outside"
    outside.write_bytes(original)
    try:
        binary.symlink_to(outside)
    except OSError:
        pytest.skip("native symlink creation unavailable")
    with pytest.raises(ContractError, match="redirect"):
        bundle.verify_linux_tree(root, target="x86_64-unknown-linux-musl")


def test_wrong_target_does_not_fallback(tmp_path, monkeypatch):
    root, _ = fixture(tmp_path, monkeypatch)
    with pytest.raises(ContractError, match="target mismatch"):
        bundle.verify_linux_tree(root, target="x86_64-pc-windows-msvc")
