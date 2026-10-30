"""Bundle pin is exact and never falls back to a global executable."""

import hashlib

import pytest

from ahc.adapters import codex_bundle
from ahc.domain.sessions import ContractError


def fixture(tmp_path, monkeypatch):
    root = tmp_path / "bundle"
    root.mkdir()
    binary = root / "codex.exe"
    binary.write_bytes(b"fixture-executable-not-executed")
    pin = {
        "entrypoint": "codex.exe",
        "members": [
            {
                "path": "codex.exe",
                "size": binary.stat().st_size,
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            }
        ],
    }
    monkeypatch.setattr(codex_bundle, "CODEX_PIN", pin)
    return root, binary


def test_exact_bundle_is_resolved_without_execution(tmp_path, monkeypatch):
    root, binary = fixture(tmp_path, monkeypatch)
    assert codex_bundle.verify_bundle(root) == binary


@pytest.mark.parametrize("mutation", ["content", "extra", "missing"])
def test_bundle_drift_has_no_fallback(tmp_path, monkeypatch, mutation):
    root, binary = fixture(tmp_path, monkeypatch)
    if mutation == "content":
        binary.write_bytes(b"x" * binary.stat().st_size)
    elif mutation == "extra":
        (root / "unbound.exe").write_bytes(b"fixture")
    else:
        binary.rename(root / "renamed.exe")
    with pytest.raises(ContractError):
        codex_bundle.verify_bundle(root)


def test_relative_path_is_not_resolved_through_current_directory():
    from pathlib import Path

    with pytest.raises(ContractError):
        codex_bundle.verify_bundle(Path("codex"))
