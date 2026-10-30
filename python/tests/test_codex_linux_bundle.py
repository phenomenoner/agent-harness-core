"""Archive drift and structure tests; no executable is launched or installed."""

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from ahc.adapters import codex_linux_bundle as bundle
from ahc.domain.sessions import ContractError


def fixture(tmp_path, monkeypatch, *, mutation=None):
    path = tmp_path / "linux.tar.gz"
    data = b"inert-ELF-fixture"
    info = tarfile.TarInfo("bin/codex")
    info.size = len(data)
    info.mode = 0o755
    with tarfile.open(path, "w:gz") as package:
        directory = tarfile.TarInfo("bin")
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o755
        if mutation != "missing-directory":
            package.addfile(directory)
        if mutation == "extra-directory":
            directory.name = "extra"
            package.addfile(directory)
        if mutation == "link":
            info.type = tarfile.SYMTYPE
            info.linkname = "/other/codex"
        if mutation == "mode":
            info.mode = 0o4755
        if mutation != "missing":
            package.addfile(info, io.BytesIO(b"x" * len(data) if mutation == "content" else data))
        if mutation == "duplicate":
            package.addfile(info, io.BytesIO(data))
        if mutation == "extra":
            package.addfile(tarfile.TarInfo("../unbound"), io.BytesIO())
    monkeypatch.setattr(
        bundle,
        "LINUX_CODEX_PIN",
        {
            "target": "x86_64-unknown-linux-musl",
            "package_size": path.stat().st_size,
            "package_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "directories": [{"path": "bin", "mode": 0o755}],
            "members": [
                {
                    "path": "bin/codex",
                    "size": len(data),
                    "mode": 0o755,
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            ],
        },
    )
    return path


def test_exact_archive_without_extraction(tmp_path, monkeypatch):
    path = fixture(tmp_path, monkeypatch)
    bundle.verify_linux_archive(path, target="x86_64-unknown-linux-musl")
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "mutation",
    [
        "content",
        "mode",
        "link",
        "duplicate",
        "extra",
        "missing",
        "missing-directory",
        "extra-directory",
    ],
)
def test_member_contract_independent_of_outer_digest(tmp_path, monkeypatch, mutation):
    path = fixture(tmp_path, monkeypatch, mutation=mutation)
    with pytest.raises(ContractError):
        bundle.verify_linux_archive(path, target="x86_64-unknown-linux-musl")


def test_wrong_platform_and_relative_path_do_not_fallback(tmp_path, monkeypatch):
    path = fixture(tmp_path, monkeypatch)
    with pytest.raises(ContractError, match="target mismatch"):
        bundle.verify_linux_archive(path, target="x86_64-pc-windows-msvc")
    with pytest.raises(ContractError, match="explicit archive"):
        bundle.verify_linux_archive(Path("linux.tar.gz"), target="x86_64-unknown-linux-musl")


@pytest.mark.parametrize("same_size", [True, False])
def test_outer_bytes_drift_rejected(tmp_path, monkeypatch, same_size):
    path = fixture(tmp_path, monkeypatch)
    path.write_bytes(b"x" * (path.stat().st_size if same_size else 1))
    with pytest.raises(ContractError, match="archive (size|digest) mismatch"):
        bundle.verify_linux_archive(path, target="x86_64-unknown-linux-musl")


@pytest.mark.parametrize(
    "uid,mode,parent,allowed",
    [
        (0, 0o755, False, True),
        (1000, 0o755, False, False),
        (0, 0o775, False, False),
        (0, 0o777, True, False),
        (0, 0o1777, True, True),
        (0, 0o1777, False, False),
        (1000, 0o1777, True, False),
    ],
)
def test_root_replacement_boundary(uid, mode, parent, allowed):
    from types import SimpleNamespace

    assert (
        bundle._protected_root_metadata(
            SimpleNamespace(st_uid=uid, st_mode=mode), sticky_parent=parent
        )
        is allowed
    )


def test_nonlinux_cannot_claim_root_protection(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle.sys, "platform", "win32")
    with pytest.raises(ContractError, match="requires Linux"):
        bundle.verify_linux_tree(
            tmp_path, target=bundle.LINUX_CODEX_PIN["target"], require_root_owned=True
        )
