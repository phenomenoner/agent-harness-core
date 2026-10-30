from __future__ import annotations

import hashlib
import importlib.util
import json
import zipfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "deployment_preflight", Path(__file__).parents[1] / "tools/deployment_preflight.py"
)
preflight = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preflight)


@pytest.fixture
def candidate(tmp_path):
    prefix = "ahc-0.3.0.dist-info/"
    entries = {
        "ahc/__init__.py": b"raise RuntimeError('never import')\n",
        prefix + "METADATA": (
            b"Metadata-Version: 2.5\nName: ahc\nVersion: 0.3.0\n"
            b"Requires-Python: >=3.14\nRequires-Dist: portalocker>=3.0\n\n"
        ),
        prefix + "entry_points.txt": b"[console_scripts]\nahc = ahc.cli:main\n",
        prefix + "WHEEL": b"Wheel-Version: 1.0\nGenerator: test\n",
        prefix + "RECORD": b"",
    }
    return tmp_path, entries


def write_wheel(root: Path, entries: dict[str, bytes]) -> Path:
    path = root / "candidate.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_preflight(root: Path, wheel: Path, expected: str | None = None):
    target = root / "install"
    return preflight.preflight(wheel, expected or digest(wheel), target)


def test_valid_wheel_reports_metadata_without_creating_target(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)

    receipt = run_preflight(root, wheel)

    assert receipt["status"] == "PASS"
    assert receipt["version"] == "0.3.0"
    assert receipt["requires_python"] == ">=3.14"
    assert receipt["requires_dist"] == ["portalocker>=3.0"]
    assert receipt["entry_point"] == "ahc = ahc.cli:main"
    assert receipt["source_members"] == 1
    assert receipt["target_created"] is False
    assert not (root / "install").exists()


def test_cli_emits_json_receipt(candidate, capsys):
    root, entries = candidate
    wheel = write_wheel(root, entries)
    target = root / "install"

    result = preflight.main(
        ["--wheel", str(wheel), "--expected-sha256", digest(wheel), "--target", str(target)]
    )

    assert result == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "PASS"
    assert receipt["wheel_name"] == "candidate.whl"
    assert not target.exists()


def test_wrong_hash_fails_without_creating_target(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)

    with pytest.raises(preflight.PreflightError, match="SHA-256") as failure:
        run_preflight(root, wheel, "0" * 64)

    assert failure.value.code == "wheel_hash_mismatch"
    assert not (root / "install").exists()


def test_occupied_target_is_rejected(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)
    target = root / "install"
    target.mkdir()
    (target / "keep.txt").write_text("keep", encoding="utf-8")

    with pytest.raises(preflight.PreflightError, match="already exists") as failure:
        preflight.preflight(wheel, digest(wheel), target)

    assert failure.value.code == "target_occupied"
    assert (target / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_missing_target_ancestor_is_rejected(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)
    target = root / "missing" / "install"

    with pytest.raises(preflight.PreflightError, match="ancestor") as failure:
        preflight.preflight(wheel, digest(wheel), target)

    assert failure.value.code == "target_ancestor_missing"
    assert not target.exists()


def test_traversal_member_is_rejected_without_creating_target(candidate):
    root, entries = candidate
    entries["../outside.py"] = b"raise RuntimeError('never execute')\n"
    wheel = write_wheel(root, entries)

    with pytest.raises(preflight.PreflightError, match="unsafe") as failure:
        run_preflight(root, wheel)

    assert failure.value.code == "unsafe_archive_path"
    assert not (root / "install").exists()
    assert not (root.parent / "outside.py").exists()


def test_duplicate_member_is_rejected_without_creating_target(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)
    with zipfile.ZipFile(wheel, "a") as archive, pytest.warns(UserWarning):
        archive.writestr("ahc/__init__.py", entries["ahc/__init__.py"])

    with pytest.raises(preflight.PreflightError, match="duplicate") as failure:
        run_preflight(root, wheel)

    assert failure.value.code == "duplicate_archive_member"
    assert not (root / "install").exists()


def test_malformed_distribution_metadata_is_rejected_without_creating_target(candidate):
    root, entries = candidate
    entries["ahc-0.3.0.dist-info/METADATA"] = (
        b"Metadata-Version: 2.5\nName ahc\nVersion: 0.3.0\nRequires-Python: >=3.14\n\n"
    )
    wheel = write_wheel(root, entries)

    with pytest.raises(preflight.PreflightError, match="malformed") as failure:
        run_preflight(root, wheel)

    assert failure.value.code == "malformed_metadata"
    assert not (root / "install").exists()


def test_oversized_metadata_is_rejected_before_decompression(candidate, monkeypatch):
    root, entries = candidate
    entries["ahc-0.3.0.dist-info/METADATA"] = b"x" * (preflight.MAX_METADATA_BYTES + 1)
    wheel = write_wheel(root, entries)

    def forbidden_read(*args, **kwargs):
        pytest.fail("metadata was decompressed before checking its declared size")

    monkeypatch.setattr(zipfile.ZipFile, "read", forbidden_read)
    with pytest.raises(preflight.PreflightError, match="too large"):
        run_preflight(root, wheel)
    assert not (root / "install").exists()
