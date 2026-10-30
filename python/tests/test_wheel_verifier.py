from __future__ import annotations

import importlib.util
import zipfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "wheel_verifier", Path(__file__).parents[1] / "tools/verify_wheel.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.fixture
def candidate(tmp_path):
    (tmp_path / "src/ahc").mkdir(parents=True)
    (tmp_path / "src/ahc/__init__.py").write_bytes(b"raise RuntimeError('never import')\n")
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="ahc"\nversion="0.3.0"\nrequires-python=">=3.14"\n'
        'dependencies=["portalocker>=3.0"]\n[project.scripts]\nahc="ahc.cli:main"\n'
    )
    prefix = "ahc-0.3.0.dist-info/"
    entries = {
        "ahc/__init__.py": b"raise RuntimeError('never import')\n",
        prefix + "METADATA": (
            b"Name: ahc\nVersion: 0.3.0\nRequires-Python: >=3.14\n"
            b"Requires-Dist: portalocker>=3.0\n\n"
        ),
        prefix + "entry_points.txt": b"[console_scripts]\nahc = ahc.cli:main\n",
        prefix + "WHEEL": b"Wheel-Version: 1.0\n",
        prefix + "RECORD": b"",
    }
    return tmp_path, entries


def write_wheel(root, entries):
    path = root / "candidate.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return path


def test_exact_members_pass_without_import(candidate):
    root, entries = candidate
    assert verifier.verify(write_wheel(root, entries), root)["source_members"] == 1


@pytest.mark.parametrize("change", ["missing", "changed", "extra", "dependency", "entrypoint"])
def test_discriminates_mismatched_candidate(candidate, change):
    root, entries = candidate
    if change == "missing":
        del entries["ahc/__init__.py"]
    elif change == "changed":
        entries["ahc/__init__.py"] = b"pass\n"
    elif change == "extra":
        entries["ahc/unexpected.json"] = b"{}"
    elif change == "dependency":
        key = "ahc-0.3.0.dist-info/METADATA"
        entries[key] = entries[key].replace(b"portalocker>=3.0", b"portalocker>=99")
    else:
        entries["ahc-0.3.0.dist-info/entry_points.txt"] = b"[console_scripts]\nahc=bad:main"
    with pytest.raises(ValueError):
        verifier.verify(write_wheel(root, entries), root)


def test_duplicate_member_rejected(candidate):
    root, entries = candidate
    wheel = write_wheel(root, entries)
    with zipfile.ZipFile(wheel, "a") as archive, pytest.warns(UserWarning):
        archive.writestr("ahc/__init__.py", entries["ahc/__init__.py"])
    with pytest.raises(ValueError, match="duplicate"):
        verifier.verify(wheel, root)


@pytest.mark.parametrize("change", [None, "missing", "version", "marker", "duplicate", "group"])
def test_optional_dependencies_are_verified(candidate, change):
    root, entries = candidate
    with (root / "pyproject.toml").open("a") as stream:
        stream.write('\n[project.optional-dependencies]\n'
                     'discord=["websockets>=17.0.1,<18"]\n')
    dependency = b'Requires-Dist: websockets<18,>=17.0.1; extra == "discord"\n'
    if change == "missing":
        dependency = b""
    elif change == "version":
        dependency = dependency.replace(b"17.0.1", b"17.0.2")
    elif change == "marker":
        dependency = dependency.replace(b'discord', b'mcp')
    elif change == "duplicate":
        dependency *= 2
    group = b"Provides-Extra: discord\n" if change != "group" else b""
    key = "ahc-0.3.0.dist-info/METADATA"
    entries[key] = entries[key].rstrip(b"\n") + b"\n" + group + dependency + b"\n"
    wheel = write_wheel(root, entries)
    if change is None:
        assert verifier.verify(wheel, root)["status"] == "PASS"
    else:
        with pytest.raises(ValueError, match="dependencies|dependency groups"):
            verifier.verify(wheel, root)
