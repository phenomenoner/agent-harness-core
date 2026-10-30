"""Compare a wheel with this checkout without importing or installing its code."""

from __future__ import annotations

import argparse
import hashlib
import json
import tomllib
import zipfile
from configparser import ConfigParser
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

from packaging.markers import Marker
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

PROJECT = Path(__file__).resolve().parents[1]


def requirements(project: dict) -> list[Requirement]:
    expected = [Requirement(value) for value in project["dependencies"]]
    for extra, values in project.get("optional-dependencies", {}).items():
        for value in values:
            requirement = Requirement(value)
            marker = f'extra == "{canonicalize_name(extra)}"'
            if requirement.marker:
                marker = f"({requirement.marker}) and {marker}"
            requirement.marker = Marker(marker)
            expected.append(requirement)
    return expected


def verify(wheel: Path, project_root: Path = PROJECT) -> dict:
    project = tomllib.loads((project_root / "pyproject.toml").read_text("utf-8"))["project"]
    source_root = project_root / "src"
    source = {
        path.relative_to(source_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (source_root / "ahc").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    if not source:
        raise ValueError("source package is empty")
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("duplicate archive member")
        metadata_root = f"{project['name']}-{project['version']}.dist-info/"
        allowed_metadata = {
            metadata_root + name
            for name in (
                "METADATA",
                "WHEEL",
                "entry_points.txt",
                "RECORD",
            )
        }
        for name in names:
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or "\\" in name:
                raise ValueError("unsafe archive member")
            if name not in source and name not in allowed_metadata:
                raise ValueError(f"unexpected archive member: {name}")
        members = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in names
            if name.startswith("ahc/")
        }
        if members != source:
            raise ValueError("wheel source members or content differ from checkout")
        if not allowed_metadata.issubset(names):
            raise ValueError("required package metadata is missing")
        metadata = BytesParser().parsebytes(archive.read(metadata_root + "METADATA"))
        for field, expected in (
            ("Name", project["name"]),
            ("Version", project["version"]),
            ("Requires-Python", project["requires-python"]),
        ):
            if metadata.get_all(field) != [expected]:
                raise ValueError(f"package metadata mismatch: {field}")
        actual = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
        expected = requirements(project)
        # Requirement equality normalizes names, specifier order and marker formatting.
        # Remove one occurrence at a time so duplicate declarations still fail.
        for requirement in actual:
            if requirement not in expected:
                raise ValueError("runtime dependencies differ from pyproject.toml")
            expected.remove(requirement)
        if expected:
            raise ValueError("runtime dependencies differ from pyproject.toml")
        extras = [canonicalize_name(value) for value in metadata.get_all("Provides-Extra", [])]
        expected_extras = map(canonicalize_name, project.get("optional-dependencies", {}))
        if sorted(extras) != sorted(expected_extras):
            raise ValueError("optional dependency groups differ from pyproject.toml")
        entries = ConfigParser()
        entries.optionxform = str
        entries.read_string(archive.read(metadata_root + "entry_points.txt").decode("utf-8"))
        if (
            entries.sections() != ["console_scripts"]
            or dict(entries["console_scripts"]) != project["scripts"]
        ):
            raise ValueError("console entry points differ from pyproject.toml")
    return {
        "status": "PASS",
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "source_members": len(source),
        "archive_members": len(names),
        "installation_performed": False,
        "package_code_executed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    try:
        result = verify(args.wheel)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.exit(1, f"wheel verification failed: {error}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
