"""Validate an AHC wheel and an empty installation target without installing it."""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import stat
import zipfile
from collections.abc import Sequence
from email import policy
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

PROJECT_NAME = "ahc"
SHA256_LENGTH = 64
MAX_ARCHIVE_MEMBERS = 100_000
MAX_METADATA_BYTES = 1_048_576
MAX_RECEIPT_TEXT = 256
MAX_REQUIREMENTS = 128

_HEX_DIGEST = re.compile(r"[0-9a-fA-F]{64}\Z")
_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+)+\Z")
_ENTRY_POINT = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_.]*\Z")
_DRIVE_PATH = re.compile(r"[A-Za-z]:")


class PreflightError(ValueError):
    """A bounded, user-facing preflight failure."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _clean_text(value: str, *, limit: int = MAX_RECEIPT_TEXT) -> str:
    """Return a bounded single-line value suitable for a receipt."""

    if any(ord(character) < 32 and character not in "\t" for character in value):
        raise PreflightError("malformed_metadata", "metadata contains a control character")
    value = " ".join(value.split())
    if not value or len(value) > limit:
        raise PreflightError("malformed_metadata", "metadata field is empty or too long")
    return value


def _read_utf8(data: bytes, label: str) -> str:
    if len(data) > MAX_METADATA_BYTES:
        raise PreflightError("malformed_metadata", f"{label} is too large")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreflightError("malformed_metadata", f"{label} is not UTF-8") from error
    if "\x00" in text:
        raise PreflightError("malformed_metadata", f"{label} contains a NUL")
    return text


def _required_header(message, field: str) -> str:
    values = message.get_all(field, [])
    if len(values) != 1:
        raise PreflightError("malformed_metadata", f"{field} must occur exactly once")
    return _clean_text(values[0])


def _parse_headers(data: bytes, label: str):
    text = _read_utf8(data, label)
    message = BytesParser(policy=policy.default).parsebytes(data)
    if message.defects:
        raise PreflightError("malformed_metadata", f"{label} has malformed headers")
    return message, text


def _normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def _archive_member_error(name: str) -> None:
    if not name or "\x00" in name:
        raise PreflightError("unsafe_archive_path", "archive member name is invalid")
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or name.startswith(("/", "\\"))
        or "\\" in name
        or _DRIVE_PATH.match(name)
        or ".." in path.parts
    ):
        raise PreflightError("unsafe_archive_path", "archive member path is unsafe")
    if any(ord(character) < 32 for character in name):
        raise PreflightError(
            "unsafe_archive_path", "archive member name contains a control character"
        )


def _is_reparse_point(path: Path) -> bool:
    try:
        result = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise PreflightError("target_unreadable", "target path could not be inspected") from error
    attributes = getattr(result, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _absolute_path(path: Path) -> Path:
    # abspath normalizes the lexical path without resolving symlink ancestors.
    return Path(os.path.abspath(os.fspath(path)))


def _validate_target(target: Path) -> None:
    target = _absolute_path(target)
    try:
        if target.exists() or target.is_symlink() or _is_reparse_point(target):
            raise PreflightError("target_occupied", "target already exists")
    except OSError as error:
        raise PreflightError("target_unreadable", "target state could not be inspected") from error

    ancestor = target.parent
    while True:
        try:
            if not ancestor.exists():
                raise PreflightError("target_ancestor_missing", "target ancestor does not exist")
            if ancestor.is_symlink() or _is_reparse_point(ancestor):
                raise PreflightError(
                    "target_ancestor_unsafe", "target ancestor is a link or reparse point"
                )
            if not ancestor.is_dir():
                raise PreflightError(
                    "target_ancestor_invalid", "target ancestor is not a directory"
                )
        except OSError as error:
            raise PreflightError(
                "target_unreadable", "target ancestor could not be inspected"
            ) from error
        if ancestor.parent == ancestor:
            break
        ancestor = ancestor.parent


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise PreflightError("wheel_unreadable", "wheel could not be read") from error
    return digest.hexdigest()


def _parse_entry_point(data: bytes) -> str:
    text = _read_utf8(data, "entry_points.txt")
    parser = configparser.ConfigParser(
        interpolation=None,
        strict=True,
        empty_lines_in_values=False,
    )
    parser.optionxform = str
    try:
        parser.read_string(text)
    except configparser.Error as error:
        raise PreflightError("malformed_metadata", "entry_points.txt is malformed") from error
    if parser.sections() != ["console_scripts"]:
        raise PreflightError("malformed_metadata", "console entry point section is missing")
    entries = dict(parser["console_scripts"])
    if set(entries) != {PROJECT_NAME}:
        raise PreflightError("malformed_metadata", "AHC console entry point is missing")
    value = entries[PROJECT_NAME].strip()
    if not _ENTRY_POINT.fullmatch(value):
        raise PreflightError("malformed_metadata", "AHC console entry point is invalid")
    return f"{PROJECT_NAME} = {value}"


def _inspect_archive(wheel: Path) -> dict:
    try:
        with zipfile.ZipFile(wheel) as archive:
            infos = archive.infolist()
            if len(infos) > MAX_ARCHIVE_MEMBERS:
                raise PreflightError("archive_too_large", "archive has too many members")
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise PreflightError(
                    "duplicate_archive_member", "archive contains a duplicate member"
                )
            for name in names:
                _archive_member_error(name)

            metadata_names = [
                name for name in names if name.endswith("/METADATA") and name.count("/") == 1
            ]
            if len(metadata_names) != 1:
                raise PreflightError(
                    "malformed_metadata", "distribution METADATA is missing or ambiguous"
                )
            metadata_name = metadata_names[0]
            dist_info = metadata_name.rsplit("/", 1)[0]
            if not dist_info.endswith(".dist-info"):
                raise PreflightError(
                    "malformed_metadata", "distribution metadata directory is invalid"
                )
            required = {
                "METADATA": metadata_name,
                "WHEEL": f"{dist_info}/WHEEL",
                "RECORD": f"{dist_info}/RECORD",
                "entry_points.txt": f"{dist_info}/entry_points.txt",
            }
            if not all(name in names for name in required.values()):
                raise PreflightError(
                    "malformed_metadata", "required distribution metadata is missing"
                )

            # Bound decompression before reading, not only after allocation.
            if any(archive.getinfo(name).file_size > MAX_METADATA_BYTES
                   for name in required.values()):
                raise PreflightError("malformed_metadata", "distribution metadata is too large")

            metadata, _ = _parse_headers(archive.read(metadata_name), "METADATA")
            metadata_version = _required_header(metadata, "Metadata-Version")
            if not _VERSION.fullmatch(metadata_version):
                raise PreflightError("malformed_metadata", "Metadata-Version is invalid")
            name = _required_header(metadata, "Name")
            if _normalize_name(name) != PROJECT_NAME:
                raise PreflightError("malformed_metadata", "package name is not ahc")
            version = _required_header(metadata, "Version")
            requires_python = _required_header(metadata, "Requires-Python")
            expected_dist_info = f"{_normalize_name(name)}-{version}.dist-info"
            if dist_info != expected_dist_info:
                raise PreflightError(
                    "malformed_metadata",
                    "distribution metadata directory does not match metadata",
                )

            requires_dist = [
                _clean_text(value)
                for value in metadata.get_all("Requires-Dist", [])
            ]
            if len(requires_dist) > MAX_REQUIREMENTS:
                raise PreflightError("malformed_metadata", "too many runtime dependencies")

            wheel_metadata, _ = _parse_headers(archive.read(required["WHEEL"]), "WHEEL")
            wheel_version = _required_header(wheel_metadata, "Wheel-Version")
            if not _VERSION.fullmatch(wheel_version):
                raise PreflightError("malformed_metadata", "Wheel-Version is invalid")
            _read_utf8(archive.read(required["RECORD"]), "RECORD")
            entry_point = _parse_entry_point(archive.read(required["entry_points.txt"]))

            source_member_count = sum(
                name.startswith(f"{PROJECT_NAME}/") and not info.is_dir()
                for name, info in zip(names, infos, strict=True)
            )
            if source_member_count == 0:
                raise PreflightError("malformed_archive", "wheel contains no ahc source members")
            return {
                "package": PROJECT_NAME,
                "version": version,
                "requires_python": requires_python,
                "requires_dist": requires_dist,
                "entry_point": entry_point,
                "source_members": source_member_count,
                "archive_members": len(names),
            }
    except PreflightError:
        raise
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile) as error:
        raise PreflightError("invalid_archive", "wheel is not a readable ZIP archive") from error


def preflight(wheel: Path, expected_sha256: str, target: Path) -> dict:
    """Return a bounded success receipt or raise :class:`PreflightError`."""

    if not _HEX_DIGEST.fullmatch(expected_sha256):
        raise PreflightError(
            "invalid_expected_hash", "expected SHA-256 must be 64 hexadecimal characters"
        )
    _validate_target(target)
    actual_sha256 = _sha256(wheel)
    if actual_sha256 != expected_sha256.lower():
        raise PreflightError(
            "wheel_hash_mismatch", "wheel SHA-256 does not match the expected digest"
        )
    details = _inspect_archive(wheel)
    return {
        "status": "PASS",
        "operation": "deployment_preflight",
        "wheel_name": _clean_text(wheel.name),
        "wheel_sha256": actual_sha256,
        "target_name": _clean_text(target.name),
        "target_created": False,
        "installation_performed": False,
        "package_code_executed": False,
        "dependency_resolution_performed": False,
        **details,
    }


def _failure_receipt(error: PreflightError) -> dict:
    return {
        "status": "FAIL",
        "operation": "deployment_preflight",
        "error_code": error.code,
        "error": error.message,
        "target_created": False,
        "installation_performed": False,
        "package_code_executed": False,
        "dependency_resolution_performed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        receipt = preflight(args.wheel, args.expected_sha256, args.target)
    except PreflightError as error:
        receipt = _failure_receipt(error)
    except Exception:
        receipt = _failure_receipt(
            PreflightError("preflight_failed", "preflight could not be completed")
        )
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
