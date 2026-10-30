"""Lossless note/JSONL inputs for operator-controlled memory migration.

File containers preserve formatting, blank lines and invalid bytes. JSONL records
are separate logical entries with byte offsets into those containers. Neither a
path, an embedded owner field nor historical instructions confer serving authority.
"""

import base64
import hashlib
import json
import re
import zipfile
from pathlib import Path, PurePosixPath

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

MAX_FILE = 8 * 1024**2
MAX_ARCHIVE = 64 * 1024**2
MAX_FILES = 1024
MAX_RECORDS = 100000


def _checksum(value):
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ContractError("note archive checksum invalid")


def _source_path(value):
    if (
        type(value) is not str or not 1 <= len(value) <= 768
        or "\\" in value or any(ord(char) < 32 for char in value)
    ):
        raise ContractError("note archive source path invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute() or str(path) != value or ".." in path.parts
        or path.suffix.lower() not in (".md", ".jsonl")
    ):
        raise ContractError("note archive source path invalid")
    return path


def _nonfinite(value):
    raise ValueError("non-finite JSON number")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def note_archive_rows(archive_path, manifest):
    """Yield (stable key, inert original) pairs, never scope or observations.

    The caller must exhaust the iterator and seal its exact migration denominator.
    A late validation error leaves any staged originals incomplete, not accepted.
    Containers and logical lines are distinct kinds, not duplicate memories.
    """
    if type(manifest) is not dict or manifest.get("schema_version") != (
        "ahc.legacy-note-snapshot.v1"
    ):
        raise ContractError("note archive manifest invalid")
    checksum = manifest.get("archive_sha256")
    _checksum(checksum)
    entries = manifest.get("files")
    if type(entries) is not list or not 0 <= len(entries) <= MAX_FILES:
        raise ContractError("note archive file denominator invalid")
    expected = {}
    for row in entries:
        if type(row) is not dict:
            raise ContractError("note archive file entry invalid")
        name, size, sha256 = row.get("path"), row.get("bytes"), row.get("sha256")
        _source_path(name)
        _checksum(sha256)
        if type(size) is not int or not 0 <= size <= MAX_FILE or name in expected:
            raise ContractError("note archive file size or identity invalid")
        expected[name] = row
    if (
        len({name.casefold() for name in expected}) != len(expected)
        or sum(row["bytes"] for row in entries) > MAX_ARCHIVE
    ):
        raise ContractError("note archive aliases or total size invalid")
    path = Path(archive_path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_ARCHIVE:
        raise ContractError("note archive file invalid")
    with path.open("rb") as source:
        if hashlib.file_digest(source, "sha256").hexdigest() != checksum:
            raise ContractError("note archive bytes changed")
        source.seek(0)
        with zipfile.ZipFile(source) as archive:
            members = archive.infolist()
            if len(members) != len(expected) or {m.filename for m in members} != set(expected):
                raise ContractError("note archive denominator mismatch")
            count = 0
            for member in sorted(members, key=lambda value: value.filename):
                row = expected[member.filename]
                if member.is_dir() or member.file_size != row["bytes"]:
                    raise ContractError("note archive file size mismatch")
                with archive.open(member) as reader:
                    raw = reader.read(MAX_FILE + 1)
                if len(raw) != row["bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                    raise ContractError("note archive original hash mismatch")
                is_jsonl = member.filename.lower().endswith(".jsonl")
                file_key = "file:" + digest_json(member.filename)
                yield file_key, {
                    "kind": "jsonl-container" if is_jsonl else "note-file",
                    "path": member.filename, "sha256": row["sha256"],
                    "archive_sha256": checksum,
                    "bytes_base64": base64.b64encode(raw).decode("ascii"),
                }
                if not is_jsonl:
                    continue
                offset = 0
                logical = 0
                for number, line in enumerate(raw.splitlines(keepends=True), 1):
                    start, offset = offset, offset + len(line)
                    if not line.strip():
                        continue
                    count += 1
                    logical += 1
                    if count > MAX_RECORDS or len(line) > 4 * 1024**2:
                        raise ContractError("note archive record bound exceeded")
                    original = {
                        "kind": "jsonl-record", "path": member.filename,
                        "container_key": file_key, "file_sha256": row["sha256"],
                        "line": number, "byte_offset": start,
                        "bytes_base64": base64.b64encode(line).decode("ascii"),
                    }
                    try:
                        original["parsed"] = json.loads(
                            line.decode("utf-8-sig"), parse_constant=_nonfinite,
                            object_pairs_hook=_unique_object,
                        )
                        original["parse_status"] = "parsed"
                    except (ValueError, UnicodeError):
                        original["parse_status"] = "invalid-json"
                    yield "line:" + digest_json([member.filename, number]), original
                if "logical_lines" in row and row["logical_lines"] != logical:
                    raise ContractError("note archive logical denominator mismatch")
        source.seek(0)
        if hashlib.file_digest(source, "sha256").hexdigest() != checksum:
            raise ContractError("note archive changed during iteration")


__all__ = ["note_archive_rows"]
