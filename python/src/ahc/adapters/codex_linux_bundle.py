"""Offline Linux archive qualification; never extraction or launch authorization."""

import hashlib
import json
import tarfile
from importlib.resources import files
from pathlib import Path

from ahc.domain.sessions import ContractError

from .linux_codex_tree import (
    LinuxTreeVerification,
    TreeRejected,
)
from .linux_codex_tree import (
    _protected_root_metadata as _protected_root_metadata,
)
from .linux_codex_tree import (
    sys as sys,
)
from .linux_codex_tree import (
    verify_linux_tree as _verify_linux_tree,
)

LINUX_CODEX_PIN = json.loads(
    files(__package__).joinpath("codex_linux_backend.json").read_text(encoding="utf-8")
)


def verify_linux_tree(
    root: Path, *, target: str, require_root_owned: bool = False
) -> LinuxTreeVerification:
    try:
        return _verify_linux_tree(
            root, pin=LINUX_CODEX_PIN, target=target, require_root_owned=require_root_owned
        )
    except TreeRejected as exc:
        raise ContractError(str(exc)) from None


def verify_linux_archive(archive: Path, *, target: str) -> None:
    """Check pinned bytes and exact regular members on one open file handle.

    Callers must separately verify extracted Linux files and execution permissions
    at admission. Success here says nothing about installed bytes, process custody,
    vendor signatures or sandbox enforcement.
    """
    if target != LINUX_CODEX_PIN["target"]:
        raise ContractError("Linux Codex archive target mismatch")
    path = Path(archive)
    if not path.is_absolute() or not path.is_file():
        raise ContractError("Linux Codex requires an explicit archive file")
    if any(p.is_symlink() or p.is_junction() for p in (path, *path.parents)):
        raise ContractError("Linux Codex archive cannot traverse redirects")
    expected = {member["path"]: member for member in LINUX_CODEX_PIN["members"]}
    directories = {item["path"]: item["mode"] for item in LINUX_CODEX_PIN["directories"]}
    try:
        with path.open("rb") as stream:
            stream.seek(0, 2)
            if stream.tell() != LINUX_CODEX_PIN["package_size"]:
                raise ContractError("Linux Codex archive size mismatch")
            stream.seek(0)
            if (
                hashlib.file_digest(stream, "sha256").hexdigest()
                != (LINUX_CODEX_PIN["package_sha256"])
            ):
                raise ContractError("Linux Codex archive digest mismatch")
            stream.seek(0)
            seen = set()
            # GzipFile bounds decompression to the requested read size. The tar
            # streaming gzip reader can allocate an unbounded decompressed buffer.
            with tarfile.open(fileobj=stream, mode="r:gz") as package:
                for member in package:
                    if member.name in seen:
                        raise ContractError("Linux Codex archive duplicate member")
                    if member.isdir():
                        if directories.get(member.name) != member.mode:
                            raise ContractError("Linux Codex archive directory mismatch")
                        seen.add(member.name)
                        continue
                    pin = expected.get(member.name)
                    if pin is None or not member.isfile():
                        raise ContractError("Linux Codex archive member mismatch")
                    if member.size != pin["size"] or member.mode != pin["mode"]:
                        raise ContractError("Linux Codex archive member metadata mismatch")
                    content = package.extractfile(member)
                    with content:
                        if hashlib.file_digest(content, "sha256").hexdigest() != pin["sha256"]:
                            raise ContractError("Linux Codex archive member digest mismatch")
                    seen.add(member.name)
            if seen != set(expected) | set(directories):
                raise ContractError("Linux Codex archive members missing")
    except (OSError, EOFError, tarfile.TarError):
        raise ContractError("Linux Codex archive unavailable or malformed") from None
