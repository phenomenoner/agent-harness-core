"""One explicit AHC-owned Codex bundle pin; never search PATH or other installations."""

import hashlib
import json
from importlib.resources import files
from pathlib import Path, PurePosixPath

from ahc.domain.sessions import ContractError

CODEX_PIN = json.loads(
    files(__package__).joinpath("codex_backend.json").read_text(encoding="utf-8")
)


def verify_bundle(bundle_root: Path) -> Path:
    root = Path(bundle_root)
    if not root.is_absolute() or not root.is_dir():
        raise ContractError("Codex requires an explicit existing bundle root")
    for part in (root, *root.parents):
        if part.is_symlink() or part.is_junction():
            raise ContractError("Codex bundle root must contain no redirect")
    expected = set()
    for member in CODEX_PIN["members"]:
        relative = PurePosixPath(member["path"])
        if relative.is_absolute() or ".." in relative.parts or ":" in member["path"]:
            raise ContractError("Codex bundle pin member path invalid")
        path = root.joinpath(*relative.parts)
        for part in (path, *path.parents):
            if part == root:
                break
            if part.is_symlink() or part.is_junction():
                raise ContractError("Codex bundle member must contain no redirect")
        if not path.is_file() or path.stat().st_size != member["size"]:
            raise ContractError("Codex bundle member size mismatch")
        if hashlib.sha256(path.read_bytes()).hexdigest() != member["sha256"]:
            raise ContractError("Codex bundle member digest mismatch")
        expected.add(member["path"])
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise ContractError("Codex bundle contains unpinned members")
    return root / CODEX_PIN["entrypoint"]
