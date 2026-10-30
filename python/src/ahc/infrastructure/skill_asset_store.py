"""Inert content-addressed resources, kept outside model-writable workspaces."""

import os
import tempfile
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import MAX_ASSET_BYTES, SkillAsset
from ahc.domain.skill_values import digest_bytes


def _path(root: Path, asset: SkillAsset) -> Path:
    directory = root / "blobs"
    if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
        raise ContractError("skill blob directory is invalid")
    return directory / asset.sha256


def read_asset(root: Path, asset: SkillAsset) -> bytes:
    path = _path(root, asset)
    if path.is_symlink() or not path.is_file() or path.stat().st_size != asset.size:
        raise ContractError("skill resource is missing or has changed")
    with path.open("rb") as stream:
        value = stream.read(MAX_ASSET_BYTES + 1)
    if len(value) != asset.size or digest_bytes(value) != asset.sha256:
        raise ContractError("skill resource differs from immutable descriptor")
    return value


def store_asset(root: Path, asset: SkillAsset, value: bytes) -> None:
    # The repository owns the writer lock. Publishing never executes the bytes.
    if type(value) is not bytes or len(value) != asset.size or digest_bytes(value) != asset.sha256:
        raise ContractError("skill resource input does not match descriptor")
    path = _path(root, asset)
    path.parent.mkdir(exist_ok=True)
    if path.exists() or path.is_symlink():
        read_asset(root, asset)
        return
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    read_asset(root, asset)
