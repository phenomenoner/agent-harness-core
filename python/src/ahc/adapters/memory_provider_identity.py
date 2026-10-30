"""Content identity for a frozen, source-only local provider staging directory."""

import hashlib
from pathlib import Path

from ahc.domain.skill_values import digest_json


def provider_digest(provider_root):
    root = Path(provider_root)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("provider directory invalid")
    manifest, total = {}, 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or path.name == "__pycache__" or path.suffix == ".pyc":
            raise ValueError("provider must be frozen source without links or bytecode")
        if path.is_file():
            total += path.stat().st_size
            if total > 33554432 or len(manifest) >= 1024:
                raise ValueError("provider size invalid")
            manifest[path.relative_to(root).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    if "openclaw_mem/core/api.py" not in manifest:
        raise ValueError("public provider API missing")
    return digest_json({"format": "ahc.provider-source.v1", "files": manifest})
