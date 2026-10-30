"""Preserve an offline database bundle and replay only private disposable copies."""

import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

from .runtime_backup import LIMIT, _digest, _inspect_database
from .runtime_restore_activation import _write_json
from .runtime_restore_guard import offline_restore_ownership
from .working_db import WORKING_DB_FILENAME, require_real_directory, require_regular_file

SUFFIXES = ("", "-wal", "-shm", "-journal")


def capture_runtime_sidecars(store, supervisor_lock, destination):
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == store.root or store.root in destination.parents:
        raise ContractError("sidecar capture must be outside source runtime")
    with offline_restore_ownership(store, supervisor_lock):
        paths = [store.root / (WORKING_DB_FILENAME + suffix) for suffix in SUFFIXES]
        require_regular_file(paths[0], allow_absent=False)
        inventory = {}
        total = 0
        for path in paths:
            require_regular_file(path, allow_absent=True)
            if path.exists():
                size = path.stat().st_size
                total += size
                if total > LIMIT:
                    raise ContractError("sidecar bundle exceeds bound")
                inventory[path.name] = {"byte_count": size, "sha256": _digest(path)}
        destination.mkdir(exist_ok=False)
        raw = destination / "raw"
        raw.mkdir()
        deadline = time.monotonic() + 30
        for name, item in inventory.items():
            with (store.root / name).open("rb") as source, (raw / name).open("xb") as target:
                count = 0
                while block := source.read(1048576):
                    count += len(block)
                    if count > item["byte_count"] or time.monotonic() > deadline:
                        raise ContractError("sidecar capture changed or exceeded time bound")
                    target.write(block)
                target.flush()
                os.fsync(target.fileno())
            if _digest(raw / name) != item["sha256"]:
                raise ContractError("sidecar capture digest mismatch")
        after = {path.name: {"byte_count": path.stat().st_size, "sha256": _digest(path)}
                 for path in paths if path.exists()}
        if after != inventory:
            raise ContractError("sidecar source changed during capture")
        _write_json(destination / "capture.json", {"schema": "ahc.sidecar-capture.v1",
                                                   "files": inventory})
    # SQLite recovery may modify/remove replay sidecars. Raw capture stays immutable.
    replay = destination / "replay"
    replay.mkdir()
    for name in inventory:
        with (raw / name).open("rb") as source, (replay / name).open("xb") as target:
            while block := source.read(1048576):
                target.write(block)
    normalized = destination / "normalized.sqlite"
    with normalized.open("xb"):
        pass
    deadline = time.monotonic() + 30

    def progress(*_):
        if time.monotonic() > deadline:
            raise ContractError("sidecar normalization deadline exceeded")

    with closing(sqlite3.connect(replay / WORKING_DB_FILENAME, timeout=1)) as source:
        with closing(sqlite3.connect(normalized, timeout=1)) as target:
            source.backup(target, pages=256, progress=progress)
            target.execute("PRAGMA journal_mode=DELETE")
    _inspect_database(normalized)
    with normalized.open("r+b") as stream:
        os.fsync(stream.fileno())
    result = {"schema": "ahc.sidecar-normalization.v1", "files": inventory,
              "capture_digest": digest_json(inventory),
              "database": normalized.name, "sha256": _digest(normalized),
              "activation_allowed": False}
    _write_json(destination / "normalization.json", result)
    return result
