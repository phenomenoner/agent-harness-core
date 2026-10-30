"""Offline, byte-preserving runtime relocation; never starts a receiver."""

import json
import os
import shutil
from pathlib import Path

from ahc.domain.sessions import ContractError
from .runtime_backup import _digest, _inspect_database
from .runtime_restore_guard import offline_restore_source
from .working_db import WORKING_DB_FILENAME, require_real_directory


def relocate_offline_runtime(store, destination, *, supervisor_lock, source_digest):
    """Hold all cooperative source owners through capture and verification.

    The operator must also retire external receiver launchers. This artifact is
    data-only: activation requires separate configuration and receiver custody.
    Failed or interrupted destinations are retained and never overwritten.
    """
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == store.root or store.root in destination.parents:
        raise ContractError("relocation destination must be outside source runtime")
    with offline_restore_source(store, supervisor_lock, source_digest) as snapshots:
        from .runtime_inbox import RuntimeInbox
        inbox = RuntimeInbox(store)
        for snapshot in snapshots:
            if snapshot.active_turn_id is not None or (snapshot.status == "active"
                    and inbox.next_pending(snapshot.session) is not None):
                raise ContractError("relocation requires quiescent runtime input")
        with store._read() as connection:
            for table, predicate in (
                ("runtime_turns", "status!='terminal'"),
                ("workspace_heads", "state!='ready'"),
                ("runtime_children", "state IN ('queued','claimed')"),
                ("runtime_channel_outbox", "state IN ('pending','claimed')"),
                ("runtime_channel_attachments", "state IN ('pending','claimed')"),
                ("runtime_channel_progress", "state='claimed'"),
            ):
                present = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if present and connection.execute(
                    f"SELECT 1 FROM {table} WHERE {predicate} LIMIT 1"
                ).fetchone():
                    raise ContractError("relocation requires quiescent runtime effects")
        source = store.root / WORKING_DB_FILENAME
        tables = _inspect_database(source)
        destination.mkdir(mode=0o700, exist_ok=False)
        target = destination / WORKING_DB_FILENAME
        with source.open("rb") as reader, target.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        if _digest(source) != source_digest or _digest(target) != source_digest:
            raise ContractError("relocation source or copy changed")
        if _inspect_database(target) != tables:
            raise ContractError("relocation table inventory changed")
        receipt = {"schema": "ahc.runtime-relocation.v1", "sha256": source_digest,
                   "byte_count": target.stat().st_size, "tables": tables,
                   "identity_preserved": True, "activation_authorized": False}
        with (destination / "relocation.json").open("x", encoding="utf-8") as output:
            json.dump(receipt, output, sort_keys=True, indent=2)
            output.flush()
            os.fsync(output.fileno())
        return receipt
