"""Read-only restore planning evidence, never activation authority."""

import sqlite3
from contextlib import closing
from pathlib import Path

from .runtime_backup import inspect_runtime_backup
from .working_db import decode_snapshot

_COUNTS = {
    "pending_turns": ("runtime_turns", "status='submitted'"),
    "undelivered_outbox": ("runtime_channel_outbox", "state!='sent'"),
    "unfinished_children": ("runtime_children", "state IN ('queued','claimed','unknown')"),
    "backend_held": ("backend_custody", "state='held'"),
    "backend_creation_pending": ("backend_creation_intents", "state='pending'"),
    "workspace_heads": ("workspace_heads", "1=1"),
    "schedules": ("runtime_schedules", "1=1"),
    "submissions": ("runtime_submissions", "1=1"),
    "undelivered_attachments": ("runtime_channel_attachments", "state!='sent'"),
    "unconfirmed_progress": ("runtime_channel_progress", "state IN ('claimed','unknown')"),
    "staged_file_events": ("runtime_channel_file_inbox", "1=1"),
    "completed_file_imports": ("runtime_channel_file_imports", "1=1"),
    "telegram_cursors": ("runtime_telegram_cursors", "1=1"),
    "discord_resume_checkpoints": ("runtime_discord_resume", "1=1"),
}


def assess_runtime_restore(root):
    manifest = inspect_runtime_backup(root)
    path = Path(root).absolute() / "runtime.sqlite"
    return assess_database(path, manifest["sha256"], manifest["tables"])


def assess_database(path, backup_digest, tables):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        rows = connection.execute(
            "SELECT s.snapshot_json FROM current_working_sets c JOIN snapshots s "
            "ON s.snapshot_digest=c.snapshot_digest ORDER BY c.virtual_session_id"
        ).fetchall()
        sessions = []
        for row in rows:
            snapshot = decode_snapshot(row[0])
            sessions.append({"session": snapshot.session.to_dict(), "status": snapshot.status,
                             "snapshot_digest": snapshot.snapshot_digest,
                             "active_turn_present": snapshot.active_turn_id is not None})
        counts = {}
        for name, (table, predicate) in _COUNTS.items():
            counts[name] = (connection.execute(
                f"SELECT count(*) FROM {table} WHERE {predicate}"
            ).fetchone()[0] if table in tables else None)
    # Missing tables mean unobserved domain, not proof of zero outstanding work.
    return {"schema": "ahc.runtime-restore-assessment.v1",
            "backup_digest": backup_digest, "sessions": sessions, "counts": counts,
            "activation_allowed": False, "disposition": "restore-not-implemented",
            "required_boundaries": ["source-runtime-fencing", "successor-authority",
                                    "external-effects-no-replay", "cross-domain-restore"]}
