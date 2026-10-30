"""Explicit, restart-safe legacy originals and dispositions in the memory backup.

Only the operator/importer sees unqualified originals. A serving record requires
an explicit scope and normalized observation. Historical schedules are not
accepted sources. No source procedure or embedded instruction is executed.
"""

import base64
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .memory_journal import _scope

_TABLES = {
    "observations",
    "docs_chunks",
    "episodic_events",
    "graph_nodes",
    "graph_edges",
    "graph_facts",
    "meta",
    "files",
    "chunks",
    "native_identity",
    "native_operations",
    "ahc_legacy_main_import_v1",
}
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS migration_sources ("
    "source_id TEXT PRIMARY KEY, identity TEXT NOT NULL, descriptor TEXT NOT NULL, "
    "expected INTEGER NOT NULL, sealed INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE IF NOT EXISTS migration_entries ("
    "source_id TEXT NOT NULL REFERENCES migration_sources(source_id), "
    "source_key TEXT NOT NULL, digest TEXT NOT NULL, original TEXT NOT NULL, "
    "disposition TEXT NOT NULL DEFAULT 'unresolved', scope TEXT, reason TEXT, "
    "observation TEXT, record_ref TEXT, PRIMARY KEY(source_id,source_key))",
    "CREATE INDEX IF NOT EXISTS migration_imported ON migration_entries(source_id,source_key) "
    "WHERE record_ref IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS migration_pending ON migration_entries(source_id,source_key) "
    "WHERE disposition='serve' AND record_ref IS NULL",
)


def _label(value):
    if (
        type(value) is not str
        or not value.strip()
        or len(value) > 1024
        or any(ord(c) < 32 for c in value)
    ):
        raise ContractError("migration identity invalid")


def _serializable(value):
    if isinstance(value, bytes):
        return {"sqlite_blob_base64": base64.b64encode(value).decode("ascii")}
    return value


def snapshot_sqlite(source, destination):
    """SQLite's consistent read snapshot includes committed WAL; source is read-only.

    Destination is exclusively created. A failed/partial destination is retained
    and never mistaken for a completed snapshot or overwritten on retry.
    """
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or not source.is_file() or destination.is_symlink():
        raise ContractError("memory snapshot source or destination invalid")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb"):
        pass
    reader = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)
    target = sqlite3.connect(destination)
    try:
        reader.execute("PRAGMA query_only=ON")
        reader.backup(target, pages=1024)
        if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ContractError("memory source snapshot integrity failed")
    finally:
        target.close()
        reader.close()
    with destination.open("rb") as stream:
        checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"sha256": checksum, "bytes": destination.stat().st_size}


def sqlite_rows(snapshot, tables):
    """Yield stable primary-key identities and lossless selected logical rows.

    Full snapshot preserves derived indexes too; these are not counted again as
    independent memories. Explicit allowlist excludes any cron/schedule source.
    """
    if (
        type(tables) not in (list, tuple)
        or not tables
        or len(set(tables)) != len(tables)
        or not set(tables) <= _TABLES
    ):
        raise ContractError("unsupported legacy memory tables")
    db = sqlite3.connect(Path(snapshot).resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA query_only=ON")
        for table in tables:
            columns = db.execute(f'PRAGMA table_info("{table}")').fetchall()
            keys = [row["name"] for row in sorted(columns, key=lambda row: row["pk"]) if row["pk"]]
            if not keys:
                raise ContractError("memory source requires stable primary keys")
            for row in db.execute(f'SELECT * FROM "{table}"'):
                original = {key: _serializable(row[key]) for key in row.keys()}
                key = table + ":" + digest_json([original[k] for k in keys])
                yield key, {"table": table, "row": original}
    finally:
        db.close()


class MemoryMigration:
    def __init__(self, journal):
        self.journal = journal
        with journal._writer(), journal._connect() as db:
            for statement in _SCHEMA:
                db.execute(statement)

    def begin(self, source_id, descriptor, *, expected):
        _label(source_id)
        if (
            type(descriptor) is not dict
            or type(expected) is not int
            or expected < 0
            or len(canonical_json(descriptor)) > 65536
        ):
            raise ContractError("migration source descriptor invalid")
        encoded = canonical_json(descriptor).decode()
        identity = digest_json({"descriptor": descriptor, "expected": expected})
        with self.journal._writer(), self.journal._connect() as db:
            prior = db.execute(
                "SELECT identity FROM migration_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            if prior and prior[0] != identity:
                raise ContractError("migration source identity changed; admit a new delta source")
            db.execute(
                "INSERT OR IGNORE INTO migration_sources VALUES(?,?,?,?,0)",
                (source_id, identity, encoded, expected),
            )
        return identity

    def stage(self, source_id, entries):
        """At most 512 originals per transaction; exact retries consume no new IDs."""
        if type(entries) not in (tuple, list) or not 1 <= len(entries) <= 512:
            raise ContractError("migration batch must contain 1 to 512 originals")
        encoded = []
        for key, original in entries:
            _label(key)
            if type(original) is not dict:
                raise ContractError("migration original must be an object")
            value = canonical_json(original)
            if len(value) > 16 * 1024**2:
                raise ContractError(
                    "migration original exceeds 16 MiB; preserve and split explicitly"
                )
            encoded.append((key, hashlib.sha256(value).hexdigest(), value.decode()))
        with self.journal._writer(), self.journal._connect() as db:
            source = db.execute(
                "SELECT sealed,expected FROM migration_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            if source is None:
                raise ContractError("migration source is not admitted")
            for key, checksum, value in encoded:
                prior = db.execute(
                    "SELECT digest,original FROM migration_entries "
                    "WHERE source_id=? AND source_key=?",
                    (source_id, key),
                ).fetchone()
                if prior:
                    if tuple(prior) != (checksum, value):
                        raise ContractError("migration original changed")
                    continue
                if source[0]:
                    raise ContractError("migration source is sealed")
                db.execute(
                    "INSERT INTO migration_entries(source_id,source_key,digest,original) "
                    "VALUES(?,?,?,?)",
                    (source_id, key, checksum, value),
                )
            total = db.execute(
                "SELECT count(*) FROM migration_entries WHERE source_id=?", (source_id,)
            ).fetchone()[0]
            if total > source[1]:
                raise ContractError("migration exceeds admitted denominator")
        return total

    def seal(self, source_id):
        with self.journal._writer(), self.journal._connect() as db:
            source = db.execute(
                "SELECT expected FROM migration_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            total = db.execute(
                "SELECT count(*) FROM migration_entries WHERE source_id=?", (source_id,)
            ).fetchone()[0]
            if source is None or total != source[0]:
                raise ContractError("migration original census is incomplete")
            db.execute("UPDATE migration_sources SET sealed=1 WHERE source_id=?", (source_id,))

    def classify(self, source_id, source_key, *, disposition, reason, scope=None, observation=None):
        """Control-plane decision; not a model-facing permission or scope grant."""
        return self.classify_batch(
            source_id,
            [
                (
                    source_key,
                    {
                        "disposition": disposition,
                        "reason": reason,
                        "scope": scope,
                        "observation": observation,
                    },
                )
            ],
        )

    def classify_batch(self, source_id, decisions):
        if type(decisions) not in (list, tuple) or not 1 <= len(decisions) <= 512:
            raise ContractError("migration decision batch exceeds bounds")
        with self.journal._writer(), self.journal._connect() as db:
            for source_key, decision in decisions:
                if type(decision) is not dict or set(decision) != {
                    "disposition",
                    "reason",
                    "scope",
                    "observation",
                }:
                    raise ContractError("migration decision fields invalid")
                self._classify(db, source_id, source_key, **decision)

    def _classify(self, db, source_id, source_key, *, disposition, reason, scope, observation):
        if (
            disposition not in ("serve", "archive", "blocked")
            or type(reason) is not str
            or not reason.strip()
            or len(reason) > 4096
        ):
            raise ContractError("migration disposition requires an explicit reason")
        if disposition == "serve":
            _scope(scope)
            self.journal._observation(scope, observation)
        elif scope is not None or observation is not None:
            raise ContractError("non-serving originals have no serving authority")
        normalized = None if observation is None else canonical_json(observation).decode()
        row = db.execute(
            "SELECT * FROM migration_entries WHERE source_id=? AND source_key=?",
            (source_id, source_key),
        ).fetchone()
        if row is None:
            raise ContractError("migration original is missing")
        if row["disposition"] not in ("unresolved", "blocked"):
            if (row["disposition"], row["reason"], row["scope"], row["observation"]) != (
                disposition,
                reason,
                scope,
                normalized,
            ):
                raise ContractError("accepted migration disposition is immutable")
            return
        db.execute(
            "UPDATE migration_entries SET disposition=?,reason=?,scope=?,observation=? "
            "WHERE source_id=? AND source_key=?",
            (disposition, reason, scope, normalized, source_id, source_key),
        )

    def import_pending(self, source_id, *, limit=128):
        """Atomic initial import batch. Changed-source deltas use explicit import_one CAS."""
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ContractError("migration import batch exceeds bounds")
        with self.journal._writer(), self.journal._connect() as db:
            source = db.execute(
                "SELECT * FROM migration_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            if source is None or not source["sealed"]:
                raise ContractError("migration source is not sealed")
            family = json.loads(source["descriptor"]).get("logical_source", source_id)
            _label(family)
            other = db.execute(
                "SELECT 1 FROM migration_entries e JOIN migration_sources s USING(source_id) "
                "WHERE e.source_id<>? AND e.record_ref IS NOT NULL "
                "AND coalesce(json_extract(s.descriptor,'$.logical_source'),s.source_id)=? LIMIT 1",
                (source_id, family),
            ).fetchone()
            if other:
                raise ContractError("delta source requires individual predecessor reconciliation")
            rows = db.execute(
                "SELECT * FROM migration_entries WHERE source_id=? "
                "AND disposition='serve' AND record_ref IS NULL "
                "ORDER BY source_key LIMIT ?",
                (source_id, limit),
            ).fetchall()
            refs = []
            for row in rows:
                if hashlib.sha256(row["original"].encode()).hexdigest() != row["digest"]:
                    raise ContractError("migration original integrity changed")
                observation = json.loads(row["observation"])
                observation["detail"] = {
                    **observation["detail"],
                    "migration_source": family,
                    "migration_key": row["source_key"],
                    "original_digest": row["digest"],
                }
                request_digest = self.journal._observation(row["scope"], observation)
                operation = "import:" + digest_json([family, row["source_key"], row["digest"]])
                existing = db.execute(
                    "SELECT ref,request_digest,retired FROM records "
                    "WHERE scope=? AND operation_id=?",
                    (row["scope"], operation),
                ).fetchone()
                if existing:
                    if existing[1] != request_digest or existing[2]:
                        raise ContractError("migration operation differs or was retired")
                    ref = existing[0]
                else:
                    if db.execute(
                        "SELECT 1 FROM mutations WHERE scope=? AND operation_id=?",
                        (row["scope"], operation),
                    ).fetchone():
                        raise ContractError("migration operation already belongs to a mutation")
                    ref = "ahcm:" + digest_json([self.journal.store_id, row["scope"], operation])
                    db.execute(
                        "INSERT INTO records(ref,scope,operation_id,request_digest,"
                        "observation,created_at) VALUES(?,?,?,?,?,?)",
                        (
                            ref,
                            row["scope"],
                            operation,
                            request_digest,
                            canonical_json(observation).decode(),
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                db.execute(
                    "UPDATE migration_entries SET record_ref=? WHERE source_id=? AND source_key=?",
                    (ref, source_id, row["source_key"]),
                )
                refs.append(ref)
        return refs

    def import_one(self, source_id, source_key, *, expected_ref=None):
        with self.journal._connect() as db:
            row = db.execute(
                "SELECT e.*,s.sealed,s.descriptor FROM migration_entries e "
                "JOIN migration_sources s "
                "USING(source_id) WHERE source_id=? AND source_key=?",
                (source_id, source_key),
            ).fetchone()
        if row is None or not row["sealed"] or row["disposition"] != "serve":
            raise ContractError("memory original is not admitted for serving")
        if hashlib.sha256(row["original"].encode()).hexdigest() != row["digest"]:
            raise ContractError("migration original integrity changed")
        family = json.loads(row["descriptor"]).get("logical_source", source_id)
        _label(family)
        with self.journal._connect() as db:
            previous = db.execute(
                "SELECT e.digest,e.record_ref,e.scope FROM migration_entries e "
                "JOIN migration_sources s USING(source_id) WHERE e.source_key=? "
                "AND e.source_id<>? AND e.record_ref IS NOT NULL "
                "AND coalesce(json_extract(s.descriptor,'$.logical_source'),s.source_id)=?",
                (source_key, source_id, family),
            ).fetchall()
        if any(item["scope"] != row["scope"] for item in previous):
            raise ContractError("migration delta cannot silently transfer record scope")
        same = [item for item in previous if item["digest"] == row["digest"]]
        changed = [item for item in previous if item["digest"] != row["digest"]]
        if changed and not same and not any(item["record_ref"] == expected_ref for item in changed):
            raise ContractError("changed source row requires an explicit current predecessor")
        if expected_ref is not None and not changed:
            raise ContractError("migration predecessor is not a changed source record")
        observation = json.loads(row["observation"])
        observation["detail"] = {
            **observation["detail"],
            "migration_source": family,
            "migration_key": source_key,
            "original_digest": row["digest"],
        }
        operation = "import:" + digest_json([family, source_key, row["digest"]])
        # A crash after store but before ledger acknowledgment is an exact retry.
        # Derived embeddings run in bounded sync batches after originals commit.
        if same:
            with self.journal._connect() as db:
                retained = db.execute(
                    "SELECT * FROM records WHERE ref=? AND scope=?",
                    (same[0]["record_ref"], row["scope"]),
                ).fetchone()
            if (
                retained is None
                or retained["retired"]
                or retained["request_digest"]
                != self.journal._observation(row["scope"], observation)
            ):
                raise ContractError("migration delta cannot replace a later correction or deletion")
            result = self.journal._record(retained)
        elif changed:
            result = self.journal.revise(
                scope=row["scope"],
                operation_id=operation,
                expected_ref=expected_ref,
                observation=observation,
            )
            result = self.journal.get(scope=row["scope"], record_ref=result["record_ref"])
        else:
            result = self.journal.store(
                scope=row["scope"], operation_id=operation, observation=observation, index=False
            )
        with self.journal._writer(), self.journal._connect() as db:
            db.execute(
                "UPDATE migration_entries SET record_ref=? WHERE source_id=? AND source_key=?",
                (result["record_ref"], source_id, source_key),
            )
        return result

    def reconcile(self, source_id):
        with self.journal._connect() as db:
            source = db.execute(
                "SELECT * FROM migration_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            if source is None:
                raise ContractError("migration source is missing")
            counts = dict(
                db.execute(
                    "SELECT disposition,count(*) FROM migration_entries "
                    "WHERE source_id=? GROUP BY disposition",
                    (source_id,),
                )
            )
            pending = db.execute(
                "SELECT count(*) FROM migration_entries WHERE source_id=? "
                "AND disposition='serve' AND record_ref IS NULL",
                (source_id,),
            ).fetchone()[0]
            indexed = db.execute(
                "SELECT count(*) FROM migration_entries e JOIN records r "
                "ON e.record_ref=r.ref WHERE source_id=? AND r.indexed=1 "
                "AND r.retired=0",
                (source_id,),
            ).fetchone()[0]
            total = sum(counts.values())
            complete = (
                bool(source["sealed"])
                and total == source["expected"]
                and not counts.get("unresolved", 0)
                and not counts.get("blocked", 0)
                and not pending
                and indexed == counts.get("serve", 0)
            )
            return {
                "source_id": source_id,
                "expected": source["expected"],
                "preserved": total,
                "dispositions": counts,
                "pending_import": pending,
                "indexed": indexed,
                "technical_complete": complete,
                "activation_allowed": False,
            }


__all__ = ["MemoryMigration", "snapshot_sqlite", "sqlite_rows"]
