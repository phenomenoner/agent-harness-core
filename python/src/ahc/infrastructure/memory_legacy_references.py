"""Exact predecessor citations and acknowledgments; never a replay mechanism."""

import json
import re

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json


def initialize(db):
    db.execute(
        "CREATE TABLE IF NOT EXISTS legacy_memory_refs ("
        "scope TEXT NOT NULL, ref TEXT NOT NULL, canonical_ref TEXT NOT NULL "
        "REFERENCES records(ref), payload TEXT NOT NULL, digest TEXT NOT NULL, "
        "PRIMARY KEY(scope,ref))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS legacy_memory_operations ("
        "scope TEXT NOT NULL, operation_id TEXT NOT NULL, request_digest TEXT NOT NULL, "
        "ref TEXT NOT NULL, PRIMARY KEY(scope,operation_id), "
        "FOREIGN KEY(scope,ref) REFERENCES legacy_memory_refs(scope,ref))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS legacy_memory_namespace ("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1), source_id TEXT NOT NULL, "
        "manifest_digest TEXT NOT NULL)"
    )


def available(db):
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_memory_namespace'"
        ).fetchone()
        is not None
    )


def get(db, scope, ref, *, include_retired=False):
    if not available(db):
        return None
    row = db.execute(
        "SELECT a.payload,a.digest,r.retired,a.canonical_ref FROM legacy_memory_refs a "
        "JOIN records r ON r.ref=a.canonical_ref AND r.scope=a.scope "
        "WHERE a.scope=? AND a.ref=?",
        (scope, ref),
    ).fetchone()
    if row is None or (row[2] and not include_retired):
        return None
    value = json.loads(row[0])
    if digest_json([scope, ref, row[3], value]) != row[1] or value["record_ref"] != ref:
        raise ContractError("legacy memory reference integrity changed")
    return value


def operation(db, scope, operation_id, request_digest):
    if not available(db):
        return None
    row = db.execute(
        "SELECT request_digest,ref FROM legacy_memory_operations WHERE scope=? AND operation_id=?",
        (scope, operation_id),
    ).fetchone()
    if row is None:
        return None
    if row[0] != request_digest:
        raise ContractError("legacy memory operation payload changed")
    value = get(db, scope, row[1], include_retired=True)
    if (
        value is None
        or value["request_digest"] != request_digest
        or value["trace"]["operation_id"] != operation_id
    ):
        raise ContractError("legacy memory operation reference unavailable")
    return value


def canonical(db, scope, ref):
    if not available(db):
        return ref
    row = db.execute(
        "SELECT canonical_ref FROM legacy_memory_refs WHERE scope=? AND ref=?", (scope, ref)
    ).fetchone()
    if row is not None:
        get(db, scope, ref, include_retired=True)
    return ref if row is None else row[0]


def adopt_namespace(journal, *, source_id, entries):
    """Operator-only preparation of an unpublished import, before any indexing.

    Entries contain exact old results verified against a source snapshot by the
    importer. The complete one-to-one row map preserves old numeric citations.
    No runtime/model tool exposes this operation. Later writes, mutations,
    partial maps or a second source namespace make adoption unavailable.
    """
    if (
        type(source_id) is not str
        or not 1 <= len(source_id) <= 256
        or type(entries) is not list
        or not 1 <= len(entries) <= 100000
    ):
        raise ContractError("legacy memory namespace manifest invalid")
    manifest_digest = digest_json(entries)
    refs, ids = set(), set()
    aliases, operations = [], []
    alias_keys, operation_keys = set(), set()
    for entry in entries:
        if type(entry) is not dict or set(entry) != {
            "ref",
            "row_id",
            "scope",
            "results",
            "operation_ref",
        }:
            raise ContractError("legacy memory namespace entry invalid")
        ref, row_id, scope = entry["ref"], entry["row_id"], entry["scope"]
        if (
            type(ref) is not str
            or not re.fullmatch(r"ahcm:[0-9a-f]{64}", ref)
            or type(row_id) is not int
            or not 0 < row_id < 2**63 - 1
            or ref in refs
            or row_id in ids
            or type(scope) is not str
            or type(entry["results"]) is not list
            or not 1 <= len(entry["results"]) <= 2
        ):
            raise ContractError("legacy memory namespace requires a complete unique row map")
        refs.add(ref)
        ids.add(row_id)
        found_operation = entry["operation_ref"] is None
        for result in entry["results"]:
            expected_fields = {
                "status",
                "record_ref",
                "request_digest",
                "record",
                "provenance",
                "trust",
                "trace",
            }
            if (
                type(result) is not dict
                or set(result) != expected_fields
                or result["status"] != "succeeded"
                or result["trust"] != "untrusted"
                or result["provenance"] != {"origin": "unknown"}
                or type(result["record_ref"]) is not str
                or not re.fullmatch(
                    r"(?:citation:[0-9a-f]{32}:[1-9][0-9]{0,18}:[0-9a-f]{64}"
                    r"|memory:[0-9a-f]{32}:[0-9a-f]{32})",
                    result["record_ref"],
                )
                or type(result["request_digest"]) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", result["request_digest"])
                or type(result["trace"]) is not dict
                or set(result["trace"]) != {"operation_id", "request_digest"}
                or result["trace"].get("request_digest") != result["request_digest"]
                or type(result["record"]) is not dict
                or set(result["record"]) != {"kind", "summary", "detail", "created_at"}
                or type(result["record"]["detail"]) is not dict
                or result["record"].get("detail", {}).get("scope") != scope
                or any(
                    type(result["record"][key]) is not str or not result["record"][key]
                    for key in ("kind", "summary", "created_at")
                )
                or len(canonical_json(result)) > 131072
            ):
                raise ContractError("legacy memory result identity invalid")
            if result["record_ref"].startswith("citation:"):
                if int(result["record_ref"].split(":")[2]) != row_id:
                    raise ContractError("legacy citation row identity differs")
            alias_key = (scope, result["record_ref"])
            if alias_key in alias_keys:
                raise ContractError("legacy reference must have one canonical target")
            alias_keys.add(alias_key)
            aliases.append(
                (
                    scope,
                    result["record_ref"],
                    ref,
                    canonical_json(result).decode(),
                    digest_json([scope, result["record_ref"], ref, result]),
                )
            )
            if result["record_ref"] == entry["operation_ref"]:
                operation_id = result["trace"].get("operation_id")
                if type(operation_id) is not str or not 1 <= len(operation_id) <= 256:
                    raise ContractError("legacy memory operation identity invalid")
                if (scope, operation_id) in operation_keys:
                    raise ContractError("legacy operation must have one acknowledgment")
                operation_keys.add((scope, operation_id))
                operations.append(
                    (scope, operation_id, result["request_digest"], result["record_ref"])
                )
                found_operation = True
        if not found_operation:
            raise ContractError("legacy memory operation must reference a supplied result")
    with journal._writer(), journal._connect() as db:
        initialize(db)
        prior = db.execute(
            "SELECT source_id,manifest_digest FROM legacy_memory_namespace"
        ).fetchone()
        if prior is not None:
            if tuple(prior) != (source_id, manifest_digest):
                raise ContractError("legacy memory namespace already adopted")
            return {"manifest_digest": manifest_digest, "records": len(entries), "adopted": True}
        rows = db.execute(
            "SELECT ref,scope,operation_id,observation,retired,indexed FROM records"
        ).fetchall()
        if (
            refs != {row[0] for row in rows}
            or db.execute("SELECT 1 FROM chunks LIMIT 1").fetchone()
            or db.execute("SELECT 1 FROM mutations LIMIT 1").fetchone()
            or any(not row[2].startswith("import:") or row[4] or row[5] for row in rows)
        ):
            raise ContractError(
                "legacy namespace requires an untouched complete unpublished import"
            )
        by_ref = {row[0]: row for row in rows}
        for entry in entries:
            row = by_ref[entry["ref"]]
            record = json.loads(row[3])
            if row[1] != entry["scope"] or any(
                value["record"][key] != record[key]
                for value in entry["results"]
                for key in ("kind", "summary")
            ):
                raise ContractError("legacy reference does not describe its canonical record")
        existing_operations = {(row[1], row[2]) for row in rows}
        if any((value[0], value[1]) in existing_operations for value in operations):
            raise ContractError("legacy operation conflicts with a current operation")
        # No public IDs have been served from this isolated generation. Negative
        # temporary IDs avoid primary-key collisions inside the same transaction.
        db.execute("UPDATE records SET id=-id")
        db.executemany(
            "UPDATE records SET id=? WHERE ref=?", [(e["row_id"], e["ref"]) for e in entries]
        )
        db.executemany("INSERT INTO legacy_memory_refs VALUES(?,?,?,?,?)", aliases)
        db.executemany("INSERT INTO legacy_memory_operations VALUES(?,?,?,?)", operations)
        db.execute(
            "INSERT INTO legacy_memory_namespace VALUES(1,?,?)", (source_id, manifest_digest)
        )
    return {"manifest_digest": manifest_digest, "records": len(entries), "adopted": True}
