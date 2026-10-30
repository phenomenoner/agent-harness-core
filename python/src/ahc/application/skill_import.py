"""Lossless, restart-safe operator import of inert skill bundles.

Source text and metadata never execute. Import does not select an owner, resolve
semantic duplicates, prove dependencies, or activate a revision.
"""

import json
import os
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import (
    MARKDOWN_SKILL_SCHEMA,
    MAX_ASSET_BYTES,
    MAX_BUNDLE_BYTES,
    SkillAsset,
    asset_path,
)
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import canonical_json, digest_bytes, digest_json


def read_bundle(source):
    source = Path(source)
    if source.is_symlink() or not source.is_dir():
        raise ContractError("skill import requires a real directory")
    source = source.resolve(strict=True)
    files = {}
    for directory, directories, names in os.walk(source, followlinks=False):
        for name in directories + names:
            path = Path(directory) / name
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise ContractError("skill import cannot follow links or junctions")
        if ".git" in directories:
            raise ContractError("import a skill directory, not a repository root")
        for name in sorted(names):
            path = Path(directory) / name
            relative = path.relative_to(source).as_posix()
            if path.stat().st_size > MAX_ASSET_BYTES:
                raise ContractError("skill source file exceeds import bound")
            with path.open("rb") as stream:
                value = stream.read(MAX_ASSET_BYTES + 1)
            if len(value) > MAX_ASSET_BYTES:
                raise ContractError("skill source file grew beyond import bound")
            files[relative] = value
            if len(files) > 257 or sum(len(value) for value in files.values()) > MAX_BUNDLE_BYTES:
                raise ContractError("skill source bundle exceeds import bound")
    if "SKILL.md" not in files:
        raise ContractError("skill source has no SKILL.md")
    original = tuple(
        SkillAsset("source/" + path, digest_bytes(value), len(value))
        for path, value in sorted(files.items())
    )
    manifest = {"files": [item.to_dict() for item in original]}
    return files, original, digest_json(manifest)


def import_bundle(
    repository,
    source,
    *,
    disposition,
    metadata=None,
    reason,
    canonical_digest=None,
    adapted_body=None,
    adapted_assets=None,
):
    """Stage one explicit disposition; raw archives are not a migration PASS.

    `ready` means the caller has checked dependencies and adapted instructions.
    `adapted_assets` explicitly replaces/adds resource bytes in the new revision;
    raw source custody is unchanged, and no resource is executed by the importer.
    All dispositions preserve every original byte. Only `ready` registers an
    inactive revision; existing evaluator/CAS activation remains a separate step.
    """
    if (
        disposition not in {"ready", "blocked", "retired", "duplicate"}
        or type(reason) is not str
        or not reason.strip()
        or len(reason) > 4096
    ):
        raise ContractError("skill import disposition requires a bounded rationale")
    if disposition == "duplicate":
        if type(canonical_digest) is not str:
            raise ContractError("duplicate requires an exact canonical revision")
        repository.get_revision(canonical_digest)
    elif canonical_digest is not None:
        raise ContractError("canonical digest is only for a duplicate disposition")
    if adapted_body is not None and (disposition != "ready" or type(adapted_body) is not str):
        raise ContractError("adapted instructions require a reviewed ready disposition")
    if adapted_assets is not None:
        if (
            disposition != "ready" or type(adapted_assets) is not dict
            or not 1 <= len(adapted_assets) <= 256
        ):
            raise ContractError("adapted resources require a bounded reviewed ready disposition")
        for path, value in adapted_assets.items():
            asset_path(path)
            if type(value) is not bytes or len(value) > MAX_ASSET_BYTES:
                raise ContractError("adapted resource bytes exceed bounds")
        # Own the input mapping before reading source or performing repository effects.
        adapted_assets = dict(adapted_assets)
    files, originals, source_digest = read_bundle(source)
    locator = str(Path(source).resolve(strict=True))
    revision = None
    resource_bytes = {path: value for path, value in files.items() if path != "SKILL.md"}
    resource_bytes.update(adapted_assets or {})
    assets = tuple(
        SkillAsset(path, digest_bytes(value), len(value))
        for path, value in sorted(resource_bytes.items())
    )
    if disposition == "ready":
        if type(metadata) is not dict:
            raise ContractError("skill import requires explicitly reviewed metadata")
        body = files["SKILL.md"].decode("utf-8") if adapted_body is None else adapted_body
        revision = SkillRevision(
            schema_version=MARKDOWN_SKILL_SCHEMA, body=body, assets=assets, **metadata
        )
    elif metadata is not None:
        raise ContractError("inactive archive does not accept revision metadata")
    payload = {
        "schema_version": "ahc.skill-import.v1",
        "source": locator,
        "source_digest": source_digest,
        "original_files": [v.to_dict() for v in originals],
        "disposition": disposition,
        "reason": reason,
        "revision_digest": None if revision is None else revision.revision_digest,
        "canonical_digest": canonical_digest,
        "effectiveness": "unobserved",
    }
    if adapted_body is not None:
        payload.update(
            schema_version="ahc.skill-import.v2",
            adaptation={
                "original_body_digest": digest_bytes(files["SKILL.md"]),
                "adapted_body_digest": digest_bytes(adapted_body.encode("utf-8")),
            },
        )
    if adapted_assets is not None:
        payload.update(
            schema_version="ahc.skill-import.v3",
            adaptation={
                "original_body_digest": digest_bytes(files["SKILL.md"]),
                "adapted_body_digest": digest_bytes(revision.body.encode("utf-8")),
                "assets": [
                    {"path": path, "original_sha256":
                     digest_bytes(files[path]) if path in files else None,
                     "adapted_sha256": digest_bytes(value), "size": len(value)}
                    for path, value in sorted(adapted_assets.items())
                ],
            },
        )
    receipt_digest = digest_json(payload)
    repository.stage_assets(originals, {"source/" + path: value for path, value in files.items()})
    if revision is not None:
        repository.stage_assets(assets, resource_bytes)
        repository.register_revision(revision)
    # Re-read the source before claiming a coherent import, preserving partial
    # staged bytes on drift for diagnosis/resume. Never overwrite the source.
    if read_bundle(source)[2] != source_digest:
        raise ContractError("skill source changed during import")
    with repository.writer_lock():
        receipts = repository.root / "imports"
        if receipts.is_symlink():
            raise ContractError("skill import receipt directory cannot be a link")
        receipts.mkdir(exist_ok=True)
        path = receipts / (receipt_digest + ".json")
        encoded = canonical_json(payload) + b"\n"
        if path.exists():
            if path.is_symlink() or path.read_bytes() != encoded:
                raise ContractError("skill import receipt has changed")
        else:
            with path.open("xb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
    return {**payload, "receipt_digest": receipt_digest}


def reconcile_imports(expected_sources, receipts):
    """Close a finite denominator, never treating blocked or absent rows as done."""
    expected = {str(Path(source).resolve(strict=True)) for source in expected_sources}
    if len(expected) != len(expected_sources):
        raise ContractError("skill denominator contains duplicate source locators")
    by_source = {}
    for value in receipts:
        item = json.loads(canonical_json(value))
        receipt = item.pop("receipt_digest")
        if digest_json(item) != receipt or item["source"] in by_source:
            raise ContractError("skill reconciliation has invalid or duplicate receipts")
        by_source[item["source"]] = item
    missing = sorted(expected - by_source.keys())
    extra = sorted(by_source.keys() - expected)
    blocked = sorted(
        source for source, item in by_source.items() if item["disposition"] == "blocked"
    )
    return {
        "technical_inventory_complete": not (missing or extra or blocked),
        "expected": len(expected),
        "accounted": len(by_source),
        "missing": missing,
        "extra": extra,
        "blocked": blocked,
        "activation_verified": False,
        "effectiveness": "unobserved",
    }
