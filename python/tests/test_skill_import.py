"""Full bundle custody and finite import accounting, without activation."""

import json

import pytest

from ahc.application.skill_import import import_bundle, reconcile_imports
from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import SkillAsset
from ahc.infrastructure.skill_asset_store import read_asset


def source_at(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "SKILL.md").write_bytes(
        "---\r\nname: example\r\n---\r\n# 原文\r\n```sh\r\necho ok\r\n```\r\n".encode()
    )
    (source / "references").mkdir()
    (source / "references/ref.txt").write_bytes(b"original reference")
    return source


def metadata():
    return {
        "skill_name": "example",
        "version": "1.0.0",
        "description": "Example skill",
        "cognitive_layer": "how-procedure",
        "roles": ("procedure",),
        "agent_allowlist": ("agent-test",),
        "required_tools": (),
        "triggers": ("example",),
        "expected_effects": ("a useful answer",),
        "positive_examples": ("related task",),
        "negative_examples": ("unrelated task",),
        "relationships": (),
        "predecessor_digest": None,
    }


def test_import_preserves_originals_resumes_exactly_and_does_not_activate(repository, tmp_path):
    source = source_at(tmp_path)
    result = import_bundle(
        repository,
        source,
        disposition="ready",
        metadata=metadata(),
        reason="reference-only fixture verified",
    )
    again = import_bundle(
        repository,
        source,
        disposition="ready",
        metadata=metadata(),
        reason="reference-only fixture verified",
    )
    assert result == again
    assert repository.active_revisions() == ()
    assert len(repository.list_revisions()) == 1
    item = repository.get_revision(result["revision_digest"])
    assert item.body.encode() == (source / "SKILL.md").read_bytes()
    for asset in result["original_files"]:
        assert (
            read_asset(repository.root, SkillAsset.from_dict(asset))
            == (source / asset["path"].removeprefix("source/")).read_bytes()
        )
    check = reconcile_imports([source], [result])
    assert check["technical_inventory_complete"]
    assert not check["activation_verified"]
    assert check["effectiveness"] == "unobserved"


def test_retired_and_blocked_are_archived_without_revision(repository, tmp_path):
    source = source_at(tmp_path)
    receipt = import_bundle(
        repository,
        source,
        disposition="retired",
        reason="operator-retired workflow; retained as inert data",
    )
    assert repository.list_revisions() == ()
    assert reconcile_imports([source], [receipt])["technical_inventory_complete"]
    blocked = import_bundle(repository, source, disposition="blocked", reason="dependency missing")
    result = reconcile_imports([source], [blocked])
    assert result["blocked"] == [str(source.resolve())]
    assert not result["technical_inventory_complete"]
    assert not reconcile_imports([source], [])["technical_inventory_complete"]


def test_duplicate_requires_existing_canonical_revision(repository, tmp_path):
    source = source_at(tmp_path)
    with pytest.raises(ContractError):
        import_bundle(repository, source, disposition="duplicate", reason="same skill")
    ready = import_bundle(
        repository, source, disposition="ready", metadata=metadata(), reason="fixture"
    )
    duplicate = import_bundle(
        repository,
        source,
        disposition="duplicate",
        reason="same source",
        canonical_digest=ready["revision_digest"],
    )
    assert duplicate["canonical_digest"] == ready["revision_digest"]
    with pytest.raises(ContractError):
        reconcile_imports([source], [ready, duplicate])


def test_reconciliation_rejects_altered_receipt(repository, tmp_path):
    source = source_at(tmp_path)
    receipt = import_bundle(repository, source, disposition="blocked", reason="missing adapter")
    receipt["disposition"] = "ready"
    with pytest.raises(ContractError):
        reconcile_imports([source], [receipt])


def test_receipt_is_an_inert_private_artifact(repository, tmp_path):
    source = source_at(tmp_path)
    receipt = import_bundle(repository, source, disposition="blocked", reason="missing adapter")
    path = repository.root / "imports" / (receipt["receipt_digest"] + ".json")
    assert json.loads(path.read_bytes())["source_digest"] == receipt["source_digest"]
    assert not list(source.glob("*.json"))


def test_adaptation_serves_reviewed_text_and_keeps_original_bytes(repository, tmp_path):
    from ahc.domain.skill_values import digest_bytes

    source = source_at(tmp_path)
    original = (source / "SKILL.md").read_bytes()
    adapted = "# Adapted example\nChoose a useful approach within the user's authority.\n"
    kwargs = dict(
        disposition="ready",
        metadata=metadata(),
        reason="obsolete route adapted",
        adapted_body=adapted,
    )
    receipt = import_bundle(repository, source, **kwargs)
    assert import_bundle(repository, source, **kwargs) == receipt
    item = repository.get_revision(receipt["revision_digest"])
    assert item.body == adapted
    assert receipt["schema_version"] == "ahc.skill-import.v2"
    assert receipt["adaptation"] == {
        "original_body_digest": digest_bytes(original),
        "adapted_body_digest": digest_bytes(adapted.encode()),
    }
    raw_asset = next(
        value for value in receipt["original_files"] if value["path"] == "source/SKILL.md"
    )
    assert read_asset(repository.root, SkillAsset.from_dict(raw_asset)) == original
    assert (
        repository.read_asset(item.revision_digest, "references/ref.txt") == b"original reference"
    )
    assert reconcile_imports([source], [receipt])["technical_inventory_complete"]
    assert repository.active_revisions() == ()
    with pytest.raises(ContractError):
        import_bundle(
            repository, source, disposition="retired", reason="inactive", adapted_body=adapted
        )


def test_resource_adaptation_preserves_raw_custody_and_has_exact_retry(repository, tmp_path):
    from ahc.domain.skill_values import digest_bytes

    source = source_at(tmp_path)
    original = (source / "references/ref.txt").read_bytes()
    resources = {"references/ref.txt": b"Native Linux reference",
                 "scripts/example.py": b"raise RuntimeError('must never execute during import')\n"}
    kwargs = dict(disposition="ready", metadata=metadata(), reason="reviewed port fixture",
                  adapted_assets=resources, adapted_body="# Native procedure\nRead references.")
    receipt = import_bundle(repository, source, **kwargs)
    assert import_bundle(repository, source, **kwargs) == receipt
    assert receipt["schema_version"] == "ahc.skill-import.v3"
    assert receipt["adaptation"]["assets"][0] == {
        "path": "references/ref.txt", "original_sha256": digest_bytes(original),
        "adapted_sha256": digest_bytes(resources["references/ref.txt"]),
        "size": len(resources["references/ref.txt"])}
    assert receipt["adaptation"]["assets"][1]["original_sha256"] is None
    for path, raw in resources.items():
        assert repository.read_asset(receipt["revision_digest"], path) == raw
    raw = next(value for value in receipt["original_files"]
               if value["path"] == "source/references/ref.txt")
    assert read_asset(repository.root, SkillAsset.from_dict(raw)) == original
    assert (source / "references/ref.txt").read_bytes() == original
    assert not (source / "scripts").exists()
    assert repository.active_revisions() == ()
    with pytest.raises(ContractError):
        changed = json.loads(json.dumps(receipt))
        changed["adaptation"]["assets"][0]["adapted_sha256"] = "0" * 64
        reconcile_imports([source], [changed])


@pytest.mark.parametrize("resources", [
    {}, {"../escape": b"bad"}, {"SKILL.md": b"bypass body"},
    {"/absolute": b"bad"}, {"ref.txt": "not bytes"},
    {"ref.txt": b"one", "REF.txt": b"case alias"},
    {"scripts": b"file", "scripts/run.py": b"child"},
])
def test_resource_adaptation_rejects_invalid_shapes_before_staging(repository, tmp_path, resources):
    source = source_at(tmp_path)
    with pytest.raises(ContractError):
        import_bundle(repository, source, disposition="ready", metadata=metadata(),
                      reason="invalid fixture", adapted_assets=resources)
    assert repository.list_revisions() == ()
    assert not (repository.root / "imports").exists()


def test_resource_only_adaptation_does_not_change_body_or_legacy_receipt(repository, tmp_path):
    source = source_at(tmp_path)
    legacy = import_bundle(repository, source, disposition="ready", metadata=metadata(),
                           reason="old")
    adapted = import_bundle(repository, source, disposition="ready", metadata=metadata(),
                            reason="port", adapted_assets={"reference.txt": b"new bytes"})
    assert legacy["schema_version"] == "ahc.skill-import.v1"
    assert legacy["revision_digest"] != adapted["revision_digest"]
    assert repository.get_revision(adapted["revision_digest"]).body.encode() == (
        source / "SKILL.md").read_bytes()
    for disposition in ("blocked", "retired", "duplicate"):
        with pytest.raises(ContractError):
            import_bundle(repository, source, disposition=disposition, reason="inactive",
                          adapted_assets={"reference.txt": b"not admitted"})
