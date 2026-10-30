"""Lossless legacy Markdown and immutable, scoped resource disclosure."""

import base64
from dataclasses import replace

import pytest
from skill_test_support import revision
from test_skill_graph_convergence import _activate_with_report
from test_workspace_skills import call, tools_for

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import MARKDOWN_SKILL_SCHEMA, SkillAsset
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import digest_bytes
from ahc.infrastructure.skill_asset_store import read_asset


def modern(**kwargs):
    return replace(
        revision(),
        schema_version=MARKDOWN_SKILL_SCHEMA,
        body="# 說明\r\n\r\n```python\r\n# execute subprocess with existing authority\r\n``` \r\n",
        body_digest=None,
        revision_digest=None,
        **kwargs,
    )


def resource(path="references/guide.md", content=b"read me"):
    return SkillAsset(path, digest_bytes(content), len(content))


def test_markdown_roundtrip_is_lossless_and_v1_remains_strict():
    item = modern()
    assert SkillRevision.from_dict(item.to_dict()) == item
    assert "\r\n" in item.body and "\u00a0" in item.body
    with pytest.raises(ContractError, match="fenced"):
        replace(
            item, schema_version="ahc.skill-revision.v1", body_digest=None, revision_digest=None
        )
    old = revision()
    assert "assets" not in old.to_dict()
    assert SkillRevision.from_dict(old.to_dict()) == old


@pytest.mark.parametrize(
    "path",
    [
        "../policy",
        "/etc/file",
        "a/../../b",
        "C:/a",
        "a\\b",
        "a:stream",
        "a/CON.txt",
        "a./b",
        "SKILL.md",
        "a//b",
    ],
)
def test_resource_paths_reject_escapes_and_aliases(path):
    with pytest.raises(ContractError):
        resource(path)


def test_case_collision_missing_bytes_and_tampering_are_rejected(repository):
    asset = resource()
    with pytest.raises(ContractError, match="collide"):
        modern(assets=(asset, resource("REFERENCES/guide.md")))
    item = modern(assets=(asset,))
    with pytest.raises(ContractError, match="missing"):
        repository.register_revision(item)
    repository.stage_assets(item.assets, {asset.path: b"read me"})
    repository.register_revision(item)  # the incomplete pre-registration is recoverable
    assert repository.get_revision(item.revision_digest) == item
    (repository.root / "blobs" / asset.sha256).write_bytes(b"changed")
    with pytest.raises(ContractError):
        repository.get_revision(item.revision_digest)
    with pytest.raises(ContractError):
        repository.stage_assets(item.assets, {asset.path: b"read me"})


def test_asset_metadata_is_digest_bound():
    first = modern(assets=(resource(),))
    second = modern(assets=(resource(content=b"new text"),))
    assert first.revision_digest != second.revision_digest
    assert first.body_digest == second.body_digest


def test_resources_and_binary_content_read_via_pinned_tools(repository):
    values = {"references/guide.md": "甲乙😀\r\n".encode(), "assets/data.bin": b"\xff\x00\x01"}
    assets = tuple(resource(path, value) for path, value in values.items())
    item = modern(assets=assets)
    repository.stage_assets(assets, values)
    _activate_with_report(repository, item, "markdown")
    tools = tools_for(repository)
    result = call(tools, "skills_resources", revision_digest=item.revision_digest, limit=1)
    assert result["result"]["next_offset"] == 1
    assert result["result"]["total"] == 2
    text = ""
    offset = 0
    while offset is not None:
        result = call(
            tools,
            "skills_read",
            revision_digest=item.revision_digest,
            path="references/guide.md",
            offset=offset,
            limit=2,
        )["result"]
        text += result["text"]
        offset = result["next_offset"]
    assert text.encode() == values["references/guide.md"]
    result = call(
        tools, "skills_read", revision_digest=item.revision_digest, path="assets/data.bin"
    )["result"]
    assert result["encoding"] == "base64"
    assert base64.b64decode(result["text"]) == values["assets/data.bin"]
    for path in ("../writer.lock", "unknown", None):
        assert (
            call(tools, "skills_read", revision_digest=item.revision_digest, path=path)[
                "disposition"
            ]
            == "rejected"
        )


def test_asset_dedup_is_content_addressed_and_does_not_execute(repository):
    value = b"raise AssertionError('must never run')"
    assets = (resource("scripts/a.py", value), resource("scripts/b.py", value))
    repository.stage_assets(assets, {asset.path: value for asset in assets})
    assert len(list((repository.root / "blobs").iterdir())) == 1
    assert read_asset(repository.root, assets[0]) == value


def test_instruction_does_not_grant_new_tool_capability(repository):
    item = modern()
    _activate_with_report(repository, item, "instruction")
    tools = tools_for(repository)
    before = tools.capabilities()
    assert (
        call(tools, "skills_read", revision_digest=item.revision_digest)["disposition"]
        == "observed"
    )
    assert tools.capabilities() == before
    assert tools.invoke("workspace.deploy", {})["disposition"] == "unsupported"
