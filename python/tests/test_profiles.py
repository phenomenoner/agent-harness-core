from __future__ import annotations

from copy import deepcopy

import pytest
from profile_test_support import profile_registry_payload

from ahc.domain import AgentProfileRegistry, ContractError, ProfileResource


def test_two_agent_profile_registry_round_trips_exactly() -> None:
    payload = profile_registry_payload()

    registry = AgentProfileRegistry.from_dict(payload)

    assert registry.to_dict() == payload
    assert registry.default_agent_id == "hexi"
    assert registry.profile("hexi").workspace_ref == "hexi.workspace"
    assert registry.profile("lizi").workspace_ref == "lizi.workspace"
    assert set(registry.profile("hexi").plugin_refs).isdisjoint(
        registry.profile("lizi").plugin_refs
    )


@pytest.mark.parametrize("duplicate_kind", ["profile", "resource", "list-ref"])
def test_registry_rejects_duplicate_authority(duplicate_kind: str) -> None:
    payload = profile_registry_payload()
    if duplicate_kind == "profile":
        payload["profiles"].append(deepcopy(payload["profiles"][0]))
    elif duplicate_kind == "resource":
        payload["resources"].append(deepcopy(payload["resources"][0]))
    else:
        payload["profiles"][0]["plugin_refs"] *= 2

    with pytest.raises(ContractError, match="duplicate"):
        AgentProfileRegistry.from_dict(payload)


@pytest.mark.parametrize("case", ["unknown", "wrong-kind"])
def test_registry_rejects_unknown_or_wrong_kind_reference(case: str) -> None:
    payload = profile_registry_payload()
    if case == "unknown":
        payload["profiles"][0]["workspace_ref"] = "hexi.unknown"
        expected = "unknown resource"
    else:
        payload["profiles"][0]["workspace_ref"] = "hexi.plugin"
        expected = "requires kind"

    with pytest.raises(ContractError, match=expected):
        AgentProfileRegistry.from_dict(payload)


@pytest.mark.parametrize(
    ("field_name", "foreign_ref"),
    [
        ("workspace_ref", "lizi.workspace"),
        ("credential_ref", "lizi.credential"),
        ("plugin_refs", ["lizi.plugin"]),
    ],
)
def test_registry_rejects_cross_agent_resource_reference(
    field_name: str, foreign_ref: str | list[str]
) -> None:
    payload = profile_registry_payload()
    payload["profiles"][0][field_name] = foreign_ref

    with pytest.raises(ContractError, match="crosses owner boundary"):
        AgentProfileRegistry.from_dict(payload)


def test_registry_rejects_unknown_resource_owner_and_unreferenced_resource() -> None:
    bad_owner = profile_registry_payload()
    bad_owner["resources"][0]["owner_agent_id"] = "unknown-agent"
    with pytest.raises(ContractError, match="owner does not identify"):
        AgentProfileRegistry.from_dict(bad_owner)

    orphan = profile_registry_payload()
    extra = deepcopy(orphan["resources"][0])
    extra["resource_id"] = "hexi.persona.extra"
    orphan["resources"].append(extra)
    with pytest.raises(ContractError, match="not referenced"):
        AgentProfileRegistry.from_dict(orphan)


def test_profile_schema_rejects_credential_value_without_echoing_it() -> None:
    sentinel = "RAW-SECRET-SENTINEL"
    payload = profile_registry_payload()
    payload["resources"][3]["value"] = sentinel

    with pytest.raises(ContractError) as raised:
        AgentProfileRegistry.from_dict(payload)

    assert "unknown keys" in str(raised.value)
    assert sentinel not in str(raised.value)


def test_registry_requires_default_agent_and_exact_root_fields() -> None:
    missing_default = profile_registry_payload()
    missing_default["default_agent_id"] = "missing"
    with pytest.raises(ContractError, match="default_agent_id"):
        AgentProfileRegistry.from_dict(missing_default)

    unknown = profile_registry_payload()
    unknown["legacy_agents"] = []
    with pytest.raises(ContractError, match="unknown keys"):
        AgentProfileRegistry.from_dict(unknown)


def test_profile_identifier_grammar_boundaries_are_exact() -> None:
    payload = {
        "schema_version": "ahc.profile-resource.v1",
        "resource_id": "a" * 128,
        "kind": "workspace",
        "owner_agent_id": "Z9._:-",
    }
    assert ProfileResource.from_dict(payload).resource_id == "a" * 128

    for invalid in ("a" * 129, " leading", "trailing ", "é", "a/b", "a\\b", "a!", "a\n"):
        payload["resource_id"] = invalid
        with pytest.raises(ContractError):
            ProfileResource.from_dict(payload)
