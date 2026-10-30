"""T0/T1 acceptance for the M2.3-Lite capability authority core."""

from __future__ import annotations

import ast
import builtins
import hashlib
import json
import os
import socket
import subprocess
import tomllib
from pathlib import Path

import pytest

from ahc.application.capability_composition import (
    compose_capability_views,
    compose_optional_capability_views,
)
from ahc.application.capability_doctor import inspect_capability_authority
from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    CapabilityGrant,
    DecisionKind,
    DecisionReason,
    EffectClass,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError

_SCALAR_FIELDS = {
    "persona_ref": ProfileResourceKind.PERSONA,
    "workspace_ref": ProfileResourceKind.WORKSPACE,
    "backend_profile_ref": ProfileResourceKind.BACKEND_PROFILE,
    "credential_ref": ProfileResourceKind.CREDENTIAL,
    "working_memory_ref": ProfileResourceKind.WORKING_MEMORY,
    "durable_memory_ref": ProfileResourceKind.DURABLE_MEMORY,
    "subagent_policy_ref": ProfileResourceKind.SUBAGENT_POLICY,
    "cron_policy_ref": ProfileResourceKind.CRON_POLICY,
}
_LIST_FIELDS = {
    "plugin_refs": ProfileResourceKind.PLUGIN,
    "channel_refs": ProfileResourceKind.CHANNEL,
}


def _profile_registry_payload() -> dict[str, object]:
    profiles: list[dict[str, object]] = []
    resources: list[dict[str, str]] = []
    for agent_id in ("hexi", "lizi"):
        profile: dict[str, object] = {
            "schema_version": "ahc.agent-profile.v1",
            "agent_id": agent_id,
        }
        for field_name, kind in _SCALAR_FIELDS.items():
            resource_id = f"{agent_id}.{kind.value}"
            profile[field_name] = resource_id
            resources.append(
                {
                    "schema_version": "ahc.profile-resource.v1",
                    "resource_id": resource_id,
                    "kind": kind.value,
                    "owner_agent_id": agent_id,
                }
            )
        for field_name, kind in _LIST_FIELDS.items():
            resource_id = f"{agent_id}.{kind.value}"
            profile[field_name] = [resource_id]
            resources.append(
                {
                    "schema_version": "ahc.profile-resource.v1",
                    "resource_id": resource_id,
                    "kind": kind.value,
                    "owner_agent_id": agent_id,
                }
            )
        profiles.append(profile)
    return {
        "schema_version": "ahc.agent-profile-registry.v1",
        "default_agent_id": "hexi",
        "profiles": profiles,
        "resources": resources,
    }


def _registry() -> AgentProfileRegistry:
    return AgentProfileRegistry.from_dict(_profile_registry_payload())


def _definitions() -> list[CapabilityDefinition]:
    return [
        CapabilityDefinition(
            "workspace.read",
            ProfileResourceKind.WORKSPACE,
            EffectClass.READ_ONLY,
            ApprovalMode.NONE,
        ),
        CapabilityDefinition(
            "memory.cache",
            ProfileResourceKind.WORKING_MEMORY,
            EffectClass.REVERSIBLE_LOCAL,
            ApprovalMode.NONE,
        ),
        CapabilityDefinition(
            "channel.send",
            ProfileResourceKind.CHANNEL,
            EffectClass.EXTERNAL_OR_IRREVERSIBLE,
            ApprovalMode.REQUIRED,
        ),
    ]


def _grants() -> list[CapabilityGrant]:
    return [
        CapabilityGrant("hexi", "hexi.workspace", "workspace.read"),
        CapabilityGrant("lizi", "lizi.channel", "channel.send"),
    ]


def test_complete_policy_issues_only_bound_opaque_views_and_exact_decisions() -> None:
    # L-T1-01, L-T1-07..L-T1-11
    composition = compose_capability_views(
        profile_registry=_registry(), definitions=_definitions(), grants=_grants()
    )
    hexi = composition.view("hexi")
    lizi = composition.view("lizi")
    assert not hasattr(hexi, "agent_id")
    assert not hasattr(hexi, "policy")
    assert hexi.decide("workspace.read", "hexi.workspace", EffectClass.READ_ONLY) == (
        _decision(
            DecisionKind.ALLOW, DecisionReason.ALLOWED, "workspace.read", EffectClass.READ_ONLY
        )
    )
    assert hexi.decide(
        "workspace.read", "hexi.workspace", EffectClass.REVERSIBLE_LOCAL
    ) == _decision(
        DecisionKind.DENY,
        DecisionReason.EFFECT_MISMATCH,
        "workspace.read",
        EffectClass.READ_ONLY,
    )
    assert hexi.decide(
        "channel.send", "hexi.channel", EffectClass.EXTERNAL_OR_IRREVERSIBLE
    ) == _decision(
        DecisionKind.DENY,
        DecisionReason.CAPABILITY_NOT_GRANTED,
        "channel.send",
        EffectClass.EXTERNAL_OR_IRREVERSIBLE,
    )
    assert hexi.decide("capability.missing", "hexi.workspace", EffectClass.READ_ONLY) == _decision(
        DecisionKind.DENY,
        DecisionReason.CAPABILITY_UNKNOWN,
        "capability.missing",
        None,
    )
    assert lizi.decide(
        "channel.send", "lizi.channel", EffectClass.EXTERNAL_OR_IRREVERSIBLE
    ) == _decision(
        DecisionKind.REQUIRES_APPROVAL,
        DecisionReason.APPROVAL_REQUIRED,
        "channel.send",
        EffectClass.EXTERNAL_OR_IRREVERSIBLE,
    )
    assert hexi.decide("workspace.read", "lizi.workspace", EffectClass.READ_ONLY) == _decision(
        DecisionKind.DENY,
        DecisionReason.RESOURCE_NOT_OWNED,
        "workspace.read",
        EffectClass.READ_ONLY,
    )


@pytest.mark.parametrize(
    "grant,match",
    [
        (CapabilityGrant("missing", "hexi.workspace", "workspace.read"), "unknown agent"),
        (CapabilityGrant("hexi", "missing.workspace", "workspace.read"), "unknown resource"),
        (CapabilityGrant("hexi", "lizi.workspace", "workspace.read"), "owner boundary"),
        (CapabilityGrant("hexi", "hexi.plugin", "workspace.read"), "resource kind"),
        (CapabilityGrant("hexi", "hexi.workspace", "capability.missing"), "unknown capability"),
    ],
)
def test_invalid_agent_resource_owner_kind_or_capability_rejects_all_views(
    grant: CapabilityGrant, match: str
) -> None:
    # L-T1-02, L-T1-03
    with pytest.raises(ContractError, match=match):
        compose_capability_views(
            profile_registry=_registry(), definitions=_definitions(), grants=[grant]
        )


def test_duplicate_policy_rows_and_unsafe_external_definition_reject() -> None:
    # L-T1-04, L-T1-05
    definition = _definitions()[0]
    with pytest.raises(ContractError, match="duplicate capability definition"):
        compose_capability_views(
            profile_registry=_registry(), definitions=[definition, definition], grants=[]
        )
    grant = _grants()[0]
    with pytest.raises(ContractError, match="duplicate agent-capability grant"):
        compose_capability_views(
            profile_registry=_registry(), definitions=_definitions(), grants=[grant, grant]
        )
    with pytest.raises(ContractError, match="require approval"):
        CapabilityDefinition(
            "unsafe.external",
            ProfileResourceKind.CHANNEL,
            EffectClass.EXTERNAL_OR_IRREVERSIBLE,
            ApprovalMode.NONE,
        )


def test_composition_detaches_every_caller_owned_value_and_container() -> None:
    # L-T1-06
    definitions = _definitions()
    grants = _grants()
    original_definition = definitions[0]
    original_grant = grants[0]
    composition = compose_capability_views(
        profile_registry=_registry(), definitions=definitions, grants=grants
    )
    before = composition.view("hexi").decide(
        "workspace.read", "hexi.workspace", EffectClass.READ_ONLY
    )
    object.__setattr__(original_definition, "capability_id", "mutated.definition")
    object.__setattr__(original_grant, "agent_id", "lizi")
    object.__setattr__(original_grant, "resource_id", "lizi.workspace")
    definitions.clear()
    grants.clear()
    assert (
        composition.view("hexi").decide("workspace.read", "hexi.workspace", EffectClass.READ_ONLY)
        == before
    )
    assert (
        composition.view("hexi")
        .decide("workspace.read", "lizi.workspace", EffectClass.READ_ONLY)
        .reason
        is DecisionReason.RESOURCE_NOT_OWNED
    )
    assert composition.view("hexi").decide(
        "mutated.definition", "hexi.workspace", EffectClass.READ_ONLY
    ).kind is (DecisionKind.DENY)


def test_absent_or_invalid_optional_policy_is_available_but_authority_denied() -> None:
    # Lower-level helper regression only. L-T1-12 remains OPEN until real foreground integration.
    registry = _registry()
    absent = compose_optional_capability_views(
        profile_registry=registry, definitions=None, grants=None
    )
    assert absent.composition is None
    assert inspect_capability_authority(absent.snapshot)["status"] == "not-configured"
    invalid = compose_optional_capability_views(
        profile_registry=registry,
        definitions=_definitions(),
        grants=[CapabilityGrant("hexi", "lizi.workspace", "workspace.read")],
    )
    assert invalid.composition is None
    report = inspect_capability_authority(invalid.snapshot)
    assert report["status"] == "invalid-policy"
    assert all(item["configured_count"] == 0 for item in report["agents"])


def test_doctor_is_pure_sanitized_and_never_overstates_grants(monkeypatch) -> None:
    # L-T1-13, L-T1-14
    composition = compose_capability_views(
        profile_registry=_registry(), definitions=_definitions(), grants=_grants()
    )
    counters = {"open": 0, "env": 0, "network": 0, "process": 0}

    def forbidden(name: str):
        def call(*_args, **_kwargs):
            counters[name] += 1
            raise AssertionError(f"doctor attempted forbidden {name} effect")

        return call

    monkeypatch.setattr(builtins, "open", forbidden("open"))
    monkeypatch.setattr(os, "getenv", forbidden("env"))
    monkeypatch.setattr(socket, "socket", forbidden("network"))
    monkeypatch.setattr(subprocess, "Popen", forbidden("process"))
    report = inspect_capability_authority(composition.snapshot)
    assert counters == {"open": 0, "env": 0, "network": 0, "process": 0}
    assert report == {
        "status": "configured",
        "agents": [
            {
                "agent_id": "hexi",
                "configured_count": 1,
                "denied_count": 2,
                "approval_required_count": 0,
            },
            {
                "agent_id": "lizi",
                "configured_count": 1,
                "denied_count": 2,
                "approval_required_count": 1,
            },
        ],
    }
    assert not any(token in repr(report).casefold() for token in ("private", "credential", "path"))


def test_static_scope_and_risk_projection_are_exact() -> None:
    # L-T0-01..03, L-RISK-01..06
    root = Path(__file__).resolve().parents[1]
    docs = root / "tests/fixtures/capability_history"
    source_paths = [
        root / "src/ahc/domain/capability_values.py",
        root / "src/ahc/domain/capability_policy.py",
        root / "src/ahc/application/capability_composition.py",
        root / "src/ahc/application/capability_view.py",
        root / "src/ahc/application/capability_doctor.py",
    ]
    test_paths = [
        root / "tests/test_capability_policy.py",
        root / "tests/test_capability_integration.py",
    ]
    forbidden = (
        "plugin_",
        "importlib",
        "sqlite3",
        "requests",
        "threading",
        "multiprocessing",
    )
    for path in source_paths:
        text = path.read_text(encoding="utf-8")
        assert len(text.splitlines()) < 300
        assert not any(token in text for token in forbidden)
    for path in test_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        imported_modules.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        assert "profile_test_support" not in imported_modules

    acceptance = json.loads((docs / "M2.3-LITE-ACCEPTANCE.json").read_text(encoding="utf-8"))
    amendment_path = docs / "M2.3-LITE-RESOURCE-BINDING-AMENDMENT.md"
    amendment = amendment_path.read_text(encoding="utf-8")
    evidence = json.loads((docs / "M2.3-LITE-ACCEPTANCE-EVIDENCE.json").read_text(encoding="utf-8"))
    with (root / "pyproject.toml").open("rb") as handle:
        package = tomllib.load(handle)
    assert package["project"]["dependencies"] == ["portalocker>=3.0", "psutil>=6.0"]

    ids = [row["id"] for row in acceptance["rows"]]
    assert len(ids) == len(set(ids)) == 28
    assert {
        tier: sum(row["tier"] == tier for row in acceptance["rows"]) for tier in ("T0", "T1", "T2")
    } == {"T0": 8, "T1": 15, "T2": 5}
    assert acceptance["implementationAuthorized"] is False
    assert evidence["baseFreeze"]["verdictTransfer"] is False
    assert evidence["ownerDecision"] == {
        "authorizedAt": "2026-08-29T00:38:53+08:00",
        "decision": "OPEN_BOUNDED_RB1_EPOCH",
        "directModuleOnly": True,
        "resourceDecisionSignature": "decide(capability_id, resource_id, requested_effect, /)",
        "lT112": "OPEN",
    }
    assert "resource_id: str" in amendment
    assert "requested_effect: EffectClass" in amendment
    assert "`L-T1-12` remains `OPEN`" in amendment
    assert (
        evidence["amendment"]["fixtureNormalizedSha256"]
        == hashlib.sha256(amendment.encode("utf-8")).hexdigest()
    )

    expected_allowlist = {
        "python/src/ahc/domain/capability_values.py",
        "python/src/ahc/domain/capability_policy.py",
        "python/src/ahc/application/capability_view.py",
        "python/src/ahc/application/capability_doctor.py",
        "python/src/ahc/application/capability_composition.py",
        "python/tests/test_capability_policy.py",
        "python/tests/test_capability_integration.py",
        "docs/python-standalone/M2.3-LITE-RESOURCE-BINDING-AMENDMENT.md",
        "docs/python-standalone/M2.3-LITE-ACCEPTANCE-EVIDENCE.json",
        "docs/python-standalone/CURRENT-M2.3-LITE-HANDOFF.md",
        "docs/python-standalone/PRODUCT-ROADMAP.md",
        "docs/python-standalone/PM-DASHBOARD.html",
    }
    assert set(evidence["writerAllowlist"]) == expected_allowlist
    assert {
        "python/src/ahc/__init__.py",
        "python/src/ahc/domain/__init__.py",
        "python/src/ahc/application/__init__.py",
        "python/tests/test_package.py",
        "python/src/ahc/cli/**",
        "python/src/ahc/application/supervisor.py",
    }.issubset(evidence["closedPathsAndEffects"])

    relative_paths = [
        path.relative_to(root.parent).as_posix() for path in source_paths + test_paths
    ]
    for relative_path in relative_paths:
        # This is the retained RB1 candidate, not the current development tree.
        frozen_hash = evidence["candidateHashes"][relative_path]
        assert len(frozen_hash) == 64 and all(c in "0123456789abcdef" for c in frozen_hash)
    for relative_path, expected_hash in evidence["readOnlyDependencyHashes"].items():
        assert relative_path and len(expected_hash) == 64
        assert all(c in "0123456789abcdef" for c in expected_hash)

    evidence_rows = {row["id"]: row for row in evidence["acceptanceRows"]}
    assert set(evidence_rows) == set(ids)
    assert evidence_rows["L-T1-12"]["status"] == "OPEN_OWNER_DEFERRED"
    assert evidence["summary"]["acceptedCount"] == 0

    pairs = [(row["consequence"], row["frequency"]) for row in acceptance["riskPolicy"]["matrix"]]
    assert len(pairs) == len(set(pairs)) == 9
    matrix = {
        (row["consequence"], row["frequency"]): row for row in acceptance["riskPolicy"]["matrix"]
    }
    assert matrix[("HIGH", "LOW")]["runtimeDisposition"] == "FAIL_CLOSED_BEFORE_EFFECT"
    assert matrix[("LOW", "LOW")]["mvpDisposition"].startswith("NO_MVP_BLOCK")
    assert "MEDIUM-frequency" in acceptance["riskPolicy"]["unknownFrequencyTreatment"]
    for consequence in ("LOW", "MEDIUM", "HIGH"):
        unknown_frequency_disposition = matrix[(consequence, "MEDIUM")]
        assert unknown_frequency_disposition["consequence"] == consequence

    required_finding_fields = {
        "id",
        "scopeClass",
        "consequence",
        "estimatedFrequency",
        "confidence",
        "firstEffectBoundary",
        "reversibilityContainment",
        "disposition",
        "affectedAcceptanceIds",
        "residualOwner",
        "repairTrigger",
    }
    findings = evidence["materialFindings"]
    assert {finding["id"] for finding in findings} == {"R1", "R2", "R3", "R4"}
    assert all(required_finding_fields <= set(finding) for finding in findings)
    assert all(set(finding["affectedAcceptanceIds"]) <= set(ids) for finding in findings)
    assert all(
        "FAIL_OPEN" not in finding["disposition"]
        for finding in findings
        if finding["consequence"] == "HIGH"
    )


def _decision(kind, reason, capability_id, effect):
    from ahc.domain.capability_values import CapabilityDecision

    return CapabilityDecision(kind, reason, capability_id, effect)
