from __future__ import annotations

import json
from copy import deepcopy
from hashlib import sha256

import pytest
from plugin_test_support import agent_binding, plugin_manifest

from ahc.domain import (
    NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
    NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
    NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
    ContractError,
    NativePluginManifest,
    PluginBinding,
    PluginCapability,
    PluginEffect,
    PluginInvocationReceipt,
    PluginInvocationRequest,
    PluginReadiness,
)
from ahc.domain.plugin_values import bounded_json_object
from ahc.domain.skill_values import digest_json


def _digest(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return sha256(encoded).hexdigest()


def _wire_contract_witnesses():
    manifest = plugin_manifest()
    assert manifest.manifest_digest is not None
    binding = agent_binding(manifest)
    readiness = PluginReadiness(
        NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        "ready",
        "adapter-ready",
    )
    request = PluginInvocationRequest(
        NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
        "invoke-field-matrix",
        "hexi",
        binding.binding_id,
        "memory.read",
        _digest({"query": "matrix"}),
    )
    receipt = PluginInvocationReceipt(
        NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
        request.invocation_id,
        request.agent_id,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        request.capability_id,
        PluginEffect.READ,
        _digest(request.to_dict()),
        _digest({"ok": True}),
        "succeeded",
        "adapter-succeeded",
    )
    return {
        "capability": (PluginCapability.from_dict, manifest.capabilities[0].to_dict()),
        "manifest": (NativePluginManifest.from_dict, manifest.to_dict()),
        "binding": (PluginBinding.from_dict, binding.to_dict()),
        "readiness": (PluginReadiness.from_dict, readiness.to_dict()),
        "request": (PluginInvocationRequest.from_dict, request.to_dict()),
        "receipt": (PluginInvocationReceipt.from_dict, receipt.to_dict()),
    }


@pytest.mark.parametrize(
    ("kind", "field", "invalid", "match"),
    [
        ("capability", "capability_id", 1, "capability_id"),
        ("capability", "effect", 1, "effect"),
        ("capability", "approval_required", 1, "approval_required"),
        ("manifest", "schema_version", 1, "schema"),
        ("manifest", "plugin_id", 1, "plugin_id"),
        ("manifest", "api_version", 1, "api_version"),
        ("manifest", "binding_scopes", "agent", "JSON array"),
        ("manifest", "capabilities", "memory.read", "JSON array"),
        ("manifest", "capability_digest", 1, "capability_digest"),
        ("manifest", "manifest_digest", 1, "manifest_digest"),
        ("binding", "schema_version", 1, "schema"),
        ("binding", "binding_id", 1, "binding_id"),
        ("binding", "plugin_id", 1, "plugin_id"),
        ("binding", "manifest_digest", 1, "manifest_digest"),
        ("binding", "scope", 1, "binding scope"),
        ("binding", "agent_id", 1, "agent_id"),
        ("binding", "plugin_ref", 1, "plugin_ref"),
        ("binding", "enabled", 1, "enabled"),
        ("binding", "grants", "memory.read", "JSON array"),
        ("readiness", "schema_version", 1, "schema"),
        ("readiness", "binding_id", 1, "binding_id"),
        ("readiness", "plugin_id", 1, "plugin_id"),
        ("readiness", "manifest_digest", 1, "manifest_digest"),
        ("readiness", "status", 1, "readiness status"),
        ("readiness", "reason_code", 1, "reason_code"),
        ("request", "schema_version", 1, "schema"),
        ("request", "invocation_id", 1, "invocation_id"),
        ("request", "agent_id", 1, "agent_id"),
        ("request", "binding_id", 1, "binding_id"),
        ("request", "capability_id", 1, "capability_id"),
        ("request", "payload_digest", 1, "payload_digest"),
        ("receipt", "schema_version", 1, "schema"),
        ("receipt", "invocation_id", 1, "invocation_id"),
        ("receipt", "agent_id", 1, "agent_id"),
        ("receipt", "binding_id", 1, "binding_id"),
        ("receipt", "plugin_id", 1, "plugin_id"),
        ("receipt", "manifest_digest", 1, "manifest_digest"),
        ("receipt", "capability_id", 1, "capability_id"),
        ("receipt", "effect", 1, "effect"),
        ("receipt", "request_digest", 1, "request_digest"),
        ("receipt", "result_digest", 1, "result_digest"),
        ("receipt", "outcome", 1, "invocation outcome"),
        ("receipt", "reason_code", 1, "reason_code"),
    ],
)
def test_every_wire_field_has_a_single_axis_type_or_digest_oracle(
    kind: str, field: str, invalid: object, match: str
) -> None:
    loader, valid = _wire_contract_witnesses()[kind]
    mutated = deepcopy(valid)
    mutated[field] = invalid
    with pytest.raises(ContractError, match=match):
        loader(mutated)


def test_manifest_round_trip_issues_canonical_capability_and_manifest_digests() -> None:
    manifest = plugin_manifest()
    payload = manifest.to_dict()
    capability_payload = [item.to_dict() for item in manifest.capabilities]
    expected_capability = _digest(capability_payload)
    expected_manifest = _digest(
        {key: value for key, value in payload.items() if key != "manifest_digest"}
    )

    assert manifest.capability_digest == expected_capability
    assert manifest.manifest_digest == expected_manifest
    assert NativePluginManifest.from_dict(payload) == manifest


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda value: value.update({"legacy": True}), "unknown keys"),
        (lambda value: value.update({"schema_version": "ahc.native-plugin-manifest.v2"}), "schema"),
        (lambda value: value.update({"capability_digest": "0" * 64}), "capability_digest"),
        (lambda value: value.update({"manifest_digest": "0" * 64}), "manifest_digest"),
        (
            lambda value: value["capabilities"][0].update({"approval_required": 1}),
            "approval_required",
        ),
    ],
)
def test_manifest_rejects_unknown_version_digest_and_non_strict_values(
    mutation, match: str
) -> None:
    payload = deepcopy(plugin_manifest().to_dict())
    mutation(payload)
    with pytest.raises(ContractError, match=match):
        NativePluginManifest.from_dict(payload)


def test_manifest_rejects_unsorted_duplicate_and_unknown_capability_values() -> None:
    payload = plugin_manifest().to_dict()
    payload["capabilities"] = list(reversed(payload["capabilities"]))
    with pytest.raises(ContractError, match="sorted"):
        NativePluginManifest.from_dict(payload)

    payload = plugin_manifest().to_dict()
    payload["capabilities"] *= 2
    with pytest.raises(ContractError, match="duplicate"):
        NativePluginManifest.from_dict(payload)

    payload = plugin_manifest().to_dict()
    payload["capabilities"][0]["effect"] = "mixed"
    with pytest.raises(ContractError, match="effect"):
        NativePluginManifest.from_dict(payload)


def test_binding_round_trip_enforces_agent_and_global_arms() -> None:
    manifest = plugin_manifest()
    binding = agent_binding(manifest)
    assert PluginBinding.from_dict(binding.to_dict()) == binding

    bad = binding.to_dict()
    bad["enabled"] = 1
    with pytest.raises(ContractError, match="enabled"):
        PluginBinding.from_dict(bad)

    bad = binding.to_dict()
    bad["agent_id"] = None
    with pytest.raises(ContractError, match="agent scope"):
        PluginBinding.from_dict(bad)

    bad = binding.to_dict()
    bad.update({"scope": "global", "agent_id": "hexi", "plugin_ref": None})
    with pytest.raises(ContractError, match="global scope"):
        PluginBinding.from_dict(bad)

    bad = binding.to_dict()
    bad["grants"] = binding.grants
    with pytest.raises(ContractError, match="grants must be a JSON array"):
        PluginBinding.from_dict(bad)


def test_manifest_wire_arrays_must_be_json_lists() -> None:
    manifest = plugin_manifest()
    payload = manifest.to_dict()
    payload["binding_scopes"] = manifest.binding_scopes
    with pytest.raises(ContractError, match="binding_scopes must be a JSON array"):
        NativePluginManifest.from_dict(payload)

    payload = manifest.to_dict()
    payload["capabilities"] = tuple(item.to_dict() for item in manifest.capabilities)
    with pytest.raises(ContractError, match="capabilities must be a JSON array"):
        NativePluginManifest.from_dict(payload)


@pytest.mark.parametrize(
    ("plugin_id", "manifest_digest"),
    [("test.memory", None), (None, "1" * 64)],
)
def test_absent_readiness_rejects_partial_plugin_identity(
    plugin_id: str | None, manifest_digest: str | None
) -> None:
    with pytest.raises(ContractError, match="both be null or both be present"):
        PluginReadiness(
            NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
            "missing-binding",
            plugin_id,
            manifest_digest,
            "absent",
            "binding-absent",
        )

    valid = PluginReadiness(
        NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
        "missing-binding",
        None,
        None,
        "absent",
        "binding-absent",
    )
    assert PluginReadiness.from_dict(valid.to_dict()) == valid


@pytest.mark.parametrize(
    "value",
    [
        {"outer": {1: "integer-key"}},
        {"outer": {"1": "string-key", 1: "collision"}},
        {"outer": ("tuple",)},
        {"outer": {"set"}},
        {"outer": float("nan")},
    ],
)
def test_bounded_json_object_rejects_non_json_values_at_every_depth(value: object) -> None:
    with pytest.raises(ContractError, match="string keys|non-JSON|canonical JSON"):
        bounded_json_object(value, "plugin payload")


def test_valid_nested_json_retains_its_canonical_digest() -> None:
    value = {"outer": [{"flag": True, "count": 1}, None]}
    assert bounded_json_object(value, "plugin payload") is value
    assert digest_json(value) == _digest(value)


def test_readiness_request_and_receipt_are_strict_round_trip_contracts() -> None:
    manifest = plugin_manifest()
    binding = agent_binding(manifest)
    readiness = PluginReadiness(
        NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        "ready",
        "adapter-ready",
    )
    request = PluginInvocationRequest(
        NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
        "invoke-1",
        "hexi",
        binding.binding_id,
        "memory.read",
        _digest({"query": "hello"}),
    )
    receipt = PluginInvocationReceipt(
        NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
        request.invocation_id,
        request.agent_id,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        request.capability_id,
        PluginEffect.READ,
        _digest(request.to_dict()),
        _digest({"ok": True}),
        "succeeded",
        "adapter-succeeded",
    )

    assert PluginReadiness.from_dict(readiness.to_dict()) == readiness
    assert PluginInvocationRequest.from_dict(request.to_dict()) == request
    assert PluginInvocationReceipt.from_dict(receipt.to_dict()) == receipt

    unknown = receipt.to_dict()
    unknown["provider_error"] = "must-not-exist"
    with pytest.raises(ContractError, match="unknown keys"):
        PluginInvocationReceipt.from_dict(unknown)


def test_all_wire_contracts_reject_unknown_keys_and_schema_drift() -> None:
    manifest = plugin_manifest()
    assert manifest.manifest_digest is not None
    binding = agent_binding(manifest)
    readiness = PluginReadiness(
        NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        "ready",
        "adapter-ready",
    )
    request = PluginInvocationRequest(
        NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
        "invoke-matrix",
        "hexi",
        binding.binding_id,
        "memory.read",
        _digest({"query": "hello"}),
    )
    receipt = PluginInvocationReceipt(
        NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
        request.invocation_id,
        request.agent_id,
        binding.binding_id,
        manifest.plugin_id,
        manifest.manifest_digest,
        request.capability_id,
        PluginEffect.READ,
        _digest(request.to_dict()),
        _digest({"ok": True}),
        "succeeded",
        "adapter-succeeded",
    )
    contracts = (
        (NativePluginManifest.from_dict, manifest.to_dict()),
        (PluginBinding.from_dict, binding.to_dict()),
        (PluginReadiness.from_dict, readiness.to_dict()),
        (PluginInvocationRequest.from_dict, request.to_dict()),
        (PluginInvocationReceipt.from_dict, receipt.to_dict()),
    )
    for loader, valid in contracts:
        extra = deepcopy(valid)
        extra["unknown"] = True
        with pytest.raises(ContractError, match="unknown keys"):
            loader(extra)

        drifted = deepcopy(valid)
        drifted["schema_version"] = "ahc.unsupported.v999"
        with pytest.raises(ContractError, match="schema"):
            loader(drifted)


@pytest.mark.parametrize(
    ("outcome", "result_digest"),
    [("succeeded", None), ("failed", "1" * 64), ("rejected", "1" * 64), ("unknown", "1" * 64)],
)
def test_receipt_result_digest_matches_outcome(outcome: str, result_digest: str | None) -> None:
    manifest = plugin_manifest()
    with pytest.raises(ContractError, match="result_digest"):
        PluginInvocationReceipt(
            NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
            "invoke-1",
            "hexi",
            "hexi.memory",
            manifest.plugin_id,
            manifest.manifest_digest,
            "memory.read",
            PluginEffect.READ,
            "2" * 64,
            result_digest,
            outcome,
            "test-outcome",
        )
