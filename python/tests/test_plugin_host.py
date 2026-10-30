from __future__ import annotations

from copy import deepcopy

import pytest
from plugin_test_support import agent_binding, global_binding, plugin_manifest, profile_registry

from ahc.application.fake_plugin import create_fake_plugin_host
from ahc.application.plugin_doctor import inspect_native_plugin_host
from ahc.application.plugin_host import NativePluginHost
from ahc.domain import (
    NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
    ContractError,
    PluginBinding,
    PluginInvocationRequest,
)
from ahc.domain.skill_values import digest_json


def _request(
    binding_id: str,
    capability_id: str = "memory.read",
    *,
    invocation_id: str = "invoke-1",
    agent_id: str = "hexi",
    payload: dict[str, object] | None = None,
) -> tuple[PluginInvocationRequest, dict[str, object]]:
    actual = {"query": "hello"} if payload is None else payload
    return (
        PluginInvocationRequest(
            NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
            invocation_id,
            agent_id,
            binding_id,
            capability_id,
            digest_json(actual),
        ),
        actual,
    )


def test_public_host_rejects_caller_owned_catalog_without_invocation() -> None:
    manifest = plugin_manifest("caller.adhoc")
    calls = 0

    def caller_factory(*_args):
        nonlocal calls
        calls += 1
        raise AssertionError("caller factory must never run")

    with pytest.raises(TypeError):
        NativePluginHost(profile_registry(), (manifest,), {manifest.plugin_id: caller_factory})  # type: ignore[call-arg]
    assert calls == 0

    host = NativePluginHost(profile_registry())
    with pytest.raises(ContractError, match="unknown manifest"):
        host.admit(agent_binding(manifest, binding_id="caller-owned"))
    assert calls == 0

    package_host, factories = create_fake_plugin_host(profile_registry())
    assert package_host.manifest("test.memory").plugin_id == "test.memory"
    assert factories["test.memory"].construct_count == 0


def test_admission_binds_exact_profile_resource_and_never_constructs() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    factory = factories[manifest.plugin_id]

    admitted = host.admit(agent_binding(manifest))
    assert admitted.status == "unavailable"
    assert factory.construct_count == 0

    with pytest.raises(ContractError, match="profile plugin reference"):
        host.admit(agent_binding(manifest, binding_id="wrong-ref", plugin_ref="hexi.workspace"))
    with pytest.raises(ContractError, match="profile plugin reference"):
        host.admit(agent_binding(manifest, binding_id="cross", plugin_ref="lizi.plugin"))
    with pytest.raises(ContractError, match="grant"):
        host.admit(agent_binding(manifest, binding_id="grant", grants=("not-admitted",)))
    assert factory.construct_count == 0


def test_global_binding_retains_caller_agent_without_shared_profile_authority() -> None:
    host, _ = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    binding = global_binding(manifest)
    host.admit(binding)
    host.open(binding.binding_id)

    request, payload = _request(binding.binding_id)
    receipt, result = host.invoke(request, payload)
    assert receipt.agent_id == "hexi"
    assert receipt.outcome == "succeeded"
    assert result == {"capability_id": "memory.read", "payload": payload}

    unknown, payload = _request(
        binding.binding_id, invocation_id="unknown-agent", agent_id="missing"
    )
    with pytest.raises(ContractError, match="unknown agent"):
        host.invoke(unknown, payload)


def test_doctor_projects_absent_disabled_incompatible_and_unavailable_without_effects() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    disabled = host.manifest("test.disabled")
    incompatible = host.manifest("test.future")
    missing = host.manifest("test.missing")
    host.admit(agent_binding(disabled, binding_id="disabled", enabled=False))
    host.admit(agent_binding(incompatible, binding_id="future", grants=("memory.read",)))
    host.admit(agent_binding(missing, binding_id="missing-factory"))

    assert host.readiness("not-known").status == "absent"
    report = inspect_native_plugin_host(host)
    statuses = {item["binding_id"]: item["status"] for item in report["bindings"]}
    assert statuses == {
        "disabled": "disabled",
        "future": "incompatible",
        "missing-factory": "unavailable",
    }
    assert inspect_native_plugin_host(None) == {
        "schema_version": "ahc.native-plugin-doctor.v1",
        "status": "not-configured",
        "bindings": [],
    }
    assert all(factory.construct_count == 0 for factory in factories.values())


def test_close_preserves_never_constructed_readiness_arms() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    cases = (
        ("disabled", "test.disabled", False, "disabled"),
        ("future", "test.future", True, "incompatible"),
        ("missing", "test.missing", True, "unavailable"),
    )
    for binding_id, plugin_id, enabled, status in cases:
        manifest = host.manifest(plugin_id)
        host.admit(
            agent_binding(
                manifest,
                binding_id=binding_id,
                enabled=enabled,
                grants=("memory.read",),
            )
        )
        assert host.close(binding_id).status == status
        assert host.readiness(binding_id).status == status
    assert all(factory.construct_count == 0 for factory in factories.values())


def test_open_exposes_ready_degraded_and_unavailable_adapter_states() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    for status in ("ready", "degraded", "unavailable"):
        manifest = host.manifest(f"test.{status}")
        host.admit(agent_binding(manifest, binding_id=status, grants=("memory.read",)))
        opened = host.open(status)
        assert opened.status == status
        assert factories[f"test.{status}"].construct_count == 1
        assert factories[f"test.{status}"].last_adapter.open_count == 1


@pytest.mark.parametrize(
    "plugin_id",
    ["test.unavailable", "test.open-exception", "test.open-malformed"],
)
def test_repeated_unsuccessful_open_retains_one_closeable_adapter(plugin_id: str) -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest(plugin_id)
    factory = factories[plugin_id]
    host.admit(agent_binding(manifest, binding_id=plugin_id, grants=("memory.read",)))

    assert host.open(plugin_id).status == "unavailable"
    adapter = factory.last_adapter
    assert host.open(plugin_id).status == "unavailable"
    assert (factory.construct_count, adapter.open_count, adapter.close_count) == (1, 1, 0)

    assert host.close(plugin_id).status == "unavailable"
    assert adapter.close_count == 1
    assert host.open(plugin_id).status == "unavailable"
    assert factory.construct_count == 2
    assert factory.last_adapter is not adapter


def test_invocation_authority_precedes_capability_and_payload_processing() -> None:
    global_host, global_factories = create_fake_plugin_host(profile_registry())
    global_manifest = global_host.manifest("test.memory")
    global_binding_value = global_binding(global_manifest)
    global_host.admit(global_binding_value)
    global_host.open(global_binding_value.binding_id)
    valid_payload: dict[str, object] = {"query": "valid"}
    mismatched_payload: dict[str, object] = {"query": "mismatched"}

    unknown_agent, _ = _request(
        global_binding_value.binding_id,
        invocation_id="unknown-before-payload",
        agent_id="missing",
        payload=valid_payload,
    )
    with pytest.raises(ContractError, match="unknown agent"):
        global_host.invoke(unknown_agent, mismatched_payload)

    unknown_capability, payload = _request(
        global_binding_value.binding_id,
        "memory.unknown",
        invocation_id="unknown-before-capability",
        agent_id="missing",
        payload=valid_payload,
    )
    with pytest.raises(ContractError, match="unknown agent"):
        global_host.invoke(unknown_capability, payload)
    assert global_factories[global_manifest.plugin_id].last_adapter.invoke_count == 0

    agent_host, agent_factories = create_fake_plugin_host(profile_registry())
    agent_manifest = agent_host.manifest("test.memory")
    agent_binding_value = agent_binding(agent_manifest)
    agent_host.admit(agent_binding_value)
    agent_host.open(agent_binding_value.binding_id)
    wrong_agent, _ = _request(
        agent_binding_value.binding_id,
        invocation_id="wrong-agent-before-payload",
        agent_id="lizi",
        payload=valid_payload,
    )
    with pytest.raises(ContractError, match="caller does not own agent binding"):
        agent_host.invoke(wrong_agent, mismatched_payload)
    assert agent_factories[agent_manifest.plugin_id].last_adapter.invoke_count == 0

    unopened_host, unopened_factories = create_fake_plugin_host(profile_registry())
    unopened_manifest = unopened_host.manifest("test.memory")
    unopened = agent_binding(unopened_manifest, binding_id="unopened")
    unopened_host.admit(unopened)
    unopened_request, _ = _request(
        unopened.binding_id,
        invocation_id="not-ready-before-payload",
        payload=valid_payload,
    )
    with pytest.raises(ContractError, match="plugin is not ready"):
        unopened_host.invoke(unopened_request, mismatched_payload)
    assert unopened_factories[unopened_manifest.plugin_id].construct_count == 0

    read_only_host, read_only_factories = create_fake_plugin_host(profile_registry())
    read_only_manifest = read_only_host.manifest("test.memory")
    read_only = agent_binding(
        read_only_manifest,
        binding_id="precedence-read-only",
        grants=("memory.read",),
    )
    read_only_host.admit(read_only)
    read_only_host.open(read_only.binding_id)
    ungranted, _ = _request(
        read_only.binding_id,
        "memory.write",
        invocation_id="grant-before-payload",
        payload=valid_payload,
    )
    receipt, result = read_only_host.invoke(ungranted, mismatched_payload, approved=True)
    assert (receipt.outcome, receipt.reason_code, result) == (
        "rejected",
        "capability-not-granted",
        None,
    )

    unknown, _ = _request(
        read_only.binding_id,
        "memory.unknown",
        invocation_id="capability-before-payload",
        payload=valid_payload,
    )
    with pytest.raises(ContractError, match="unknown capability_id"):
        read_only_host.invoke(unknown, mismatched_payload)
    assert read_only_factories[read_only_manifest.plugin_id].last_adapter.invoke_count == 0


def test_invocation_enforces_grants_approval_digests_and_outcomes() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    factory = factories[manifest.plugin_id]
    binding = agent_binding(manifest)
    host.admit(binding)
    host.open(binding.binding_id)

    request, payload = _request(binding.binding_id)
    receipt, result = host.invoke(request, payload)
    assert receipt.outcome == "succeeded"
    assert receipt.effect == "read"
    assert receipt.request_digest == digest_json(request.to_dict())
    assert receipt.result_digest == digest_json(result)
    assert factory.last_adapter.invoke_count == 1

    write, write_payload = _request(
        binding.binding_id, "memory.write", invocation_id="write-denied"
    )
    denied, result = host.invoke(write, write_payload)
    assert (denied.outcome, denied.effect, result) == ("rejected", "write", None)
    assert factory.last_adapter.invoke_count == 1

    approved, approved_payload = _request(
        binding.binding_id, "memory.write", invocation_id="write-approved"
    )
    accepted, result = host.invoke(approved, approved_payload, approved=True)
    assert (accepted.outcome, accepted.effect) == ("succeeded", "write")
    assert result is not None
    assert factory.last_adapter.invoke_count == 2

    bad_digest = deepcopy(approved_payload)
    bad_digest["query"] = "changed"
    with pytest.raises(ContractError, match="payload_digest"):
        host.invoke(_request(binding.binding_id, invocation_id="bad-digest")[0], bad_digest)


def test_empty_grants_are_least_authority_and_reject_before_adapter_call() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    binding = agent_binding(manifest, binding_id="zero-grants", grants=())
    assert PluginBinding.from_dict(binding.to_dict()) == binding
    host.admit(binding)
    assert factories[manifest.plugin_id].construct_count == 0
    host.open(binding.binding_id)

    request, payload = _request(binding.binding_id, invocation_id="zero-grant-call")
    receipt, result = host.invoke(request, payload)
    assert (receipt.outcome, receipt.reason_code, result) == (
        "rejected",
        "capability-not-granted",
        None,
    )
    assert factories[manifest.plugin_id].last_adapter.invoke_count == 0


def test_manifest_drift_unknown_and_ungranted_capabilities_fail_closed() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    drifted = agent_binding(manifest, binding_id="drifted").to_dict()
    drifted["manifest_digest"] = "0" * 64
    with pytest.raises(ContractError, match="manifest_digest"):
        host.admit(PluginBinding.from_dict(drifted))

    binding = agent_binding(manifest, binding_id="read-only", grants=("memory.read",))
    host.admit(binding)
    host.open(binding.binding_id)
    factory = factories[manifest.plugin_id]

    unknown, payload = _request(
        binding.binding_id,
        "memory.unknown",
        invocation_id="unknown-capability",
    )
    with pytest.raises(ContractError, match="unknown capability_id"):
        host.invoke(unknown, payload)
    assert factory.last_adapter.invoke_count == 0

    ungranted, payload = _request(
        binding.binding_id,
        "memory.write",
        invocation_id="ungranted-capability",
    )
    receipt, result = host.invoke(ungranted, payload, approved=True)
    assert (receipt.outcome, receipt.reason_code, result) == (
        "rejected",
        "capability-not-granted",
        None,
    )
    assert factory.last_adapter.invoke_count == 0


def test_payload_and_result_require_recursive_json() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    binding = agent_binding(manifest)
    host.admit(binding)
    host.open(binding.binding_id)
    factory = factories[manifest.plugin_id]

    nested_tuple = {"outer": ("not-json",)}
    tuple_request, _ = _request(
        binding.binding_id,
        invocation_id="tuple-payload",
        payload=nested_tuple,  # type: ignore[arg-type]
    )
    with pytest.raises(ContractError, match="non-JSON"):
        host.invoke(tuple_request, nested_tuple)  # type: ignore[arg-type]
    assert factory.last_adapter.invoke_count == 0

    nested_integer_key = {"outer": {1: "not-json"}}
    integer_request, _ = _request(
        binding.binding_id,
        invocation_id="integer-key-payload",
        payload=nested_integer_key,  # type: ignore[arg-type]
    )
    with pytest.raises(ContractError, match="string keys"):
        host.invoke(integer_request, nested_integer_key)  # type: ignore[arg-type]
    assert factory.last_adapter.invoke_count == 0

    result_request, result_payload = _request(
        binding.binding_id,
        invocation_id="non-json-result",
        payload={"mode": "non-json-result"},
    )
    receipt, result = host.invoke(result_request, result_payload)
    assert (receipt.outcome, receipt.reason_code, result) == (
        "unknown",
        "adapter-exception",
        None,
    )
    assert factory.last_adapter.invoke_count == 1


def test_failure_unknown_generic_exception_duplicate_close_and_reopen_are_explicit() -> None:
    host, factories = create_fake_plugin_host(profile_registry())
    manifest = host.manifest("test.memory")
    factory = factories[manifest.plugin_id]
    binding = agent_binding(manifest)
    host.admit(binding)
    host.open(binding.binding_id)

    failed_request, failed_payload = _request(
        binding.binding_id, invocation_id="failed", payload={"mode": "fail-before"}
    )
    failed, result = host.invoke(failed_request, failed_payload)
    assert (failed.outcome, failed.reason_code, result) == ("failed", "fake-failed", None)

    unknown_request, unknown_payload = _request(
        binding.binding_id, invocation_id="unknown", payload={"mode": "fail-after"}
    )
    unknown, result = host.invoke(unknown_request, unknown_payload)
    assert (unknown.outcome, unknown.reason_code, result) == ("unknown", "fake-unknown", None)

    generic_request, generic_payload = _request(
        binding.binding_id, invocation_id="generic", payload={"mode": "raise-generic"}
    )
    generic, result = host.invoke(generic_request, generic_payload)
    assert (generic.outcome, generic.reason_code, result) == (
        "unknown",
        "adapter-exception",
        None,
    )
    assert "RAW" not in str(generic.to_dict())
    assert "private" not in str(generic.to_dict())

    with pytest.raises(ContractError, match="duplicate invocation_id"):
        host.invoke(unknown_request, unknown_payload)

    adapter = factory.last_adapter
    assert host.close(binding.binding_id).status == "unavailable"
    assert host.close(binding.binding_id).status == "unavailable"
    assert adapter.close_count == 1
    assert host.open(binding.binding_id).status == "ready"
    assert factory.construct_count == 2
