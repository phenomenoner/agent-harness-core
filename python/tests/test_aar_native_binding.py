"""Native identity is stricter than MCP tool discovery or public API visibility."""

from copy import deepcopy

import pytest

from ahc.adapters.aar_native_binding import NATIVE_TOOLS, NativeAarPin
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


def candidate():
    payload = {"schema_version": "aar.runtime.v1", "capabilities": [
        {"name": "rlm.execute", "access": "write", "limits": []},
        {"name": "rlm.status", "access": "read", "limits": []},
    ]}
    digest = "sha256:" + digest_json(payload)
    pin = NativeAarPin("sha256:" + "1" * 64, "sha256:" + "2" * 64,
                       "sha256:" + "3" * 64, digest)
    report = {"server_name": "aar-mcp", "server_now_unix_ms": 1000,
              "package_version": pin.package_version,
              "tool_surface_version": pin.tool_surface_version,
              "tool_surface_digest": pin.tool_surface_digest,
              "schema_bundle_digest": pin.schema_bundle_digest,
              "operation_skill_digest": pin.operation_skill_digest,
              "tool_names": sorted(NATIVE_TOOLS),
              "ready": {"runtime_generation": 7,
                        "capabilities": {**payload, "digest": digest}}}
    return pin, report


def test_exact_native_read_context_and_restart_binding():
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host-principal", session_id="host-session")
    assert binding.read_context(deadline_unix_ms=2000) == {
        "runtime_generation": 7, "capability_digest": pin.capability_digest,
        "principal_id": "host-principal", "session_id": "host-session",
        "deadline_unix_ms": 2000,
    }
    later = deepcopy(report)
    later["server_now_unix_ms"] = 1500
    assert binding.require_same_runtime(later).server_now_unix_ms == 1500
    later["ready"]["runtime_generation"] = 8
    with pytest.raises(ContractError, match="generation changed"):
        binding.require_same_runtime(later)
    assert binding.runtime_generation == 7


@pytest.mark.parametrize("field,value", [
    ("server_name", "aar-public-runtime"),
    ("package_version", "0.4.0a5"),
    ("tool_surface_version", "aar.public-tools.v2"),
    ("tool_surface_digest", "sha256:" + "f" * 64),
    ("schema_bundle_digest", "sha256:" + "f" * 64),
    ("operation_skill_digest", "sha256:" + "f" * 64),
    ("tool_names", ["aar_capabilities"]),
    ("server_now_unix_ms", True),
])
def test_rejects_incompatible_surface(field, value):
    pin, report = candidate()
    report[field] = value
    with pytest.raises(ContractError):
        pin.bind(report, principal_id="host", session_id="session")


def test_rejects_changed_capability_bytes_with_old_digest():
    pin, report = candidate()
    report["ready"]["capabilities"]["capabilities"][0]["access"] = "read"
    with pytest.raises(ContractError, match="bytes mismatch"):
        pin.bind(report, principal_id="host", session_id="session")


@pytest.mark.parametrize("deadline", [True, 0, 1000, 61001, 2000.0])
def test_deadline_is_bounded_integer(deadline):
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host", session_id="session")
    with pytest.raises(ContractError):
        binding.read_context(deadline_unix_ms=deadline)
