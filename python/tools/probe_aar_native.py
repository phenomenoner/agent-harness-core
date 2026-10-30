"""No-provider AAR SDK/MCP composition probe; requires optional AAR dependencies."""

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from ahc.adapters.aar_native_binding import NativeAarPin
from ahc.adapters.aar_native_operations import NativeOperationReceipt
from ahc.adapters.aar_native_route import verify_native_model_route
from ahc.domain.sessions import ContractError


class NoSendBroker:
    def __init__(self):
        self.requests = 0
        self.closed = 0

    def request(self, *args, **kwargs):
        self.requests += 1
        raise AssertionError("no-provider probe attempted model execution")

    def reconcile(self, *args, **kwargs):
        raise AssertionError("no-provider probe attempted model reconciliation")

    def close(self):
        self.closed += 1


async def probe(root):
    import aar.mcp.server as upstream
    from aar.broker_models import ModelRouteCatalog, ModelRouteProfile
    from aar.canonical import canonical_sha256
    from aar.mcp.models import McpMutationContext, McpReadContext, McpRlmMutationContext
    from aar.rlm_models import RlmJobSpec
    from aar.runtime.model_broker import StaticModelBrokerRegistry
    from mcp import Client

    profile = ModelRouteProfile(profile_id="ahc-no-send", provider_driver="ahc-fixture",
        provider="fixture", model="no-send", reasoning_effort="high", max_output_tokens=128)
    catalog = ModelRouteCatalog.issue((profile,))
    broker = NoSendBroker()
    registry = StaticModelBrokerRegistry(catalog, brokers={profile.profile_id: broker})
    app = upstream.build_server(root / "native.sqlite3", now_ms=lambda: 1000000,
        programmable_backend="plain", enable_durable_dispatch=False,
        model_broker_registry=registry, default_model_route_profile=profile.profile_id)
    try:
        manifest = await upstream.tool_surface_manifest(app.server)
        # Fixture expectations come from the constructed host, independently of
        # the MCP response; these are not production admission pins.
        pin = NativeAarPin(manifest["tool_surface_digest"], upstream.SCHEMA_BUNDLE_DIGEST,
                           upstream._skill_digest(), app.host.ready().capabilities.digest)
        async with Client(app.server) as client:
            reply = await client.call_tool("aar_capabilities")
            report = reply.structured_content
            assert not reply.is_error
            binding = pin.bind(report, principal_id="ahc-probe", session_id="ahc-probe-session")
            route = verify_native_model_route(binding, report, profile_id=profile.profile_id,
                catalog_digest=catalog.catalog_digest, profile_digest=profile.profile_digest)
            assert route == profile.model_dump(mode="json")
            context = binding.read_context(deadline_unix_ms=1001000)
            assert McpReadContext.model_validate(context).model_dump(mode="json") == context
            # Real server validates generation/principal/session/deadline even
            # though the operation itself intentionally does not exist.
            missing = await client.call_tool("aar_operation_status", {
                "context": context, "operation_id": "never-submitted"})
            assert not missing.is_error
            assert missing.structured_content["state"] is None
            assert missing.structured_content["failure"] is not None
            mutation = {**context, "request_id": "probe-cancel", "idempotency_key": "probe-key",
                        "grant_id": "probe-grant", "budget_wall_time_ms": 1001}
            McpMutationContext.model_validate(mutation)
            for change in ({"default_profile_id": "wrong"}, {"configured": False}):
                altered = {**report, "model_broker": {**report["model_broker"], **change}}
                try:
                    verify_native_model_route(binding, altered, profile_id=profile.profile_id,
                        catalog_digest=catalog.catalog_digest,
                        profile_digest=profile.profile_digest)
                except ContractError:
                    pass
                else:
                    raise AssertionError("mismatched native route accepted")
            grants = {item["capability"]: item["grant_id"] for item in report["reference_grants"]}
            rlm_context = McpRlmMutationContext(**context, request_id="probe-rlm",
                idempotency_key="probe-rlm-key",
                grant_ids=sorted(grants[key] for key in ("rlm.execute", "model.request")),
                budget_wall_time_ms=1000, budget_model_requests=1, budget_input_tokens=1024,
                budget_output_tokens=128, budget_child_operations=0, budget_artifact_bytes=0)
            spec = RlmJobSpec(query="No-provider admission probe", strategy="baseline", max_steps=1)
            envelope = upstream._rlm_envelope(app.host, rlm_context, canonical_sha256(spec))
            # Direct SDK admission cannot dispatch in this configuration.
            # MCP start_only is NOT used: upstream still notifies its dispatcher.
            accepted = app.host.submit_rlm(envelope, spec)
            operation_id = accepted.operation.value
            assert accepted.state.value == "accepted"
            status = await client.call_tool("aar_operation_status", {
                "context": context, "operation_id": operation_id})
            first = NativeOperationReceipt.parse(status.structured_content, operation_id)
            assert first.state == "accepted" and not first.terminal
            cancelled = await client.call_tool("aar_operation_cancel", {
                "context": {**mutation, "grant_id": grants["operation.cancel"]},
                "operation_id": operation_id})
            terminal = NativeOperationReceipt.parse(cancelled.structured_content, operation_id)
            assert terminal.state == "cancelled" and terminal.terminal
            status = await client.call_tool("aar_operation_status", {
                "context": context, "operation_id": operation_id})
            assert NativeOperationReceipt.parse(status.structured_content, operation_id) == terminal
    finally:
        app.close()
    assert broker.requests == 0 and broker.closed == 1
    return {"status": "PASS", "scope": "in-process native MCP, no provider",
            "package_version": report["package_version"],
            "tool_surface_version": report["tool_surface_version"],
            "runtime_generation": binding.runtime_generation,
            "tool_surface_digest": pin.tool_surface_digest,
            "capability_digest": pin.capability_digest,
            "catalog_digest": catalog.catalog_digest,
            "schema_models_validated": ["McpReadContext", "McpMutationContext",
                                        "McpRlmMutationContext"],
            "sdk_admission_mcp_lifecycle": [first.state, terminal.state, "terminal-readback"],
            "unknown_operation": "structured failure, no terminal receipt",
            "model_requests": broker.requests, "broker_close_calls": broker.closed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aar-no-send-", dir=args.scratch) as folder:
        report = asyncio.run(probe(Path(folder)))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
