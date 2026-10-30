"""Real native RLM dispatcher with a declared local model fixture, never a provider."""

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from ahc.adapters.aar_native_artifacts import resolve_native_artifact
from ahc.adapters.aar_native_binding import NativeAarPin
from ahc.adapters.aar_native_rlm import NativeAarRlm


async def probe(root):
    import aar.mcp.server as upstream
    from aar.broker_models import ModelRouteCatalog, ModelRouteProfile
    from aar.runtime.model_broker import ReferenceModelBroker, StaticModelBrokerRegistry
    from mcp import Client

    class FixtureBroker(ReferenceModelBroker):
        def __init__(self):
            super().__init__()
            self.calls, self.closes = 0, 0

        def request(self, *args, **kwargs):
            self.calls += 1
            return super().request(*args, **kwargs)

        def close(self):
            self.closes += 1
            super().close()

    profile = ModelRouteProfile(profile_id="ahc-fixture", provider_driver="reference",
        provider="reference", model="fixture-model", max_output_tokens=256)
    catalog = ModelRouteCatalog.issue((profile,))
    broker = FixtureBroker()
    app = upstream.build_server(root / "rlm.sqlite3", now_ms=lambda: 1000000,
        programmable_backend="plain", enable_durable_dispatch=True,
        model_broker_registry=StaticModelBrokerRegistry(catalog,
            brokers={profile.profile_id: broker}), default_model_route_profile=profile.profile_id)
    try:
        manifest = await upstream.tool_surface_manifest(app.server)
        pin = NativeAarPin(manifest["tool_surface_digest"], upstream.SCHEMA_BUNDLE_DIGEST,
                           upstream._skill_digest(), app.host.ready().capabilities.digest)
        async with Client(app.server) as client:
            report = (await client.call_tool("aar_capabilities")).structured_content
            binding = pin.bind(report, principal_id="ahc-probe", session_id="ahc-session")
            loop = asyncio.get_running_loop()

            class Bridge:
                # Probe-only bridge to the real in-process MCP client. Production
                # composition must use its admitted, durable-reservation client.
                session = "probe-only"

                def invoke(self, session, call_id, tool, arguments, **kwargs):
                    future = asyncio.run_coroutine_threadsafe(
                        client.call_tool(tool, arguments), loop)
                    reply = future.result(timeout=15)
                    return {"status": "tool-error" if reply.is_error else "observed",
                            "result": {"structuredContent": reply.structured_content}}

            grants = {item["capability"]: item["grant_id"] for item in report["reference_grants"]}
            approved = tuple(grants[key] for key in ("rlm.execute", "model.request"))
            adapter = NativeAarRlm(Bridge(), binding, profile_id=profile.profile_id,
                catalog_digest=catalog.catalog_digest, profile_digest=profile.profile_digest,
                approved_grants=approved)
            context = {**binding.read_context(deadline_unix_ms=1010000),
                "request_id": "rlm-probe", "idempotency_key": "rlm-probe-key",
                "grant_ids": sorted(approved), "budget_wall_time_ms": 10000,
                "budget_model_requests": 1, "budget_input_tokens": 4096,
                "budget_output_tokens": 1024, "budget_child_operations": 0,
                "budget_artifact_bytes": 16384}
            result = await asyncio.to_thread(adapter.submit, "submit", report, context,
                query="Return a local fixture response.", strategy="baseline", max_steps=1,
                deadline=loop.time() + 15)
            assert result["status"] == "observed", result
            receipt, operation = result["receipt"], result["operation"]
            assert receipt.state == "succeeded" and receipt.terminal, receipt
            artifacts = json.loads(receipt.payload_json)["artifacts"]
            assert len(artifacts) == 1
            resolved = await asyncio.to_thread(resolve_native_artifact, operation, "artifact",
                artifacts[0], max_bytes=16384, deadline_unix_ms=1010000, deadline=loop.time() + 15)
            assert resolved["status"] == "observed", resolved
            content = json.loads(resolved["artifact"].content)
            assert content["operation"]["value"] == operation.operation_id
            readback = await asyncio.to_thread(operation.status, "status",
                deadline_unix_ms=1010000, deadline=loop.time() + 15)
            assert readback["status"] == "observed"
            assert readback["receipt"].record_payload() == receipt.record_payload()
    finally:
        app.close()
    assert broker.calls == 1 and broker.closes == 1
    return {"status": "PASS", "model_evidence": "local ReferenceModelBroker fixture only",
            "native_state": receipt.state, "terminal_readback": "matched",
            "verified_artifact_bytes": len(resolved["artifact"].content),
            "fixture_model_requests": broker.calls, "broker_close_calls": broker.closes,
            "provider_requests": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aar-rlm-fixture-", dir=args.scratch) as folder:
        report = asyncio.run(asyncio.wait_for(probe(Path(folder)), timeout=30))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
