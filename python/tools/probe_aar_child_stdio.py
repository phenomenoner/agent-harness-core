"""Actual stdio native child pipeline with a disposable reference-model server."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path


def serve(root, recovery=False):
    import aar.mcp.server as upstream
    from aar.broker_models import ModelRouteCatalog, ModelRouteProfile
    from aar.runtime.model_broker import ReferenceModelBroker, StaticModelBrokerRegistry

    class Fixture(ReferenceModelBroker):
        def request(self, *args, **kwargs):
            count = root / "fixture-calls.txt"
            count.write_text(str(int(count.read_text()) + 1))
            return super().request(*args, **kwargs)

    profile = ModelRouteProfile(
        profile_id="ahc-fixture",
        provider_driver="reference",
        provider="reference",
        model="fixture-model",
        max_output_tokens=256,
    )
    catalog = ModelRouteCatalog.issue((profile,))
    (root / "fixture-calls.txt").write_text("0")
    app = upstream.build_server(
        root / "aar.sqlite3",
        now_ms=lambda: 1000000,
        programmable_backend="plain",
        enable_durable_dispatch=not recovery,
        model_broker_registry=StaticModelBrokerRegistry(
            catalog, brokers={profile.profile_id: Fixture()}
        ),
        default_model_route_profile=profile.profile_id,
    )
    try:
        if recovery:
            from aar.canonical import canonical_sha256
            from aar.mcp.models import McpRlmMutationContext
            from aar.rlm_models import RlmJobSpec

            ready = app.host.ready()
            seed = McpRlmMutationContext(
                runtime_generation=ready.runtime_generation,
                capability_digest=ready.capabilities.digest,
                principal_id="ahc-probe", session_id="ahc-native-session",
                deadline_unix_ms=1010000, request_id="seed-request",
                idempotency_key="seed-request-key",
                grant_ids=sorted(upstream.REFERENCE_GRANT_IDS[key]
                                 for key in ("rlm.execute", "model.request")),
                budget_wall_time_ms=10000, budget_model_requests=1,
                budget_input_tokens=4096, budget_output_tokens=1024,
                budget_child_operations=0, budget_artifact_bytes=16384,
            )
            spec = RlmJobSpec(query="Disposable recovery fixture", strategy="baseline", max_steps=1)
            accepted = app.host.submit_rlm(upstream._rlm_envelope(
                app.host, seed, canonical_sha256(spec)), spec)
            (root / "seed.json").write_text(json.dumps({
                "operation_id": accepted.operation.value,
                "submission_digest": canonical_sha256(spec),
            }))
        manifest = asyncio.run(upstream.tool_surface_manifest(app.server))
        (root / "expectations.json").write_text(
            json.dumps(
                {
                    "tool_surface_digest": manifest["tool_surface_digest"],
                    "schema_bundle_digest": upstream.SCHEMA_BUNDLE_DIGEST,
                    "operation_skill_digest": upstream._skill_digest(),
                    "capability_digest": app.host.ready().capabilities.digest,
                    "profile_id": profile.profile_id,
                    "catalog_digest": catalog.catalog_digest,
                    "profile_digest": profile.profile_digest,
                    "tool_manifest_bytes": len(json.dumps(manifest).encode()),
                }
            )
        )
        app.server.run(transport="stdio")
    finally:
        app.close()


def probe(root, server_python, aar_source, recovery=False, foreground=False, bad_pin=False,
          tool=False, artifacts=False):
    from ahc.adapters.aar_child_backend import NativeAarChildBackend
    from ahc.adapters.aar_native_binding import NativeAarPin
    from ahc.adapters.aar_native_rlm import NativeAarRlm
    from ahc.adapters.mcp_stdio import McpStdioTransport
    from ahc.application.child_dispatch import ChildDispatchService
    from ahc.application.mcp_client import McpClient
    from ahc.domain.agent_backend import AgentTurnRequest
    from ahc.domain.child_work import ChildWorkEnvelope
    from ahc.domain.judgment_models import GOAL_SCHEMA, GoalContract
    from ahc.domain.sessions import VirtualSessionIdentity
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
    from ahc.infrastructure.mcp_receipts import McpReceipts
    from ahc.infrastructure.mcp_schema import McpSchemaValidator
    from ahc.infrastructure.native_child_repository import NativeChildRepository
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    goal = GoalContract(
        GOAL_SCHEMA,
        "fixture-goal",
        1,
        None,
        "Native child probe",
        "Verify fixture mailbox",
        ("One native result",),
        ("local probe",),
        ("no provider",),
        ("fixture only",),
        (),
        "acknowledged",
    )
    store = RuntimeRepository.initialize(root / "ahc")
    snapshot = WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("probe", "agent", "route"), goal
    )
    store.create(snapshot)
    parent = AgentTurnRequest(
        snapshot.session,
        "parent",
        goal.digest,
        "Probe",
        (),
        "a" * 64,
        "b" * 64,
        "workspace",
        time.monotonic() + 25,
        capabilities=("workspace.child",) if tool else (),
    )
    store.begin(snapshot, parent)
    producer = (store.reserve_workspace_generation(store.read_current("probe"), "workspace")
                if artifacts else None)
    children = ChildDispatchRepository(store, parent, clock=lambda: 1000000)
    native = NativeChildRepository(children)
    envelope = ChildWorkEnvelope(
        "native-child", children.parent, "Return a fixture result.", "workspace", 1010000, "minion"
    )
    env = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
    env.update(
        PYTHONPATH=os.pathsep.join(
            (str(Path(__file__).resolve().parents[1] / "src"), str(aar_source.resolve()))
        ),
        PYTHONDONTWRITEBYTECODE="1",
    )
    validator = McpSchemaValidator()
    transport = McpStdioTransport.launch(
        (
            str(server_python.resolve()),
            str(Path(__file__).resolve()),
            "--serve",
            "--scratch",
            str(root),
            *(("--recovery",) if recovery else ()),
        ),
        cwd=str(root),
        env=env,
    )
    client = McpClient(
        parent.session,
        "native-aar",
        transport,
        authorize=lambda *a: True,
        validate=validator,
        receipts=McpReceipts(store, parent),
    )
    try:
        client.discover(deadline=time.monotonic() + 15)
        observed = client.invoke(
            parent.session, "capabilities", "aar_capabilities", {}, deadline=time.monotonic() + 5
        )
        assert observed["status"] == "observed", observed
        report = observed["result"]["structuredContent"]
        expected = json.loads((root / "expectations.json").read_text())
        pin = NativeAarPin(
            **{
                key: expected[key]
                for key in (
                    "tool_surface_digest",
                    "schema_bundle_digest",
                    "operation_skill_digest",
                    "capability_digest",
                )
            }
        )
        binding = pin.bind(report, principal_id="ahc-probe", session_id="ahc-native-session")
        grants = {item["capability"]: item["grant_id"] for item in report["reference_grants"]}
        approved = tuple(grants[key] for key in ("rlm.execute", "model.request"))
        rlm = NativeAarRlm(
            client,
            binding,
            approved_grants=approved,
            **{key: expected[key] for key in ("profile_id", "catalog_digest", "profile_digest")},
        )
        context = {
            **binding.read_context(deadline_unix_ms=1010000),
            "request_id": "child-submit",
            "idempotency_key": "child-submit-key",
            "grant_ids": sorted(approved),
            "budget_wall_time_ms": 10000,
            "budget_model_requests": 1,
            "budget_input_tokens": 4096,
            "budget_output_tokens": 1024,
            "budget_child_operations": 0,
            "budget_artifact_bytes": 16384,
        }
        if recovery:
            from probe_aar_recovery_composition import recover

            return recover(root, store, parent, children, native, envelope,
                           client, binding, expected, grants)
        port = NativeAarChildBackend(
            envelope,
            native,
            rlm,
            report,
            context,
            token="claim",
            deadline=min(parent.deadline, time.monotonic() + 10),
        )
        if foreground:
            from dataclasses import asdict

            from ahc.adapters.aar_native_binding import NATIVE_TOOLS
            from ahc.cli.foreground_native import parse_native
            from ahc.cli.native_child_factory import ForegroundNativeChild
            from ahc.domain.mcp_profiles import McpStdioProfile

            profile = McpStdioProfile("native-aar", parent.session.agent_id,
                (str(server_python.resolve()), str(Path(__file__).resolve()),
                 "--serve", "--scratch", str(root)), str(root), tuple(sorted(env)),
                tuple(sorted(NATIVE_TOOLS)), client.capability_digest, True)
            value = {"profile": profile.to_dict(), "pin": asdict(pin),
                     "route": {key: expected[key] for key in (
                         "profile_id", "catalog_digest", "profile_digest")},
                     "principal_id": binding.principal_id, "session_id": binding.session_id,
                     "grants": {"execute": sorted(approved), "cancel": [], "reconcile": []},
                     "budget": {key.removeprefix("budget_"): amount
                                for key, amount in context.items() if key.startswith("budget_")}}
            if bad_pin:
                value["pin"]["operation_skill_digest"] = "sha256:" + "f" * 64
            if artifacts:
                value["retain_artifacts"] = True
                value["grants"]["reconcile"] = [grants["operation.reconcile"]]
            value = parse_native({"native_minion": value}, parent.session)["native_minion"]
            client.close()
            assert transport.wire.owned.process.poll() is not None
            # A new owned server launches through the production profile connector.
            port = ForegroundNativeChild(value, children, envelope, "claim", environment=env,
                                         workspace=producer)
        service = ChildDispatchService(children, lambda _: port)
        if tool:
            from probe_aar_foreground_tool import dispatch_tool

            envelope, tool_result, _ = dispatch_tool(value, store, parent, env)
            result = children.read(children.parent)
            result = next(row for row in result if row["child_id"] == envelope.child_id)
            assert result["result"] == tool_result["result"]
        else:
            result = service.dispatch(envelope, "claim")
        assert result["state"] == ("unknown" if bad_pin else "terminal"), result
        retained = native.read(children.parent, envelope.child_id)
        if bad_pin:
            assert retained is None
        else:
            assert retained["receipt"].terminal
        if foreground and not tool:
            assert port.closed and port.client.state == "closed"
            assert port.client.transport.wire.owned.process.poll() is not None
        reopened = ChildDispatchRepository(
            RuntimeRepository(store.root), parent, clock=lambda: 1000000
        )

        def no_replay(_):
            raise AssertionError("native child replayed")

        assert ChildDispatchService(reopened, no_replay).dispatch(envelope, "new-claim") == result
        assert NativeChildRepository(reopened).read(children.parent, envelope.child_id) == retained
        assert int((root / "fixture-calls.txt").read_text()) == (0 if bad_pin else 1)
        return {
            "status": "PASS",
            "scope": ("foreground tool + factory actual stdio" if tool else
                      "foreground factory actual stdio" if foreground
                      else "actual stdio native child with fixture model"),
            "child_state": result["state"],
            "native_state": None if bad_pin else retained["receipt"].state,
            "pin_mismatch_rejected": bad_pin,
            "artifacts_retained": artifacts,
            "reopen_no_replay": True,
            "fixture_calls": 0 if bad_pin else 1,
            "provider_requests": 0,
            "mcp_receipts": len(McpReceipts(store, parent).read()),
        }
    finally:
        client.close()
        assert transport.wire.owned.process.poll() is not None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--foreground", action="store_true")
    parser.add_argument("--bad-pin", action="store_true")
    parser.add_argument("--tool", action="store_true")
    parser.add_argument("--artifacts", action="store_true")
    parser.add_argument("--server-python", type=Path, default=Path(sys.executable))
    parser.add_argument("--aar-source", type=Path)
    args = parser.parse_args()
    if (args.recovery and args.foreground) or (args.bad_pin and not args.foreground):
        parser.error("recovery and foreground are separate; bad-pin requires foreground")
    if args.tool and (not args.foreground or args.bad_pin):
        parser.error("tool requires foreground with valid pins")
    if args.artifacts and not args.tool:
        parser.error("artifacts requires foreground tool composition")
    args.scratch.mkdir(parents=True, exist_ok=True)
    if args.serve:
        serve(args.scratch, args.recovery)
    else:
        if args.aar_source is None:
            parser.error("--aar-source is required for the client probe")
        with tempfile.TemporaryDirectory(prefix="aar-child-", dir=args.scratch) as directory:
            result = probe(Path(directory).resolve(), args.server_python, args.aar_source,
                           args.recovery, args.foreground, args.bad_pin, args.tool, args.artifacts)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
