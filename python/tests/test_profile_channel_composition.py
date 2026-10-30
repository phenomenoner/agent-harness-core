"""App-selected enabled profiles use real composition with controlled external ports."""

import base64
import json
import os
import sys
from argparse import Namespace
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest
from memory_snapshot_test_support import snapshot_fixture
from profile_test_support import profile_registry_payload
from skill_test_support import goal
from test_foreground_batch_scenario import LocalTransport
from test_foreground_config_v2 import valid_config
from test_mcp_stdio import SERVER
from test_wsl_python_workspace import Custody

from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.cli import foreground_channels, foreground_children, foreground_files, foreground_trusted
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_memory import inspect_foreground_document
from ahc.cli.foreground_selection import compose_selected_foreground
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import digest_json
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.batch_receipt_repository import BatchReceiptRepository
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.config import load_config
from ahc.infrastructure.foreground_schedule import ForegroundSchedule
from ahc.infrastructure.mcp_receipts import read_mcp_receipts
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("platforms", [("telegram", "discord"), ("telegram", "telegram"),
                                      ("discord", "discord")],
                         ids=["mixed", "same-telegram", "same-discord"])
@pytest.mark.parametrize("recall", [False, True])
@pytest.mark.parametrize("attachment_reply_lost", [False, True])
def test_selected_profiles_compose_workspace_and_both_channel_deliveries(
        tmp_path, monkeypatch, recall, attachment_reply_lost, platforms):
    from ahc.adapters import wsl_python_workspace, wsl_stream_runner

    ipython = pytest.importorskip("IPython")
    packages_root = str(Path(ipython.__file__).resolve().parent.parent)
    kernel = f"import sys;sys.path.insert(0,{packages_root!r});exec({IPYTHON_KERNEL!r})"
    transports = []

    class Runner:
        def __init__(self, executable, distribution, *, persistent):
            self.distribution = distribution
            assert persistent

        def attach(self, unit, custody, lease):
            transport = LocalTransport(kernel)
            transports.append(transport)
            return transport

    names = ("alpha", "beta")
    if recall:
        memory_snapshot, packages = snapshot_fixture(tmp_path, names)
        memory_hash = sha256(memory_snapshot.read_bytes()).hexdigest()
        monkeypatch.setenv("OPENCLAW_MEM_READONLY_DB", "host-sentinel")
    answers, child_results, child_parents, child_runs, child_closes = {}, {}, {}, [], []
    artifact_ids, checkpoints, uploads, incoming_ids = {}, {}, [], {}

    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    routes, events, runtimes, bindings, calls, sent = {}, {}, {}, {}, [], []
    for name, platform in zip(names, platforms, strict=True):
        session = VirtualSessionIdentity.initial(name, name, "route")
        snapshot = repo.create(WorkingSetSnapshot.initial(session, goal()))
        channel = "457" if platforms[0] == platforms[1] and name == "beta" else "456"
        route = ChannelRoute(platform, "123", channel, "789", session)
        file_url = (f"https://cdn.discordapp.com/attachments/{channel}/2/input.txt"
                    if platform == "discord" else None)
        event = ChannelInboundEvent(route, "same-event", name, (
            ChannelFile(platform, channel, "2", "input.txt", len(name), "text/plain", file_url),))
        ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
        raw = valid_config()
        raw.update(runtime_root=str(repo.root), session=session.to_dict(), workspace_id=name)
        raw["backend"]["enabled"] = True
        raw["workspace"]["kind"] = "wsl-ipython"
        raw["batch"] = {"agent_id": name, "capabilities": ["runtime.artifact.manifest"]}
        mcp_digest = digest_json({"server_id": "fixture", "protocol": "2025-11-25",
            "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]})
        mcp = McpStdioProfile("fixture", name, (sys.executable, "-u", "-c", SERVER, "echo"),
            str(tmp_path), tuple(k for k in ("SystemRoot", "WINDIR") if k in os.environ),
            ("echo",), mcp_digest, True)
        raw["mcp"] = {"profiles": [mcp.to_dict()], "credential_environment": {}}
        if recall:
            raw["memory"] = {"enabled": True, "kind": "openclaw-mem-snapshot-pack",
                             "resource_id": f"{name}.durable-memory", "scope": name,
                             "snapshot": str(memory_snapshot), "provider_root": packages,
                             "granted": True, "search_granted": True, "timeout": 15}
        raw["channels"] = [{"route": route.to_dict(), "enabled": True,
                            "token_env": "AHC_TEST_PROFILE_TOKEN"}]
        receiver = {"account_id": "123", "token_env": "AHC_TEST_PROFILE_TOKEN",
                    "enabled": True, "include_files": True}
        if platform == "telegram":
            raw["telegram_polling"] = {**receiver, "exclusive_consumer": True,
                                       "interval_seconds": 1}
        else:
            raw["discord_gateway"] = {**receiver, "intents": 37376, "connection_limit": 2}
        (tmp_path / f"{name}.json").write_text(json.dumps(raw), encoding="utf-8")
        routes[name], events[name] = route, event
    (tmp_path / "profiles.json").write_text(
        json.dumps(profile_registry_payload(names)), encoding="utf-8")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v2"\nstate_dir="state"\n'
                   'profile_registry="profiles.json"\nservices=[]\n[foreground_configs]\n'
                   'alpha="alpha.json"\nbeta="beta.json"\n', encoding="utf-8")

    batch_runs, batch_closes = [], []
    batch_barriers = {name: Barrier(2, timeout=5) for name in names}

    class ChildFactory:
        def __init__(self, owner, ledger, envelope, token, workspace):
            self.ledger, self.envelope, self.token = ledger, envelope, token
            self.name = owner.config.session.agent_id
            assert envelope.parent.session == owner.config.session
            assert workspace.workspace_id == self.name

        def port(self):
            factory = self

            class ChildPort:
                def run(self, envelope, *, cancelled):
                    assert not cancelled()
                    with factory.ledger.backend_admission(envelope, factory.token):
                        if envelope.task_text == "batch task":
                            batch_runs.append((factory.name, envelope.child_id))
                            batch_barriers[factory.name].wait()
                        else:
                            child_runs.append(factory.name)
                    child_parents[factory.name] = envelope.parent
                    return f"child-{factory.name}"

                def close(self):
                    if factory.envelope.task_text == "batch task":
                        batch_closes.append((factory.name, factory.envelope.child_id))
                    else:
                        child_closes.append(factory.name)

            return ChildPort()

    class Factory:
        def __init__(self, store, config):
            self.config = config
            bindings[config.session.agent_id] = config.configuration_digest

        def recover(self, session, workspace_id):
            assert session.agent_id == workspace_id

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, request, tools, *, cancelled):
            assert request.configuration_digest == self.config.configuration_digest
            name = self.config.session.agent_id
            assert request.session == self.config.session
            assert request.workspace_scope == name and request.input_text == name

            class Backend:
                def invoke(self, admitted):
                    other = runtimes["beta" if name == "alpha" else "alpha"]
                    expected = tools.workspace.identity.to_dict()
                    incoming = tools.invoke("workspace.incoming_artifacts", {"expected": expected})
                    assert incoming["disposition"] == "observed"
                    assert len(incoming["result"]) == 1
                    incoming_ids[name] = incoming["result"][0]["artifact_id"]
                    content = tools.invoke("workspace.read_artifact", {
                        "expected": expected, "artifact_id": incoming_ids[name],
                        "offset": 0, "length": 4096})
                    assert base64.b64decode(content["result"]["data"]) == name.encode()
                    if other.session.agent_id in incoming_ids:
                        assert tools.invoke("workspace.read_artifact", {
                            "expected": expected,
                            "artifact_id": incoming_ids[other.session.agent_id],
                            "offset": 0, "length": 4096})["disposition"] == "rejected"
                    assert tools.invoke("workspace.execute", {
                        "expected": other.workspace.identity.to_dict(), "code": "x=0"
                    })["disposition"] == "rejected"
                    assert tools.invoke("workspace.execute", {
                        "expected": tools.workspace.identity.to_dict(),
                        "code": f"%precision 3\nx=42\nreport = 'report-{name}'"
                    })["disposition"] == "observed"
                    calls.append(name)
                    checkpoint = tools.invoke("workspace.checkpoint", {
                        "expected": tools.workspace.identity.to_dict(), "names": ["x", "report"]})
                    assert checkpoint["disposition"] == "observed"
                    checkpoints[name] = checkpoint["result"]["digest"]
                    captured = tools.invoke("workspace.artifact", {
                        "expected": tools.workspace.identity.to_dict(), "name": "report",
                        "media_type": "text/plain", "lineage": []})
                    assert captured["disposition"] == "observed"
                    artifact_ids[name] = captured["result"]["artifact_id"]
                    selected = tools.invoke("workspace.attach", {
                        "expected": tools.workspace.identity.to_dict(),
                        "artifact_id": artifact_ids[name], "filename": "report.txt"})
                    assert selected["disposition"] == "observed"
                    answer = f"answer-{name}"
                    if recall:
                        expected = tools.workspace.identity.to_dict()
                        rejected = tools.invoke("workspace.memory_pack", {
                            "expected": expected, "query": "orchid",
                            "scope": other.session.agent_id})
                        assert rejected["disposition"] == "rejected"
                        result = tools.invoke("workspace.memory_pack", {
                            "expected": expected, "query": "orchid", "max_items": 2,
                            "budget_tokens": 500})
                        assert result["disposition"] == "observed"
                        assert result["content_authority"] == "untrusted-data"
                        bundle = result["result"]["bundle_text"]
                        assert f"orchid {name}" in bundle
                        assert f"orchid {other.session.agent_id}" not in bundle
                        searched = tools.invoke("workspace.memory_search", {
                            "expected": expected, "query": "orchid", "limit": 2})
                        assert searched["disposition"] == "observed"
                        assert searched["result"]["complete_recall"] is False
                        assert [r["summary"] for r in searched["result"]["records"]] == [
                            f"orchid {name}"]
                        answer += " " + bundle
                    child_args = {"expected": tools.workspace.identity.to_dict(),
                                  "child_id": "same-worker", "task_text": "bounded task",
                                  "timeout_ms": 10000}
                    assert tools.invoke("workspace.child", {
                        **child_args, "expected": other.workspace.identity.to_dict()
                    })["disposition"] == "rejected"
                    child = tools.invoke("workspace.child", child_args)
                    assert child["disposition"] == "observed"
                    assert not child["unresolved_effects"]
                    assert child["result"]["result"] == {"output_text": f"child-{name}"}
                    assert tools.invoke("workspace.child", child_args) == child
                    child_results[name] = child["result"]
                    batch_args = {"expected": tools.workspace.identity.to_dict(),
                        "concurrency": 2, "children": [
                            {"child_id": f"batch-{index}", "task_text": "batch task",
                             "timeout_ms": 10000} for index in range(2)]}
                    assert tools.invoke("workspace.child_batch", {
                        **batch_args, "expected": other.workspace.identity.to_dict()
                    })["disposition"] == "rejected"
                    batch = tools.invoke("workspace.child_batch", batch_args)
                    assert batch["disposition"] == "observed"
                    assert len(batch["result"]) == 2
                    assert all(item["result"]["state"] == "terminal" for item in batch["result"])
                    assert tools.invoke("workspace.child_batch", batch_args) == batch
                    answer += " " + child["result"]["result"]["output_text"]
                    answers[name] = answer
                    arguments = {"expected": tools.workspace.identity.to_dict(),
                                 "server_id": "fixture", "tool": "echo", "call_id": "same-call",
                                 "arguments": {"text": name}}
                    mcp_result = tools.invoke("workspace.mcp", arguments)
                    assert mcp_result["disposition"] == "observed"
                    assert mcp_result["result"]["result"]["content"][0]["text"] == name
                    assert tools.invoke("workspace.mcp", arguments)["disposition"] == "rejected"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                        name, "turn", answer,
                        artifacts=(ResultArtifact(artifact_ids[name], "report.txt"),))

                def close(self):
                    pass

            return Backend()

    class Transport:
        def __init__(self, route, token):
            self.route = route

        def send(self, route, payload, operation):
            assert route == self.route
            sent.append((route, payload))
            return "controlled-receipt"

    class AttachmentTransport:
        def __init__(self, route, token):
            self.route = route

        def send(self, upload):
            assert upload.route == self.route
            assert any(route == self.route for route, _ in sent)
            assert f"report-{self.route.session.agent_id}".encode() in upload.body
            assert upload.byte_count == len(f"report-{self.route.session.agent_id}".encode())
            uploads.append(self.route)
            if attachment_reply_lost and self.route.session.agent_id == "beta":
                raise OSError("controlled reply lost after upload")
            return {"message_id": "controlled-attachment"}

    monkeypatch.setenv("AHC_TEST_PROFILE_TOKEN", "fixture")
    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    # Reception is separately covered: events are already staged here.
    monkeypatch.setattr(foreground_trusted, "compose_telegram", lambda *args, **kwargs: None)
    monkeypatch.setattr(foreground_trusted, "compose_discord", lambda *args: None)
    downloads = []

    def downloader(route, token):
        def fetch(reference):
            downloads.append(route.session.agent_id)
            return route.session.agent_id.encode()
        return SimpleNamespace(fetch=fetch)

    monkeypatch.setattr(foreground_files, "BotFileDownload", downloader)
    monkeypatch.setattr(foreground_children, "TrustedCodexChildFactory", ChildFactory)
    monkeypatch.setattr(foreground_channels, "BotTextTransport", Transport)
    monkeypatch.setattr(foreground_channels, "BotAttachmentTransport", AttachmentTransport)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "BackendCustody", Custody)
    config = load_config(app)
    try:
        for name in names:
            runtime = compose_selected_foreground(
                Namespace(agent=name, runtime_config=None), config)
            runtimes[name] = runtime
            runtime.start()
        for name in reversed(names):
            # Control-plane file import precedes dispatch in this same cycle.
            assert runtimes[name].step().result["accepted"]
            assert runtimes[name].step() is None  # Production automatic delivery path.
            assert runtimes[name].step() is None  # Attachment follows text.
            assert runtimes[name].step() is None  # No resend.
        assert calls == ["beta", "alpha"]
        assert bindings["alpha"] != bindings["beta"]
        assert child_runs == child_closes == ["beta", "alpha"]
        assert child_results["alpha"]["child_id"] != child_results["beta"]["child_id"]
        assert sent == [(routes[n], {"output_text": answers[n]}) for n in reversed(names)]
        assert all(type(r.workspace.port) is wsl_python_workspace.WslPythonWorkspace
                   and r.workspace.identity.revision == 1 for r in runtimes.values())
    finally:
        for runtime in reversed(list(runtimes.values())):
            runtime.close()
    if recall:
        assert sha256(memory_snapshot.read_bytes()).hexdigest() == memory_hash
        assert os.environ["OPENCLAW_MEM_READONLY_DB"] == "host-sentinel"
    reopened = RuntimeRepository(repo.root)
    for name in names:
        children = ChildWorkRepository(reopened).read(child_parents[name])
        assert len(children) == 3
        assert children[0]["child_id"] == child_results[name]["child_id"]
        assert children[0]["result"] == {"output_text": f"child-{name}"}
        assert reopened.read_turn(routes[name].session, children[0]["child_id"]) is None
        rows = ChannelOutbox(reopened).read(routes[name])
        assert len(rows) == 1 and rows[0]["state"] == "sent"
        assert rows[0]["operation_id"] == events[name].operation_id
        assert RuntimeInbox(reopened).next_pending(routes[name].session) is None

    # Recover the composed database, including both profiles' terminal child/outbox rows.
    snapshots = tuple(reopened.read_current(name) for name in names)
    schedules = {
        row.session.agent_id: IntervalSchedule(
            f"schedule-{row.session.agent_id}", row.session, row.goal.digest, 100, 100, "scheduled")
        for row in snapshots}
    for schedule in schedules.values():
        ScheduleRepository(reopened).create(schedule)
    source_digest = _digest(reopened.root / WORKING_DB_FILENAME)
    backup, prepared, transformed = (tmp_path / n for n in ("backup", "prepared", "transformed"))
    manifest = backup_runtime_database(reopened, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], snapshots,
                                        source_digest, transformed)
    activate_runtime_restore(reopened, tmp_path / "supervisor.lock", transformed,
                             receipt["sha256"], manifest["sha256"], source_digest)
    restored = RuntimeRepository(repo.root)
    resumed_calls, resumed_memory = [], []

    class RecoveredFactory(Factory):
        def __call__(self, request, tools, *, cancelled):
            assert request.configuration_digest == self.config.configuration_digest
            assert request.session == self.config.session

            class Backend:
                def invoke(self, admitted):
                    assert admitted.input_text == "scheduled"
                    resumed_calls.append(admitted.operation_id)
                    expected = tools.workspace.identity.to_dict()
                    executed = tools.invoke("workspace.execute", {
                        "expected": expected, "code": "x += 1\nprint(x)"})
                    assert executed["disposition"] == "observed"
                    assert executed["result"]["output"].strip() == "43"
                    expected = tools.workspace.identity.to_dict()
                    artifact = tools.invoke("workspace.read_artifact", {
                        "expected": expected,
                        "artifact_id": artifact_ids[admitted.session.agent_id],
                        "offset": 0, "length": 4096})
                    assert artifact["disposition"] == "observed"
                    assert base64.b64decode(artifact["result"]["data"]) == (
                        f"report-{admitted.session.agent_id}".encode())
                    batch_calls = [{"call_id": f"manifest-{i}",
                        "capability_id": "runtime.artifact.manifest", "resource_id": "runtime",
                        "arguments": {"artifact_id": artifact_ids[admitted.session.agent_id]}}
                        for i in range(2)]
                    batched = tools.invoke("workspace.execute", {"expected": expected,
                        "code": f"rows=ahc_batch({batch_calls!r},concurrency=2)\n"
                                "print(sum(v['byte_count'] for v in rows['values']))"})
                    assert batched["disposition"] == "observed"
                    assert batched["result"]["output"].strip() == str(
                        2 * len(f"report-{admitted.session.agent_id}".encode()))
                    expected = tools.workspace.identity.to_dict()
                    other_name = "beta" if admitted.session.agent_id == "alpha" else "alpha"
                    assert tools.invoke("workspace.read_artifact", {
                        "expected": expected, "artifact_id": artifact_ids[other_name],
                        "offset": 0, "length": 4096})["disposition"] == "rejected"
                    if recall:
                        expected = tools.workspace.identity.to_dict()
                        result = tools.invoke("workspace.memory_search", {
                            "expected": expected, "query": "orchid"})
                        resumed_memory.append(result)
                        assert result["disposition"] == "observed"
                        assert [r["summary"] for r in result["result"]["records"]] == [
                            f"orchid {admitted.session.agent_id}"]
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "recovered", "turn", "scheduled answer")

                def close(self):
                    pass

            return Backend()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", RecoveredFactory)
    for name in names:
        historical = ChildWorkRepository(restored).read(child_parents[name])
        assert len(historical) == 3 and historical[0]["result"] == {
            "output_text": f"child-{name}"}
        assert all(row["state"] == "terminal" and row["result"] == {
            "output_text": f"child-{name}"} for row in historical)
        historical_outbox = ChannelOutbox(restored).read(routes[name])
        attachment_rows = ChannelAttachmentOutbox(restored).read(routes[name])
        expected_attachment = "unknown" if attachment_reply_lost and name == "beta" else "sent"
        assert len(attachment_rows) == 1 and attachment_rows[0]["state"] == expected_attachment
        mcp_rows = read_mcp_receipts(restored, routes[name].session, events[name].operation_id)
        assert len(mcp_rows) == 1 and mcp_rows[0]["status"] == "observed"
        assert len(historical_outbox) == 1 and historical_outbox[0]["state"] == "sent"
        original = tmp_path / f"{name}.json"
        original_bytes = original.read_bytes()
        parsed = parse_foreground_config_v2(json.loads(original_bytes))
        destination = tmp_path / f"recovered-{name}"
        report = prepare_recovered_config(restored, original, parsed.configuration_digest,
                                          destination, checkpoint_digest=checkpoints[name])
        recovered = parse_foreground_config_v2(json.loads(
            (destination / "foreground.json").read_text(encoding="utf-8")))
        assert recovered.session != routes[name].session
        assert recovered.session.agent_id == name
        assert not report["started"] and not recovered.backend["enabled"]
        channel = recovered.to_dict()["channels"][0]
        assert channel["route"]["session"] == recovered.session.to_dict()
        assert not channel["enabled"]
        assert all(not p["enabled"] for p in recovered.to_dict()["mcp"]["profiles"])
        def forbidden_process(*args, **kwargs):
            pytest.fail("configuration doctor must not start a process")

        with monkeypatch.context() as doctor_patch:
            doctor_patch.setattr("subprocess.Popen", forbidden_process)
            doctor = inspect_foreground_document(
                recovered.to_dict(), profile_registry=config.profile_registry)
        assert doctor["configuration"] == "valid"
        assert doctor["observation_scope"] == "configuration-only"
        assert doctor["backend"]["enabled"] is False
        assert RuntimeInbox(restored).next_pending(recovered.session) is None
        scheduler = ForegroundSchedule(restored, clock_ms=lambda: 350)
        current = restored.read_current(name)
        scheduler(current)
        assert scheduler.status == "idle"  # Old-generation schedules are not inherited.
        with pytest.raises(ContractError):
            ScheduleTick(restored).tick(schedules[name], 350)
        assert RuntimeInbox(restored).next_pending(recovered.session) is None
        # Explicitly recreate a schedule for this successor; other profiles stay untouched.
        renewed = replace(schedules[name], schedule_id=f"renewed-{name}",
                          session=recovered.session)
        ScheduleRepository(restored).create(renewed)
        scheduler(current)
        assert scheduler.status == "submitted"
        pending = RuntimeInbox(restored).next_pending(recovered.session)
        assert pending is not None
        assert pending["session"] == recovered.session.to_dict()
        assert original.read_bytes() == original_bytes
        if recall:
            assert recovered.to_dict()["memory"] == parsed.to_dict()["memory"]
        enabled = recovered.to_dict()
        enabled["backend"]["enabled"] = True  # Controlled factory only, in this test.
        for attempt in range(2):
            resumed = compose_foreground_document(enabled, profile_registry=config.profile_registry)
            resumed.schedule = ForegroundSchedule(resumed.store, clock_ms=lambda: 100)
            try:
                resumed.start()
                assert "workspace.mcp" not in resumed.tools.capabilities()
                outcome = resumed.step()
                if attempt == 0:
                    assert outcome.result["accepted"], (outcome.result, resumed_memory)
                else:
                    assert outcome is None
                assert resumed.step() is None
            finally:
                resumed.close()
            assert resumed.state == "closed"
        fresh = RuntimeRepository(repo.root)
        assert fresh.read_turn(recovered.session, pending["operation_id"]).result["accepted"]
        assert RuntimeInbox(fresh).next_pending(recovered.session) is None
        batch_rows = BatchReceiptRepository(fresh).read(recovered.session, pending["operation_id"])
        assert [row["call_id"] for row in batch_rows] == ["manifest-0", "manifest-1"]
        assert all(row["state"] == "success" and row["reason"] is None for row in batch_rows)
        with pytest.raises(ContractError, match="owner mismatch"):
            BatchReceiptRepository(fresh).read(routes[name].session, pending["operation_id"])
    assert len(resumed_calls) == len(set(resumed_calls)) == 2
    assert downloads == list(reversed(names))
    assert len(transports) == 6
    assert all(t.process.poll() == 0 and not t.reader.is_alive() for t in transports)
    assert uploads == [routes[name] for name in reversed(names)]
    assert child_runs == child_closes == ["beta", "alpha"]
    assert len(batch_runs) == 4 and sorted(batch_runs) == sorted(batch_closes)
    assert len({child_id for _, child_id in batch_runs}) == 4
    assert sent == [(routes[n], {"output_text": answers[n]}) for n in reversed(names)]
    if recall:
        assert sha256(memory_snapshot.read_bytes()).hexdigest() == memory_hash
