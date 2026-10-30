"""Transfer framing/admission tests; native filesystem tests are separate."""

import ast
import base64
import json
from dataclasses import replace
from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest
from skill_test_support import session
from test_channel_tool_approval import Port, admitted
from test_skill_graph_convergence import _activate_with_report
from test_skill_markdown_resources import modern, resource

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.channel_tool_approval import ChannelToolGate
from ahc.application.workspace_skill_materialize import SkillTransferIncomplete, materialize_skill
from ahc.application.workspace_skills import WorkspaceSkillTools
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.skill_values import digest_bytes, digest_json
from ahc.domain.workspace import WorkspaceIdentity


class RecordingWorkspace:
    """Emulate only the data receiver; never execute helper or skill source."""

    def __init__(self, revision, values, *, fail=None):
        self.identity = WorkspaceIdentity(session(1), "work")
        self.port = SimpleNamespace(skill_materialization_root="/work")
        self.closed, self.codes, self.values = False, [], values
        self.revision, self.fail, self.received = revision, fail, {}

    def execute(self, expected, code):
        assert expected == self.identity and len(code.encode()) <= 16384
        self.identity = replace(expected, revision=expected.revision + 1)
        self.codes.append(code)
        if self.fail == len(self.codes):
            self.closed = True  # RuntimeWorkspace closes on transport failure.
            raise OSError("lost transfer reply")
        output = ""
        tree = ast.parse(code)
        first = tree.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Call):
            call = first.value
            if isinstance(call.func, ast.Attribute):
                args = [ast.literal_eval(node) for node in call.args]
                action, *args = args
                if action == "begin_file":
                    self.path, self.size, self.checksum = args
                    self.received[self.path] = bytearray()
                elif action == "chunk":
                    offset, encoded = args
                    assert offset == len(self.received[self.path])
                    self.received[self.path].extend(base64.b64decode(encoded, validate=True))
                elif action == "end_file":
                    assert len(self.received[self.path]) == self.size
                    assert digest_bytes(bytes(self.received[self.path])) == self.checksum
            elif isinstance(call.func, ast.Name) and call.func.id == "print":
                assert {k: bytes(v) for k, v in self.received.items()} == self.values
                manifest = [{"path": k, "sha256": digest_bytes(v), "size": len(v)}
                            for k, v in sorted(self.values.items())]
                output = json.dumps({
                    "root": "/work/.ahc-skill-" + self.revision[:12] + "-" + "a" * 24,
                    "revision_digest": self.revision, "manifest_digest": digest_json(manifest),
                    "file_count": len(manifest),
                    "byte_count": sum(len(v) for v in self.values.values())})
        return {"ok": True, "output": output, "output_truncated": False,
                "identity": self.identity.to_dict()}

    def close(self):
        self.closed = True


def prepared(repository, **kwargs):
    assets = {"src/tool.py": b"raise RuntimeError('not during materialization')\n" * 700,
              "assets/binary.bin": bytes(range(256)) * 60, "assets/empty.txt": b""}
    item = modern(agent_allowlist=(session(1).agent_id,),
                  assets=tuple(resource(k, v) for k, v in assets.items()))
    repository.stage_assets(item.assets, assets)
    _activate_with_report(repository, item, "activate")
    values = {"SKILL.md": item.body.encode(), **assets}
    workspace = RecordingWorkspace(item.revision_digest, values, **kwargs)
    return WorkspaceTools(workspace, skills=WorkspaceSkillTools(repository)), item


def invoke(tools, item, **overrides):
    return tools.invoke("workspace.skills_materialize", {
        "expected": tools.workspace.identity.to_dict(), "revision_digest": item.revision_digest,
        **overrides})


def test_full_bundle_transfer_is_bounded_exact_data_and_returns_latest_identity(repository):
    tools, item = prepared(repository)
    result = invoke(tools, item)
    assert result["disposition"] == "observed"
    assert result["result"]["scripts_executed"] is False
    assert result["result"]["copy_is_mutable"] is True
    assert result["result"]["identity"] == tools.workspace.identity.to_dict()
    assert tools.workspace.identity.revision == len(tools.workspace.codes) > 8
    assert len(json.dumps(result).encode()) < 32768
    assert repository.get_active(item.skill_name) == item


@pytest.mark.parametrize("change", ["foreign", "stale", "destination", "missing-tools"])
def test_materialization_cannot_expand_source_or_output_authority(repository, change):
    tools, item = prepared(repository)
    overrides = {}
    if change == "foreign":
        tools.workspace.identity = replace(tools.workspace.identity,
            session=replace(tools.workspace.identity.session, agent_id="foreign"))
    elif change == "stale":
        overrides["expected"] = replace(tools.workspace.identity, revision=99).to_dict()
    elif change == "destination":
        overrides["root"] = "/outside"
    else:
        tools.workspace.port.skill_materialization_root = None
    assert invoke(tools, item, **overrides)["disposition"] in ("rejected", "unsupported")
    assert tools.workspace.codes == []


def test_unknown_transfer_retains_custody_without_cleanup_or_replay(repository):
    tools, item = prepared(repository, fail=4)
    assert invoke(tools, item)["disposition"] == "unknown"
    assert len(tools.workspace.codes) == 4
    assert tools.workspace.closed
    assert invoke(tools, item)["disposition"] == "unsupported"
    assert len(tools.workspace.codes) == 4


def test_transfer_budget_stops_new_writes_and_releases_known_helper(repository):
    tools, item = prepared(repository)
    times = iter([0, 1, 31])
    with pytest.raises(SkillTransferIncomplete):
        materialize_skill(tools.workspace, repository, item, clock=lambda: next(times))
    assert len(tools.workspace.codes) == 2  # Setup then one descriptor-only cleanup.
    assert "hasattr" in tools.workspace.codes[-1]
    assert not tools.workspace.received


def test_materialization_wire_and_channel_effect_permission_are_explicit(repository):
    tools, _ = prepared(repository)
    request = SimpleNamespace(capabilities=tools.capabilities())
    declared = next(row for row in CodexWorkspaceTools.describe(request, tools)[0]["tools"]
                    if row["name"] == "skills_materialize")
    assert set(declared["inputSchema"]["properties"]) == {"expected", "revision_digest"}
    port = Port()
    gate = ChannelToolGate(admitted(capabilities=("workspace.skills_materialize",)),
                           port, clock=lambda: 10)
    gate.authorize("workspace.skills_materialize", {}, "thread", "turn", "call")
    assert len(port.requests) == 1  # Not misclassified as a read-only catalog query.


def test_resource_locator_survives_checkpoint_without_reusing_execution_identity(
    repository, tmp_path
):
    from skill_test_support import goal

    from ahc.application.runtime_workspace import RuntimeWorkspace
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.domain.workspace import WorkspaceCheckpoint
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    tools, item = prepared(repository)
    port = tools.workspace
    port.skill_materialization_root = port.port.skill_materialization_root
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = store.create(WorkingSetSnapshot.initial(session(1), goal()))
    restored_values = []

    def factory(identity, checkpoint):
        port.identity, port.closed = identity, False
        if checkpoint is not None:
            restored_values.append(json.loads(checkpoint.values_json))
        return port

    workspace = RuntimeWorkspace.open(store, snapshot, "work", factory)
    tools.workspace = workspace
    try:
        copied = invoke(tools, item)["result"]
        reference = copied["resource_context"]
        values = {"phase_a_resource": reference, "cost_rate": "0.002"}
        port.checkpoint = lambda expected, names: WorkspaceCheckpoint(
            expected, json.dumps({name: values[name] for name in names}))
        checkpoint = workspace.checkpoint(workspace.identity, tuple(values))
        previous_identity = workspace.identity
    finally:
        workspace.close()

    resumed = RuntimeWorkspace.open(store, snapshot, "work", factory, checkpoint.digest)
    try:
        reference = restored_values[0]["phase_a_resource"]
        assert restored_values[0]["cost_rate"] == "0.002"
        resource = PurePosixPath(reference["root"]) / "src/tool.py"
        assert resource != PurePosixPath("/work/src/tool.py")
        relative = resource.relative_to(reference["root"]).as_posix()
        assert port.received[relative] == port.values["src/tool.py"]
        assert reference["revision_digest"] == item.revision_digest
        assert reference["manifest_digest"] == copied["manifest_digest"]
        assert "identity" not in reference  # Locator data never grants an old effect identity.
        assert resumed.identity.generation > previous_identity.generation
        count = len(port.codes)
        stale = WorkspaceTools(resumed).invoke("workspace.execute", {
            "expected": previous_identity.to_dict(), "code": "print(42)"})
        assert stale["disposition"] == "rejected" and len(port.codes) == count
    finally:
        resumed.close()


@pytest.mark.parametrize("lost_reply", [False, True])
def test_real_runtime_records_transfer_steps_and_preserves_unknown_on_reopen(
    repository, tmp_path, lost_reply
):
    from skill_test_support import goal

    from ahc.application.runtime_workspace import RuntimeWorkspace
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.infrastructure.runtime_repository import RuntimeRepository
    from ahc.infrastructure.working_repository import WorkingSetConflict

    tools, item = prepared(repository, fail=4 if lost_reply else None)
    port = tools.workspace
    port.skill_materialization_root = port.port.skill_materialization_root
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = WorkingSetSnapshot.initial(session(1), goal())
    store.create(snapshot)

    def factory(identity, checkpoint):
        port.identity = identity
        return port

    workspace = RuntimeWorkspace.open(store, snapshot, "work", factory)
    tools.workspace = workspace
    result = invoke(tools, item)
    reopened = RuntimeRepository(store.root)
    if lost_reply:
        assert result["disposition"] == "unknown" and workspace.closed
        with pytest.raises(WorkingSetConflict):
            reopened.require_workspace(workspace.identity)
        assert len(port.codes) == 4
    else:
        assert result["disposition"] == "observed"
        assert reopened.require_workspace(workspace.identity) == snapshot
        assert workspace.identity.revision == len(port.codes)
        workspace.close()
