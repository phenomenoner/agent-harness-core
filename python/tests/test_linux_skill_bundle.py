"""Real descriptor-bound copies; Windows does not qualify Linux filesystem semantics."""

import base64
import hashlib
import os
from pathlib import Path

import pytest

from ahc.adapters.linux_skill_bundle import SkillBundleWriter

pytestmark = pytest.mark.skipif(os.name != "posix", reason="requires Linux directory descriptors")


def add(writer, name, content):
    writer.call("begin_file", name, len(content), hashlib.sha256(content).hexdigest())
    for offset in range(0, len(content), 8192):
        writer.call("chunk", offset, base64.b64encode(content[offset:offset + 8192]).decode())
    writer.call("end_file")


def test_actual_bytes_zero_length_unicode_and_repeated_copy_preserve_originals(tmp_path):
    values = {"SKILL.md": "# 資源\r\n".encode(), "src/script.py": b"raise SystemExit(99)",
              "assets/empty": b"", "assets/data.bin": bytes(range(256)) * 75}
    roots = []
    for _ in range(2):
        writer = SkillBundleWriter(str(tmp_path), "a" * 64, len(values),
                                   sum(map(len, values.values())))
        for name, raw in values.items():
            add(writer, name, raw)
        result = writer.call("finish")
        root = Path(result["root"])
        roots.append(root)
        actual = {str(p.relative_to(root)): p.read_bytes()
                  for p in root.rglob("*") if p.is_file()}
        assert actual == values
        assert writer.root_fd is None and writer.file_fd is None and writer.base_fd is None
    assert roots[0] != roots[1]


def test_replaced_parent_cannot_redirect_a_write_outside_bundle(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    writer = SkillBundleWriter(str(tmp_path), "a" * 64, 2, 2)
    add(writer, "dir/first", b"1")
    root = tmp_path / writer.name
    (root / "dir").rename(root / "retained")
    (root / "dir").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        add(writer, "dir/second", b"2")
    assert list(outside.iterdir()) == []
    assert (root / "retained/first").read_bytes() == b"1"
    assert writer.root_fd is None


def test_final_readback_rejects_changed_files_and_fifo_without_blocking(tmp_path):
    for kind in ("changed", "fifo"):
        writer = SkillBundleWriter(str(tmp_path), "a" * 64, 1, 0)
        add(writer, "file", b"")
        target = tmp_path / writer.name / "file"
        if kind == "changed":
            target.write_bytes(b"changed")
        else:
            target.unlink()  # This test owns the disposable empty file.
            os.mkfifo(target)
        with pytest.raises(ValueError):
            writer.call("finish")
        assert writer.root_fd is None


@pytest.mark.parametrize("name", ["../outside", "/absolute", "a/../b", "a\\b", "a//b"])
def test_invalid_paths_close_descriptors_without_creating_targets(tmp_path, name):
    writer = SkillBundleWriter(str(tmp_path), "a" * 64, 1, 0)
    root = tmp_path / writer.name
    with pytest.raises(ValueError):
        add(writer, name, b"")
    assert list(root.iterdir()) == []
    assert writer.root_fd is None


def test_actual_workspace_load_then_explicit_script_execution(repository, tmp_path):
    import contextlib
    import io
    import json
    import sys
    from dataclasses import replace

    from skill_test_support import goal, session
    from test_skill_graph_convergence import _activate_with_report
    from test_skill_markdown_resources import modern, resource

    from ahc.application.runtime_workspace import RuntimeWorkspace
    from ahc.application.workspace_skills import WorkspaceSkillTools
    from ahc.application.workspace_tools import WorkspaceTools
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    values = {"src/data.txt": b"42", "src/tool.py": (
        b"from pathlib import Path\nprint(Path(__file__).with_name('data.txt').read_text())\n")}
    item = modern(agent_allowlist=(session(1).agent_id,),
                  assets=tuple(resource(k, v) for k, v in values.items()))
    repository.stage_assets(item.assets, values)
    _activate_with_report(repository, item, "baseline")
    base = tmp_path / "workspace"
    base.mkdir()
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = WorkingSetSnapshot.initial(session(1), goal())
    store.create(snapshot)

    class LocalPort:
        skill_materialization_root = str(base)

        def __init__(self, identity, checkpoint):
            self.identity, self.namespace = identity, {}

        def execute(self, expected, code):
            assert expected == self.identity
            self.identity = replace(expected, revision=expected.revision + 1)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, self.namespace)
            return {"identity": self.identity.to_dict(), "ok": True,
                    "output": output.getvalue(), "output_truncated": False}

        def close(self):
            pass

    with RuntimeWorkspace.open(store, snapshot, "work", LocalPort) as workspace:
        tools = WorkspaceTools(workspace, skills=WorkspaceSkillTools(repository))
        result = tools.invoke("workspace.skills_materialize", {
            "expected": workspace.identity.to_dict(), "revision_digest": item.revision_digest})
        assert result["disposition"] == "observed"
        assert result["result"]["scripts_executed"] is False
        root = Path(result["result"]["root"])
        assert (root / "src/tool.py").read_bytes() == values["src/tool.py"]
        code = ("import subprocess,json\nprint(json.dumps(subprocess.run("
                f"[{sys.executable!r},{str(root / 'src/tool.py')!r}],"
                "capture_output=True,text=True,check=True,timeout=5).stdout))")
        executed = tools.invoke("workspace.execute", {
            "expected": workspace.identity.to_dict(), "code": code})
        assert json.loads(executed["result"]["output"]) == "42\n"
