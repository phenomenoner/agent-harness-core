"""Real interpreter bytes across backup/activation; fixture transport, no WSL claim."""

import base64
import json
import queue
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from test_runtime_core import setup

from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.adapters.workspace_kernel import KERNEL
from ahc.adapters.wsl_python_checkpoint import validate_checkpoint
from ahc.application.runtime_service import RuntimeService
from ahc.domain.workspace import WorkspaceCheckpoint
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


class KernelPort:
    def __init__(self, identity, checkpoint, command):
        validate_checkpoint(identity, checkpoint, initial=True)
        self.identity, self.sequence = identity, 0
        self.process = subprocess.Popen([sys._base_executable, "-I", "-u", "-c", command],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.replies = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()
        try:
            self.request("restore", values={} if checkpoint is None
                         else json.loads(checkpoint.values_json))
        except BaseException:
            self.close()
            raise

    def _read(self):
        for line in self.process.stdout:
            self.replies.put(json.loads(line))

    def request(self, operation, **fields):
        self.sequence += 1
        self.process.stdin.write(json.dumps({"id": self.sequence, "operation": operation,
                                             **fields}) + "\n")
        self.process.stdin.flush()
        result = self.replies.get(timeout=10)
        assert result["id"] == self.sequence and result["ok"], result
        return result

    def execute(self, expected, code):
        assert expected == self.identity
        result = self.request("execute", code=code)
        self.identity = replace(expected, revision=expected.revision + 1)
        return {**result, "identity": self.identity.to_dict()}

    def checkpoint(self, expected, names):
        assert expected == self.identity
        result = self.request("checkpoint", names=list(names))
        return WorkspaceCheckpoint(expected, json.dumps(result["values"]))

    def export_artifact(self, expected, name):
        assert expected == self.identity
        return base64.b64decode(self.request("export_artifact", name=name)["data"], validate=True)

    def close(self):
        if self.process.stdin is not None:
            self.process.stdin.close()
            self.process.stdin = None
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise
        self.process.stdout.close()
        self.process.stderr.close()


@pytest.mark.parametrize("engine", ["python", "ipython"])
def test_restore_rebuilds_real_interpreter_and_preserves_original_artifact(tmp_path, engine):
    command = KERNEL
    if engine == "ipython":
        ipython = pytest.importorskip("IPython")
        packages = str(Path(ipython.__file__).resolve().parent.parent)
        command = f"import sys;sys.path.insert(0,{packages!r});exec({IPYTHON_KERNEL!r})"
    repo, saved, _ = setup(tmp_path)
    ports = []

    def factory(identity, checkpoint):
        port = KernelPort(identity, checkpoint, command)
        ports.append(port)
        return port

    artifacts = ArtifactRepository(repo)
    with RuntimeService(repo, None).open_workspace(saved, "work", factory) as original:
        original.execute(original.identity, "value = 41\ntext = 'original bytes'")
        checkpoint = original.checkpoint(original.identity, ("value", "text"))
        artifact = original.capture_artifact(original.identity, "text", artifacts, "text/plain")
    assert ports[0].process.returncode == 0
    source_digest = _digest(repo.root / WORKING_DB_FILENAME)
    backup, prepared, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (saved,),
                                        source_digest, transformed)
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                             receipt["sha256"], manifest["sha256"], source_digest)
    recovered = repo.read_current("session")
    with RuntimeService(repo, None).open_workspace(recovered, "work", factory,
            checkpoint_digest=checkpoint.digest) as resumed:
        result = resumed.execute(resumed.identity, "value += 1\nprint(value)")
        assert result["output"].strip() == "42"
        fresh = resumed.checkpoint(resumed.identity, ("value",))
        assert json.loads(fresh.values_json) == {"value": 42}
        assert artifacts.read(recovered.session, artifact.artifact_id) == (
            artifact, b"original bytes")
        assert resumed.identity.session != original.identity.session
    assert all(port.process.returncode == 0 for port in ports)
