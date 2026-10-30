"""Real namespace boundary when an explicitly selected bubblewrap is available."""

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys

import pytest

from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.linux_workspace_boundary import LinuxWorkspaceBoundary

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux namespaces")


@pytest.fixture
def boundary(tmp_path):
    from pathlib import Path

    binary = os.environ.get("AHC_TEST_BWRAP") or shutil.which("bwrap")
    if not binary:
        pytest.skip("explicit bubblewrap test runtime unavailable")
    path = Path(binary).resolve()
    workspace, controls = tmp_path / "work", tmp_path / "controls"
    workspace.mkdir()
    controls.mkdir()
    policy = controls / "policy"
    policy.write_text("protected")
    instance = LinuxWorkspaceBoundary(
        executable=str(path),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        workspace=str(workspace),
        readonly_roots=["/usr"],
        protected_roots=[str(controls)],
    )
    return instance, policy


def test_normal_write_works_and_policy_process_network_escape_fails(boundary):
    sandbox, policy = boundary
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    script = f"""
import json, os, pathlib, socket
pathlib.Path('result').write_text('artifact')
checks = [pathlib.Path('result').read_text() == 'artifact',
          not pathlib.Path({str(policy)!r}).exists(),
          not pathlib.Path('/proc/{os.getpid()}').exists(),
          not pathlib.Path('/run/user').exists()]
try:
    pathlib.Path({str(policy)!r}).write_text('forged')
    checks.append(False)
except OSError:
    checks.append(True)
try:
    socket.create_connection(('127.0.0.1',{listener.getsockname()[1]}), timeout=1)
    checks.append(False)
except OSError:
    checks.append(True)
checks.append(os.system('/usr/bin/unshare --user /bin/true >/tmp/error 2>&1') != 0)
print(json.dumps(checks))
"""
    try:
        result = subprocess.run(
            sandbox.command(("/usr/bin/python3", "-I", "-c", script)),
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
    finally:
        listener.close()
    assert all(json.loads(result.stdout))
    assert policy.read_text() == "protected"


def test_preexisting_hardlink_or_symlink_cannot_smuggle_control_files(boundary):
    sandbox, policy = boundary
    os.link(policy, sandbox.workspace / "alias")
    with pytest.raises(ContractError, match="hardlinks"):
        sandbox.command(("/usr/bin/python3", "-I", "-c", "pass"))


def test_in_workspace_symlinks_remain_usable(boundary):
    sandbox, policy = boundary
    library = sandbox.workspace / "lib"
    library.mkdir()
    (library / "data").write_text("ordinary")
    (sandbox.workspace / "lib64").symlink_to("lib", target_is_directory=True)
    result = subprocess.run(
        sandbox.command(("/usr/bin/python3", "-I", "-c",
                         "from pathlib import Path; p=Path('lib64/data'); "
                         "assert p.read_text()=='ordinary'; p.write_text('updated')")),
        capture_output=True, text=True, timeout=20, check=True)
    assert result.returncode == 0
    assert (library / "data").read_text() == "updated"
    assert policy.read_text() == "protected"


def test_workspace_link_cannot_alias_external_or_nested_controls(boundary):
    sandbox, policy = boundary
    alias = sandbox.workspace / "alias"
    alias.symlink_to(policy)
    with pytest.raises(ContractError, match="leaves ordinary work"):
        sandbox.command(("/usr/bin/python3", "-I", "-c", "pass"))
    alias.unlink()
    nested = sandbox.workspace / "controls"
    nested.mkdir()
    (nested / "secret").write_text("hidden")
    sandbox = LinuxWorkspaceBoundary(
        executable=str(sandbox.executable), sha256=sandbox.sha256,
        workspace=str(sandbox.workspace), readonly_roots=["/usr"],
        protected_roots=[str(policy.parent), str(nested)])
    alias.symlink_to(nested / "secret")
    with pytest.raises(ContractError, match="leaves ordinary work"):
        sandbox.command(("/usr/bin/python3", "-I", "-c", "pass"))


def test_runtime_digest_and_overlapping_control_mounts_fail_before_launch(boundary):
    sandbox, policy = boundary
    sandbox.sha256 = "0" * 64
    with pytest.raises(ContractError, match="executable changed"):
        sandbox.command(("/usr/bin/python3", "-I", "-c", "pass"))
    with pytest.raises(ContractError, match="overlap"):
        LinuxWorkspaceBoundary(
            executable=str(sandbox.executable),
            sha256="0" * 64,
            workspace=str(policy.parent),
            readonly_roots=["/usr"],
            protected_roots=[str(policy)],
        )


def test_ephemeral_workspace_and_tmp_exhaust_at_their_caps_without_host_writes(boundary):
    existing, policy = boundary
    sentinel = existing.workspace / "host-input"
    sentinel.write_text("must remain hidden and unchanged")
    sandbox = LinuxWorkspaceBoundary(
        executable=str(existing.executable), sha256=existing.sha256,
        workspace=str(existing.workspace), readonly_roots=["/usr"],
        protected_roots=[str(policy.parent)],
        scratch_limits=NativeScratchLimits(2 * 1024**2, 2 * 1024**2))
    script = '''import errno,json,os,pathlib
assert not pathlib.Path('host-input').exists()
checks=[]
for name in ('artifact','/tmp/temporary'):
    written=0
    try:
        with open(name,'wb',buffering=0) as stream:
            for _ in range(4):
                written+=stream.write(b'x'*(1024**2))
    except OSError as error:
        checks.append(error.errno==errno.ENOSPC and written<=2*1024**2)
    else:
        checks.append(False)
    pathlib.Path(name).unlink()
pathlib.Path('small').write_text('still usable')
checks.append(pathlib.Path('small').read_text()=='still usable')
print(json.dumps(checks))
'''
    command = sandbox.command(("/usr/bin/python3", "-I", "-c", script))
    assert "--bind" not in command
    result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=True)
    assert json.loads(result.stdout) == [True, True, True]
    assert list(existing.workspace.iterdir()) == [sentinel]
    assert sentinel.read_text() == "must remain hidden and unchanged"
    assert policy.read_text() == "protected"
