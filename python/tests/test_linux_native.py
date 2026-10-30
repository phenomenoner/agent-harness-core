"""Real Linux ownership checks; require a disposable delegated user service."""

import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux cgroup v2 contract")


@pytest.fixture
def native_owner():
    from ahc.infrastructure.process.linux_native import _service_directory
    from ahc.infrastructure.process.managed import ProcessCleanupError

    try:
        descriptor = _service_directory()
    except ProcessCleanupError:
        if os.environ.get("AHC_REQUIRE_NATIVE_TESTS") == "1":
            pytest.fail("required native delegated service is unavailable")
        pytest.skip("run in a delegated systemd user service")
    os.close(descriptor)


def spawn(code, *, env=None, cwd=None, extra=()):
    from ahc.infrastructure.process.managed import spawn_managed

    return spawn_managed((sys.executable, "-I", "-u", "-c", code, *extra),
                         cwd=cwd, env={} if env is None else env, protocol_pipes=True)


def frame(process):
    assert select.select([process.stdout], [], [], 5)[0], "native peer did not respond"
    return json.loads(process.stdout.readline())


def dispose(owned):
    assert owned.cleanup(5).complete
    for stream in (owned.process.stdin, owned.process.stdout):
        if stream is not None:
            stream.close()


def test_unqualified_owner_rejects_before_workload(monkeypatch):
    from ahc.infrastructure.process import linux_native as native

    def unavailable():
        raise native.ProcessCleanupError("unqualified owner")

    monkeypatch.setattr(native, "_service_directory", unavailable)
    monkeypatch.setattr(native.subprocess, "Popen", lambda *a, **kw: pytest.fail("spawned"))
    with pytest.raises(native.ProcessCleanupError, match="unqualified"):
        spawn("raise SystemExit")


def test_explicit_environment_argv_stdio_and_next_process(native_owner, tmp_path):
    code = "import os,sys,json; print(json.dumps([sys.argv[1:],os.getcwd(),dict(os.environ)]))"
    owned = spawn(code, env={"ONLY_TEST_VALUE": "ok"}, cwd=str(tmp_path),
                  extra=("literal $() ; ` text", "繁體中文"))
    try:
        arguments, cwd, environment = frame(owned.process)
        assert arguments == ["literal $() ; ` text", "繁體中文"]
        assert cwd == str(tmp_path)
        assert environment["ONLY_TEST_VALUE"] == "ok"
        assert not {"HOME", "USERPROFILE", "CODEX_HOME", "INVOCATION_ID"} & environment.keys()
        assert owned.process.wait(timeout=5) == 0
        assert owned.cleanup(5).complete
        assert owned.cleanup(0).complete
    finally:
        dispose(owned)
    successor = spawn("print('42')")
    try:
        assert frame(successor.process) == 42
    finally:
        dispose(successor)


def test_detached_descendant_after_parent_exit_and_peer_survives(native_owner):
    code = """import os,json,time
os.setsid()
pid=os.fork()
if pid==0:
    os.setsid()
    time.sleep(45)
else:
    print(json.dumps({'child':pid,'group':open('/proc/self/cgroup').read().strip()}),flush=True)
    os._exit(0)
"""
    owned = spawn(code)
    peer = spawn("import time; print('7',flush=True); time.sleep(45)")
    try:
        observation = frame(owned.process)
        assert frame(peer.process) == 7
        assert owned.process.wait(timeout=5) == 0
        group = owned._linux_owner
        assert group._populated()
        assert Path(f"/proc/{observation['child']}/cgroup").read_text().strip() == observation['group']
        assert owned.cleanup(5).complete
        assert peer.process.poll() is None
    finally:
        dispose(owned)
        dispose(peer)


@pytest.mark.parametrize("failure", ["membership", "identity"])
def test_failed_gate_never_executes_workload(native_owner, monkeypatch, tmp_path, failure):
    from ahc.infrastructure.process import linux_native as native

    marker = tmp_path / "must-not-exist"
    if failure == "membership":
        monkeypatch.setattr(native, "_BOOTSTRAP", "import os;os._exit(78)")
    else:
        def fail_identity(_pid):
            raise RuntimeError("identity unavailable")
        monkeypatch.setattr(native, "identity_for_pid", fail_identity)
    with pytest.raises((native.ProcessCleanupError, RuntimeError)):
        spawn("from pathlib import Path;Path(" + repr(str(marker)) + ").touch()")
    assert not marker.exists()
    relative = Path("/proc/self/cgroup").read_text().strip().split("::")[1]
    assert not list(Path("/sys/fs/cgroup" + relative).glob("ahc-process-*"))


def test_indeterminate_kill_is_not_replayed(native_owner, monkeypatch):
    from ahc.infrastructure.process import linux_native as native

    owned = spawn("import time;print('1',flush=True);time.sleep(45)")
    assert frame(owned.process) == 1
    group, original_write, calls = owned._linux_owner, os.write, []

    def lost_write(fd, value):
        if fd == group.kill_fd:
            calls.append(value)
            raise OSError("lost write acknowledgement")
        return original_write(fd, value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(native.os, "write", lost_write)
            with pytest.raises(native.ProcessCleanupError, match="unverified"):
                owned.cleanup(1)
            assert not owned.cleanup(0).complete
            assert calls == [b"1"]
            assert owned.process.poll() is None
    finally:
        original_write(group.kill_fd, b"1")  # Test-owned exact handle, no product retry.
        dispose(owned)


def test_failed_observation_does_not_cache_success(native_owner, monkeypatch):
    from ahc.infrastructure.process import linux_native as native

    owned = spawn("import time;print('1',flush=True);time.sleep(45)")
    assert frame(owned.process) == 1
    group = owned._linux_owner
    try:
        with monkeypatch.context() as patch:
            def unavailable():
                raise OSError("observation unavailable")
            patch.setattr(group, "_populated", unavailable)
            with pytest.raises(native.ProcessCleanupError, match="unverified"):
                owned.cleanup(1)
            assert owned._cleanup_result is None
            assert group.events_fd >= 0
        assert owned.cleanup(5).complete
    finally:
        dispose(owned)


def test_old_kill_descriptor_cannot_target_replacement(native_owner):
    from ahc.infrastructure.process import linux_native as native

    group = native.LinuxProcessGroup()
    os.rmdir(group.name, dir_fd=group.parent_fd)
    os.mkdir(group.name, dir_fd=group.parent_fd)
    replacement_fd = os.open(group.name, native._DIRECTORY_FLAGS, dir_fd=group.parent_fd)
    replacement_kill = os.open("cgroup.kill", os.O_WRONLY, dir_fd=replacement_fd)
    code = ("import os,sys,time;fd=os.open('cgroup.procs',os.O_WRONLY,dir_fd=int(sys.argv[1]));"
            "os.write(fd,str(os.getpid()).encode());os.close(fd);"
            "print('1',flush=True);time.sleep(45)")
    process = subprocess.Popen((sys.executable, "-I", "-u", "-c", code, str(replacement_fd)),
                               pass_fds=(replacement_fd,), stdout=subprocess.PIPE)
    try:
        assert frame(process) == 1
        with pytest.raises(native.ProcessCleanupError, match="unverified"):
            group.cleanup(None, 1)
        assert process.poll() is None
        group._release_empty()
        assert os.fstat(replacement_fd).st_nlink > 0
    finally:
        os.write(replacement_kill, b"1")
        process.wait(timeout=5)
        process.stdout.close()
        os.close(replacement_kill)
        os.close(replacement_fd)
        group._release_empty()


def test_real_mcp_transport_uses_native_owner(native_owner, tmp_path):
    from ahc.adapters.mcp_stdio import McpStdioTransport

    peer = ("import sys,json\nfor line in sys.stdin:\n r=json.loads(line);"
            "print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'value':42}}),flush=True)")
    transport = McpStdioTransport.launch((sys.executable, "-I", "-u", "-c", peer),
                                          cwd=str(tmp_path), env={})
    try:
        assert transport.request("probe", {}, deadline=time.monotonic() + 5) == {"value": 42}
    finally:
        transport.close()
    assert transport.wire.owned.cleanup(0).complete


@pytest.mark.parametrize("engine", ["python", "ipython"])
def test_actual_persistent_kernel_next_cell_and_restore(native_owner, tmp_path, engine):
    from ahc.adapters.codex_transport import CodexStdioTransport
    from ahc.adapters.workspace_launch import kernel_command

    def open_kernel():
        return CodexStdioTransport(spawn(kernel_command(str(tmp_path), engine)), streaming=True)

    wire = open_kernel()
    sequence = 0

    def exchange(**request):
        nonlocal sequence
        sequence += 1
        deadline = time.monotonic() + 10
        wire.send({"id": sequence, **request}, deadline=deadline)
        result = wire.receive(deadline=deadline)
        assert result["id"] == sequence
        return result

    try:
        assert exchange(operation="execute", code="value = 40")["ok"]
        assert exchange(operation="execute", code="print(value + 2)")["output"] == "42\n"
        assert not exchange(operation="execute", code="raise ValueError('controlled')")["ok"]
        assert exchange(operation="execute", code="print(value + 3)")["output"] == "43\n"
        saved = exchange(operation="checkpoint", names=["value"])["values"]
        wire.close()
        wire = open_kernel()
        assert exchange(operation="restore", values=saved)["ok"]
        assert exchange(operation="execute", code="print(value + 4)")["output"] == "44\n"
    finally:
        wire.close()


def test_owner_crash_cleans_all_descendants(native_owner, tmp_path):
    script = tmp_path / "crash_owner.py"
    receipt = tmp_path / "receipt.json"
    script.write_text("""import json,os,sys,time
from ahc.infrastructure.process.managed import spawn_managed
child=spawn_managed((sys.executable,'-I','-c','import os,time;os.setsid();os.fork();time.sleep(45)'),cwd=None,env={})
with open(sys.argv[1],'w') as output:
    json.dump({'group':open('/proc/self/cgroup').read().strip(),'pid':child.process.pid},output)
    output.flush();os.fsync(output.fileno())
os._exit(42)
""")
    unit = "ahc-native-crash-" + uuid4().hex
    result = subprocess.run(("/usr/bin/systemd-run", "--user", "--quiet", "--wait", "--pipe",
                             "--collect", "--unit=" + unit, "--property=Delegate=yes",
                             "--property=KillMode=control-group", "--property=SendSIGKILL=yes",
                             "--property=RuntimeMaxSec=30", "--property=TimeoutStopSec=5",
                             sys.executable, str(script), str(receipt)),
                            capture_output=True, timeout=40)
    assert result.returncode == 42, result.stderr.decode(errors="replace")
    observed = json.loads(receipt.read_text())
    group_path = Path("/sys/fs/cgroup" + observed["group"].split("::")[1])
    assert not group_path.exists()
