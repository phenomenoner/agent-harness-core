"""Real bounded backend namespace and composition checks, without model inference."""

import hashlib
import json
import os
import select
import socket
import sys
import time
from pathlib import Path
from uuid import UUID

import pytest
from test_foreground_backend_protection import protected_config
from test_linux_native import native_owner as _native_owner

from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.linux_backend_boundary import BACKEND_HOME, LinuxBackendBoundary

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native namespaces and custody")
native_owner = _native_owner


@pytest.fixture
def protected(tmp_path):
    bwrap = Path(os.environ["AHC_TEST_BWRAP"]).resolve()
    work, controls, state = (tmp_path / name for name in ("work", "controls", "state-home"))
    for path in (work, controls, state):
        path.mkdir()
    for name in ("sessions", "archived_sessions", "state"):
        (state / name).mkdir()
    sentinel = controls / "authority"
    sentinel.write_text("unchanged-authority")
    for parent in (work, state):
        directory = parent / ".codex" if parent == work else parent
        directory.mkdir(exist_ok=True)
        (directory / "config.toml").write_text(
            '[mcp_servers.unanticipated_server]\ncommand="/usr/bin/false"\n'
        )
        (directory / "hooks.json").write_text(
            '{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"/usr/bin/false"}]}]}}'
        )
    boundary = LinuxBackendBoundary(
        executable=str(bwrap),
        sha256=hashlib.sha256(bwrap.read_bytes()).hexdigest(),
        workspace=str(work),
        readonly_roots=["/usr"],
        protected_roots=[str(controls)],
        state_root=str(state),
        home_bytes=32 * 1024**2,
        temporary_bytes=32 * 1024**2,
    )
    return boundary, sentinel


def runner_for(boundary):
    return NativeProcessRunner(
        cwd=str(boundary.workspace),
        environment={},
        boundary=boundary,
        resource_limits=NativeResourceLimits(
            memory_bytes=512 * 1024**2,
            pids=64,
            cpu_quota_us=100000,
            cpu_period_us=100000,
            swap_bytes=0,
        ),
    )


def test_workspace_network_config_denial_and_persistent_history(native_owner, protected):
    boundary, sentinel = protected
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    code = f"""import json,pathlib,socket,os
home=pathlib.Path({BACKEND_HOME!r})
assert os.environ['HOME']==str(home) and os.environ['CODEX_HOME']==str(home)
locks=home/'thread-writer-locks'
assert locks.stat().st_ino==(home/'state'/'thread-writer-locks').stat().st_ino
assert not pathlib.Path({str(sentinel)!r}).exists()
for p in (pathlib.Path({str(sentinel)!r}),home/'config.toml',home/'hooks.json',
          pathlib.Path('.codex/config.toml'),pathlib.Path('.agents/plugins/marketplace.json')):
    assert not p.exists(),p
    try: p.write_text('escaped')
    except OSError: pass
    else: raise AssertionError('control write escaped')
assert not pathlib.Path('/run/user').exists()
assert not pathlib.Path('/etc/codex').exists()
pathlib.Path('allowed-result').write_text('bounded artifact')
(home/'sessions/retained').write_text('history')
connection=socket.create_connection(('127.0.0.1',{listener.getsockname()[1]}),timeout=2)
connection.sendall(b'network');connection.close()
print(json.dumps({{'allowed':True,'denied':True,'network':True,'sealed':True}}),flush=True)
"""
    runner = runner_for(boundary)
    command = ("/usr/bin/python3", "-I", "-u", "-c", code)
    attempt = runner.prepare_creation(command, lifetime_seconds=20)
    try:
        runner.create_attempt(attempt, command, lifetime_seconds=20)
        assert select.select([runner._owned.process.stdout], [], [], 15)[0]
        assert all(json.loads(runner._owned.process.stdout.readline()).values())
        listener.settimeout(2)
        connection, _ = listener.accept()
        assert connection.recv(64) == b"network"
        connection.close()
        assert runner._owned.process.wait(timeout=5) == 0
    finally:
        listener.close()
        runner.close()
    assert runner.observe(attempt.identity).state == "absent"
    assert sentinel.read_text() == "unchanged-authority"
    assert (boundary.state_root / "sessions/retained").read_text() == "history"
    assert "unanticipated_server" in (boundary.workspace / ".codex/config.toml").read_text()


def test_history_alias_and_changed_wrapper_cannot_launch(native_owner, protected):
    boundary, sentinel = protected
    alias = boundary.state_root / "sessions/alias"
    alias.symlink_to(sentinel)
    runner = runner_for(boundary)
    with pytest.raises(ContractError, match="aliases"):
        runner.prepare_creation(("/usr/bin/python3", "-c", "pass"), lifetime_seconds=10)
    assert runner._group is None
    alias.unlink()  # Only this test's symlink; the target is never changed.
    command = ("/usr/bin/python3", "-c", "pass")
    attempt = runner.prepare_creation(command, lifetime_seconds=10)
    boundary.sha256 = "0" * 64
    try:
        with pytest.raises(ContractError, match="changed"):
            runner.create_attempt(attempt, command, lifetime_seconds=10)
        assert runner.observe(attempt.identity).state == "stopped"
    finally:
        runner.close()
    assert runner.observe(attempt.identity).state == "absent"
    assert sentinel.read_text() == "unchanged-authority"


def test_deadline_and_cancel_retire_the_owned_group(native_owner, protected):
    boundary, _ = protected
    for deadline, abort in ((1, False), (20, True)):
        runner = runner_for(boundary)
        command = (
            "/usr/bin/python3",
            "-I",
            "-u",
            "-c",
            "import time;print('ready',flush=True);time.sleep(60)",
        )
        attempt = runner.prepare_creation(command, lifetime_seconds=deadline)
        try:
            runner.create_attempt(attempt, command, lifetime_seconds=deadline)
            assert select.select([runner._owned.process.stdout], [], [], 5)[0]
            assert runner._owned.process.stdout.readline().strip() == b"ready"
            if abort:
                runner.stop(attempt.identity)
            else:
                runner._owned.process.wait(timeout=10)
        finally:
            runner.close()
        assert runner.observe(attempt.identity).state == "absent"


def test_actual_vendor_startup_has_no_inherited_mcp_and_retains_thread_history(
    native_owner, protected
):
    boundary, sentinel = protected
    bundle = Path(os.environ["AHC_TEST_CODEX_BUNDLE"]).resolve()
    value = protected_config()
    value["runtime_root"] = str(sentinel.parent)
    value["workspace"].update(cwd=str(boundary.workspace), interpreter="/usr/bin/python3")
    value["backend"].update(codex_home=str(boundary.state_root), bundle=str(bundle))
    for profile in (value["workspace"]["protection"], value["backend"]["protection"]):
        profile.update(
            executable=str(boundary.executable),
            sha256=boundary.sha256,
            readonly_roots=["/usr", str(bundle)],
            protected_roots=[str(sentinel.parent)],
        )
    # Qualify the vendor at the observed native budget, independently of Python.
    value["backend"]["protection"]["resource_limits"] = {
        "memory_bytes": 512 * 1024**2,
        "pids": 64,
        "cpu_quota_us": 100000,
        "cpu_period_us": 100000,
        "swap_bytes": 0,
    }
    factory = TrustedCodexBackendFactory(None, parse_foreground_config_native(value))
    thread_id = None
    for generation in range(2):
        runner = factory.new_runner()
        attempt = runner.prepare_creation(factory._command, lifetime_seconds=90)
        wire = None
        try:
            runner.create_attempt(attempt, factory._command, lifetime_seconds=90)
            wire = CodexStdioTransport(runner._owned, streaming=True, max_wait=90)
            deadline = time.monotonic() + 75

            def rpc(identity, method, params, wire=wire, deadline=deadline):
                wire.send({"id": identity, "method": method, "params": params}, deadline=deadline)
                for _ in range(128):
                    frame = wire.receive(deadline=deadline)
                    if frame.get("id") == identity:
                        assert "result" in frame, frame
                        return frame["result"]
                pytest.fail("vendor RPC frame budget exceeded")

            initialized = rpc(
                1,
                "initialize",
                {
                    "clientInfo": {"name": "ahc_gate", "version": "1.0"},
                    "capabilities": {"experimentalApi": True},
                },
            )
            assert initialized["codexHome"] == BACKEND_HOME
            wire.send({"method": "initialized", "params": {}}, deadline=deadline)
            config = rpc(10, "config/read", {"cwd": str(boundary.workspace), "includeLayers": True})
            assert not config["config"].get("mcp_servers"), config
            if generation == 0:
                started = rpc(
                    2,
                    "thread/start",
                    {
                        "cwd": str(boundary.workspace),
                        "model": "gpt-5.2-codex",
                        "modelProvider": "openai",
                        "approvalPolicy": "never",
                        "sandbox": "read-only",
                        "ephemeral": False,
                    },
                )
                thread_id = started["thread"]["id"]
                # A new empty thread is lazy; append and flush synthetic history
                # through the pinned API without admitting a provider turn.
                rpc(12, "thread/inject_items", {
                    "threadId": thread_id,
                    "items": [{
                        "type": "message", "role": "user",
                        "content": [{"type": "input_text",
                                     "text": "AHC synthetic offline history fixture"}],
                    }],
                })
            else:
                saved = rpc(11, "thread/read", {"threadId": thread_id, "includeTurns": False})
                assert saved["thread"]["id"] == thread_id
            # No turn/start, auth import, login or real model request.
        finally:
            if wire is not None:
                wire.close()
            runner.close()
        assert runner.observe(attempt.identity).state == "absent"
    histories = list((boundary.state_root / "sessions").rglob("*.jsonl"))
    assert histories
    assert any("AHC synthetic offline history fixture" in path.read_text() for path in histories)
    installation = boundary.state_root / "state" / "installation_id"
    assert str(UUID(installation.read_text().strip())) == installation.read_text().strip()
    assert sentinel.read_text() == "unchanged-authority"


def test_protected_foreground_parent_child_checkpoint_and_restart(
    native_owner, tmp_path, monkeypatch
):
    import test_native_foreground as existing

    work, history = tmp_path / "work", tmp_path / "backend-history"
    work.mkdir()
    history.mkdir()
    for name in ("sessions", "archived_sessions", "state"):
        (history / name).mkdir()
    # Existing shared scenario keeps its own starts assertion; this test-owned
    # alias stays outside the mounted work directory and is never imported.
    (tmp_path / "starts").symlink_to(work / "starts")
    sentinel = tmp_path / "runtime/authority"
    peer = existing.PEER
    peer = peer.replace(
        "with open(p['starts'],'a')", "with open(" + repr(str(work / "starts")) + ",'a')"
    )
    # Check before any peer protocol input, for both parent and ordinary child.
    peer = f"from pathlib import Path\nassert not Path({str(sentinel)!r}).exists()\n" + peer
    monkeypatch.setattr(existing, "PEER", peer)
    original = existing.compose_foreground_document
    bwrap = Path(os.environ["AHC_TEST_BWRAP"]).resolve()
    roots = sorted(
        {"/usr", str(Path(sys.prefix).resolve()), str(Path(sys.executable).resolve().parent.parent)}
    )

    def compose(value):
        if not sentinel.exists():
            sentinel.write_text("protected-authority")
        p = {
            "executable": str(bwrap),
            "sha256": hashlib.sha256(bwrap.read_bytes()).hexdigest(),
            "readonly_roots": roots,
            "protected_roots": [value["runtime_root"]],
            "resource_limits": {
                "memory_bytes": 512 * 1024**2,
                "pids": 64,
                "cpu_quota_us": 100000,
                "cpu_period_us": 100000,
                "swap_bytes": 0,
            },
        }
        value["workspace"].update(kind="linux-python", cwd=str(work), protection=p)
        value["backend"].update(
            codex_home=str(history),
            protection=p
            | {
                "network_mode": "shared",
                "home_mode": "sealed-history",
                "home_bytes": 32 * 1024**2,
                "temporary_bytes": 32 * 1024**2,
            },
        )
        return original(value)

    monkeypatch.setattr(existing, "compose_foreground_document", compose)
    existing.test_native_composition_sequential_child_checkpoint_restart(
        native_owner, ("linux-python", sys.executable), tmp_path, monkeypatch
    )
    assert sentinel.read_text() == "protected-authority"
