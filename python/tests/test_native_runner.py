"""Real native custody, controller loss and subsequent work in one owner service."""

import json
import os
import subprocess
import sys
import time
from dataclasses import replace

import pytest
from test_linux_native import native_owner, frame
from test_native_process_custody import setup

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux native custody")


def runner(tmp_path):
    return NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True)


def command(marker):
    return (sys.executable, "-I", "-u", "-c",
            "import pathlib,sys,time,json; "
            "p=pathlib.Path(sys.argv[1]); "
            "p.write_text(p.read_text()+'effect\\n' if p.exists() else 'effect\\n'); "
            "print(json.dumps({'ready':True}),flush=True); time.sleep(40)", str(marker))


def close_streams(owner):
    owner.close()
    if owner._owned is not None:
        for stream in (owner._owned.process.stdin, owner._owned.process.stdout):
            stream.close()


def next_work(custody, lease, workspace, tmp_path):
    successor = runner(tmp_path)
    try:
        identity = custody.create(lease, workspace, successor, command(tmp_path / "next"),
                                  lifetime_seconds=30)
        wire = successor.attach(identity, custody, lease)
        assert wire.receive(deadline=time.monotonic() + 5) == {"ready": True}
        assert successor.observe(identity).state == "running"
        wire.close()
        assert custody.recover(lease, successor) == (identity,)
        assert not custody._held(lease)
        return identity
    finally:
        close_streams(successor)


def test_actual_preintent_attachment_cleanup_and_next(native_owner, tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    try:
        first = next_work(custody, lease, workspace, tmp_path)
        second = next_work(custody, lease, workspace, tmp_path)
        assert first != second
        assert (tmp_path / "next").read_text() == "effect\neffect\n"
    finally:
        lease.close()


def test_old_lifetime_callback_cannot_stop_reused_runner_generation(native_owner, tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    owner = runner(tmp_path)
    try:
        first = custody.create(lease, workspace, owner, command(tmp_path / "old"), lifetime_seconds=30)
        assert frame(owner._owned.process) == {"ready": True}
        old_process = owner._owned
        assert custody.recover(lease, owner) == (first,)
        second = custody.create(lease, workspace, owner, command(tmp_path / "new"), lifetime_seconds=30)
        assert frame(owner._owned.process) == {"ready": True}
        owner._expire(old_process)
        assert owner.observe(second).state == "running"
        assert owner._owned.process.poll() is None
        custody.recover(lease, owner)
    finally:
        close_streams(owner)
        lease.close()


def test_actual_lost_ack_no_replay_recovery_and_next(native_owner, tmp_path, monkeypatch):
    store, custody, lease, workspace = setup(tmp_path)
    owner = runner(tmp_path)
    original = owner.create_attempt

    def lose(*args, **kwargs):
        original(*args, **kwargs)
        assert frame(owner._owned.process) == {"ready": True}
        raise RuntimeError("lost acknowledgement")

    monkeypatch.setattr(owner, "create_attempt", lose)
    try:
        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            custody.create(lease, workspace, owner, command(tmp_path / "once"), lifetime_seconds=30)
        with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
            custody.create(lease, workspace, owner, command(tmp_path / "once"), lifetime_seconds=30)
        fresh = runner(tmp_path)
        assert custody.recover(lease, fresh) == (owner._attempt.identity,)
        assert (tmp_path / "once").read_text() == "effect\n"
        next_work(custody, lease, workspace, tmp_path)
    finally:
        close_streams(owner)
        lease.close()


def test_replaced_generation_rejects_without_killing_peer(native_owner, tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    owner = runner(tmp_path)
    try:
        identity = custody.create(lease, workspace, owner, command(tmp_path / "peer"),
                                  lifetime_seconds=30)
        assert frame(owner._owned.process) == {"ready": True}
        fresh = runner(tmp_path)
        for other in (replace(identity, inode=identity.inode + 1),
                      replace(identity, invocation_id="f" * 32)):
            with pytest.raises(BackendCustodyUnavailable, match="unverified"):
                fresh.stop(other)
            assert owner._owned.process.poll() is None
        assert custody.recover(lease, owner) == (identity,)
    finally:
        close_streams(owner)
        lease.close()


def test_controller_crash_reconstructs_durable_attempt_without_replay(native_owner, tmp_path):
    store, custody, lease, workspace = setup(tmp_path)
    lease.close()
    packet = tmp_path / "controller.json"
    packet.write_text(json.dumps({"root": str(store.root), "workspace": workspace.to_dict(),
                                  "cwd": str(tmp_path), "command": command(tmp_path / "once")}))
    code = '''import json,os,sys,select
from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.domain.workspace import WorkspaceIdentity
p=json.load(open(sys.argv[1]))
w=WorkspaceIdentity.from_dict(p['workspace'])
s=RuntimeRepository(p['root']); c=BackendCustody(s); l=c.acquire(w.session,w.workspace_id)
r=NativeProcessRunner(cwd=p['cwd'],environment={},persistent=True)
def crash(*args,**kwargs):
    assert select.select([r._owned.process.stdout],[],[],5)[0]
    assert json.loads(r._owned.process.stdout.readline())=={'ready':True}
    os._exit(42)
c._record_created=crash
c.create(l,w,r,tuple(p['command']),lifetime_seconds=30)
raise AssertionError('controller survived')
'''
    controller = subprocess.run((sys.executable, "-I", "-c", code, str(packet)),
                                env=dict(os.environ), capture_output=True, timeout=10)
    assert controller.returncode == 42, controller.stderr.decode()
    lease = custody.acquire(workspace.session, workspace.workspace_id)
    try:
        assert not custody._held(lease)  # Crash preceded record; only the durable intent exists.
        recovered = custody.recover(lease, runner(tmp_path))
        assert len(recovered) == 1
        assert (tmp_path / "once").read_text() == "effect\n"
        assert runner(tmp_path).observe(recovered[0]).cgroup_empty
        next_work(custody, lease, workspace, tmp_path)
    finally:
        lease.close()
