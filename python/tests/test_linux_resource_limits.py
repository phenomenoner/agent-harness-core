"""Actual child-cgroup limits; requires a disposable delegated Linux test service."""

import os
import sys

import pytest
from test_linux_native import dispose, frame, native_owner

from ahc.domain.native_resource_limits import NativeResourceLimits

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="requires native cgroup controllers")
__all__ = ["native_owner"]


@pytest.fixture
def limited_group(native_owner):
    from ahc.infrastructure.process.linux_native import LinuxProcessGroup

    group = LinuxProcessGroup()
    try:
        limits = NativeResourceLimits(64 * 1024**2, 8, 50000)
        group.configure_limits(limits)
        yield group, limits
    finally:
        assert group.cleanup(None, 5).complete


def read(group, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=group.directory_fd)
    try:
        return os.read(descriptor, 4096).decode('ascii').strip()
    finally:
        os.close(descriptor)


def test_kernel_readback_matches_requested_child_limits(limited_group):
    group, limits = limited_group
    group.verify_limits(limits)
    assert read(group, 'memory.max') == '67108864'
    assert read(group, 'memory.swap.max') == '0'
    assert read(group, 'pids.max') == '8'
    assert read(group, 'cpu.max') == '50000 100000'


def test_process_limit_stops_forks_inside_owned_group(limited_group, tmp_path):
    from ahc.infrastructure.process.linux_native import spawn_native

    group, _ = limited_group
    code = '''import errno,json,os,time
children=[]
for _ in range(24):
    try:
        pid=os.fork()
    except OSError as error:
        print(json.dumps({'children':len(children),'errno':error.errno}),flush=True)
        break
    if pid==0:
        time.sleep(60)
        os._exit(0)
    children.append(pid)
else:
    print(json.dumps({'unbounded':True}),flush=True)
time.sleep(60)
'''
    owned = spawn_native(('/usr/bin/python3', '-I', '-u', '-c', code),
                         cwd=str(tmp_path), env={}, protocol_pipes=True, group=group)
    try:
        result = frame(owned.process)
        assert 0 <= result['children'] <= 7 and result['errno'] == 11
        events = dict(line.split() for line in read(group, 'pids.events').splitlines())
        assert int(events['max']) >= 1
    finally:
        dispose(owned)
    assert group.complete


def test_memory_limit_kills_only_the_owned_overbudget_workload(limited_group, tmp_path):
    from ahc.infrastructure.process.linux_native import spawn_native

    group, _ = limited_group
    owned = spawn_native(('/usr/bin/python3', '-I', '-c',
                          'payload=bytearray(128*1024**2); print(len(payload))'),
                         cwd=str(tmp_path), env={}, protocol_pipes=True, group=group)
    try:
        assert owned.process.wait(timeout=15) != 0
        events = dict(line.split() for line in read(group, 'memory.events').splitlines())
        assert int(events['oom_kill']) >= 1
    finally:
        dispose(owned)
    assert group.complete
