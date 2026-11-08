"""Real persistent workbench with durable admission, error continuation and restore."""

import os
import sys
import json
import time
from dataclasses import replace

import pytest
from skill_test_support import goal
from test_linux_native import native_owner
from test_linux_workspace_boundary import boundary

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux native workbench")
__all__ = ["native_owner", "boundary"]


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_busy_read_job_observation_and_next_cell_share_core(native_owner, tmp_path, platform):
    """Recorded incident shape with a real kernel, then useful next-cell output."""
    from test_codex_workspace_tools import bridge, message
    from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools

    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("session-" + platform, "agent", "route-" + platform), goal()))
    owner = NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True,
                                owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)
    def create(identity, checkpoint):
        return NativePythonWorkspace(store, owner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(tmp_path), interpreter=sys.executable,
            user=None, timeout=10, lifetime_seconds=30, checkpoint=checkpoint)
    workspace = RuntimeWorkspace.open(store, current, "work-" + platform, create)
    tools = WorkspaceTools(workspace)
    template, _ = bridge()
    request = replace(template.request, session=current.session,
                      capabilities=("workspace.job", "workspace.inspect", "workspace.execute"))
    native = CodexWorkspaceTools(request, tools, "thread", "turn", clock=lambda: 0)
    def invoke(index, tool, **args):
        frame=message();frame['id']=index;frame['params']['callId']=f'native{index}'
        frame['params']['tool']=tool
        frame['params']['arguments']={'expected':workspace.identity.to_dict(),**args}
        response=native.respond(frame)
        return json.loads(response['result']['contentItems'][0]['text'])
    release = tmp_path / 'release-job'
    try:
        with workspace.turn_scope(time.monotonic()+30):
            started=invoke(61,'job',action='start',job_id='one',expected_seconds=20,
                code="import time\nfrom pathlib import Path\nwhile not Path("+repr(str(release))+").exists():\n    time.sleep(.01)\nanswer=42")
            assert started['result']['status']=='running'
            rejected=invoke(62,'inspect',names=['answer'])
            assert rejected['disposition']=='rejected' and not native.reporting
            release.write_text('release')
            done=invoke(63,'job',action='wait',job_id='one',wait_seconds=10)
            assert done['result']['status']=='complete'
            result=invoke(64,'execute',code='print(answer+1)')
            assert result['disposition']=='observed' and result['result']['output'].strip()=='43'
            assert not native.reporting and not native.unresolved
    finally:
        release.touch()
        workspace.jobs.finish_turn()
        workspace.close()
    custody=BackendCustody(store,resource_role='python-workspace')
    lease=custody.acquire(current.session,'work-'+platform)
    try:
        assert custody.recover(lease,owner)==() and not custody._held(lease)
    finally:
        lease.close()


@pytest.mark.parametrize("engine", ["python", "ipython"])
def test_durable_cells_error_checkpoint_restart_and_next(native_owner, tmp_path, engine):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("session", "agent", "route"), goal()))
    owner = NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True,
                                owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)

    interpreter = (os.environ.get("AHC_TEST_IPYTHON_PYTHON", sys.executable)
                   if engine == "ipython" else sys.executable)
    def create(identity, checkpoint):
        return NativePythonWorkspace(store, owner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(tmp_path), interpreter=interpreter,
            user=None, timeout=10, lifetime_seconds=30, engine=engine, checkpoint=checkpoint)

    workspace = RuntimeWorkspace.open(store, current, "work", create)
    try:
        first = workspace.execute(workspace.identity, "answer=40")
        assert first["ok"]
        failed = workspace.execute(workspace.identity, "raise ValueError('bounded')")
        assert not failed["ok"]
        next_result = workspace.execute(workspace.identity, "answer+=2;print(answer)")
        assert next_result["ok"] and "42" in next_result["output"]
        checkpoint = workspace.checkpoint(workspace.identity, ("answer",))
        old_identity = workspace.identity
    finally:
        workspace.close()
    restored = RuntimeWorkspace.open(store, current, "work", create, checkpoint.digest)
    try:
        assert restored.identity.generation > old_identity.generation
        result = restored.execute(restored.identity, "print(answer+1)")
        assert result["ok"] and "43" in result["output"]
    finally:
        restored.close()
    custody = BackendCustody(store, resource_role="python-workspace")
    lease = custody.acquire(current.session, "work")
    try:
        assert custody.recover(lease, owner) == ()
        assert not custody._held(lease)
    finally:
        lease.close()


@pytest.mark.parametrize("ephemeral", [False, True])
def test_isolated_workspace_uses_real_custody_and_keeps_controls_invisible(
    native_owner, boundary, tmp_path, ephemeral
):
    sandbox, protected_file = boundary
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    sandbox = type(sandbox)(executable=str(sandbox.executable), sha256=sandbox.sha256,
        workspace=str(sandbox.workspace), readonly_roots=list(map(str, sandbox.readonly_roots)),
        protected_roots=[*map(str, sandbox.protected_roots), str(store.root)],
        scratch_limits=NativeScratchLimits(2 * 1024**2, 2 * 1024**2) if ephemeral else None)
    current = store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("isolated-session", "agent", "route"), goal()))
    owner = NativeProcessRunner(cwd=str(sandbox.workspace), environment={}, persistent=True,
        owner_lifetime=not ephemeral, boundary=sandbox,
        resource_limits=NativeResourceLimits(128 * 1024**2, 16, 50000) if ephemeral else None)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)

    def create(identity, checkpoint):
        return NativePythonWorkspace(store, owner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(sandbox.workspace),
            interpreter="/usr/bin/python3", user=None, timeout=10, lifetime_seconds=30,
            checkpoint=checkpoint)

    with RuntimeWorkspace.open(store, current, "work", create) as workspace:
        first = workspace.execute(workspace.identity,
            "from pathlib import Path; answer=41; Path('artifact').write_text('owned')")
        assert first["ok"]
        denied = workspace.execute(workspace.identity,
            f"Path({str(protected_file)!r}).read_text()")
        assert not denied["ok"]
        hidden = workspace.execute(workspace.identity,
            f"print(Path({str(store.root)!r}).exists(), answer+1)")
        assert hidden["ok"] and hidden["output"].strip() == "False 42"
        assert store.require_workspace(workspace.identity) == current
    assert protected_file.read_text() == "protected"
    if ephemeral:
        assert not (sandbox.workspace / "artifact").exists()
    else:
        assert (sandbox.workspace / "artifact").read_text() == "owned"
    custody = BackendCustody(store, resource_role="python-workspace")
    lease = custody.acquire(current.session, "work")
    try:
        assert custody.recover(lease, owner) == ()
        assert not custody._held(lease)
    finally:
        lease.close()
    assert owner._group.complete
