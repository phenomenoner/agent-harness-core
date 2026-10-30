"""A real slow cell and subsequent continuation under an admitted task deadline."""
import sys
import time
import os

import pytest
from skill_test_support import goal
from test_linux_native import native_owner

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux native workbench")
__all__ = ["native_owner"]


@pytest.mark.parametrize("engine",["python","ipython"])
def test_slow_cell_then_checkpoint_restore_and_next_turn(native_owner, tmp_path,engine):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    current = store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("lifecycle", "fixture", "local"), goal()))
    runner = NativeProcessRunner(cwd=str(tmp_path), environment={}, persistent=True,
                                owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 120)

    def create(identity, checkpoint):
        return NativePythonWorkspace(store, runner, identity, effective_policy=policy,
            environment_digest="a" * 64, cwd=str(tmp_path),
            interpreter=os.environ.get("AHC_TEST_IPYTHON_PYTHON" if engine=="ipython" else "AHC_TEST_PYTHON",sys.executable),
            user=None, timeout=30, lifetime_seconds=120, engine=engine, checkpoint=checkpoint)

    with RuntimeWorkspace.open(store, current, "work", create) as workspace:
        started = time.monotonic()
        with workspace.turn_scope(started+90):
            result = workspace.execute(workspace.identity, "import time; time.sleep(31); answer=41; print(answer)")
            assert result["ok"] and result["output"].strip()=="41"
        assert time.monotonic()-started >= 31
        with workspace.turn_scope(time.monotonic()+60):
            result=workspace.execute(workspace.identity,"answer+=1; print(answer)")
            assert result["ok"] and result["output"].strip()=="42"
            checkpoint=workspace.checkpoint(workspace.identity,("answer",))
            old=workspace.identity
    with RuntimeWorkspace.open(store, current, "work", create, checkpoint.digest) as workspace:
        assert workspace.identity.generation>old.generation
        with workspace.turn_scope(time.monotonic()+60):
            result=workspace.execute(workspace.identity,"print(answer+1)")
            assert result["ok"] and result["output"].strip()=="43"
    assert runner._group.complete


@pytest.mark.parametrize("engine",["python","ipython"])
def test_cancel_during_admitted_scope_keeps_unknown_effect_and_next_generation(native_owner,tmp_path,engine):
    import threading
    from pathlib import Path
    from ahc.adapters.docker_workspace import WorkspaceUnavailable

    store=RuntimeRepository.initialize(tmp_path/"runtime")
    current=store.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("cancel","fixture","local"),goal()))
    runner=NativeProcessRunner(cwd=str(tmp_path),environment={},persistent=True,owner_lifetime=True)
    policy=EffectivePolicy(1,"manual","workspace-write",True,"a"*64,120)
    cancelled=[False]
    def create(identity,checkpoint):
        return NativePythonWorkspace(store,runner,identity,effective_policy=policy,
            environment_digest="a"*64,cwd=str(tmp_path),
            interpreter=os.environ.get("AHC_TEST_IPYTHON_PYTHON" if engine=="ipython" else "AHC_TEST_PYTHON",sys.executable),
            user=None,timeout=30,lifetime_seconds=120,engine=engine,checkpoint=checkpoint,
            cancelled=lambda:cancelled[0])
    workspace=RuntimeWorkspace.open(store,current,"work",create)
    old=workspace.identity
    timer=threading.Timer(0.5,lambda:cancelled.__setitem__(0,True))
    timer.start();started=time.monotonic()
    try:
        with pytest.raises(WorkspaceUnavailable):
            with workspace.turn_scope(started+90):
                workspace.execute(old,"from pathlib import Path; import time; Path('marker').write_text('once'); time.sleep(31)")
        assert time.monotonic()-started<10 and workspace.closed and runner._group.complete
        assert (tmp_path/'marker').read_text()=='once'
    finally:
        timer.cancel();workspace.close()
    cancelled[0]=False
    with RuntimeWorkspace.open(store,current,"work",create) as following:
        assert following.identity.generation>old.generation
        with following.turn_scope(time.monotonic()+60):
            result=following.execute(following.identity,"from pathlib import Path; print(Path('marker').read_text())")
            assert result['ok'] and result['output'].strip()=='once'
    assert runner._group.complete
