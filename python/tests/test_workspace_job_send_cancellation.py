"""Cancellation must interrupt initial and callback-result writes of the pinned job."""
import threading
import time
from types import SimpleNamespace

import pytest
from test_runtime_core import setup
from test_wsl_python_workspace import Runner, Transport, open_workspace
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_tools import WorkspaceTools


class BlockedSend(Transport):
    def __init__(self,phase):
        super().__init__(); self.phase=phase
        self.waiting=threading.Event(); self.release=threading.Event(); self.callback=False
    def send(self,request,*,deadline):
        super().send(request,deadline=deadline)
        if ((self.phase=='initial' and request.get('operation')=='execute')
                or (self.phase=='callback' and 'batch_result' in request)):
            self.waiting.set(); self.release.wait(5)
            if self.closed: raise OSError('closed write')
    def receive(self,*,deadline):
        if self.phase=='callback' and self.request.get('operation')=='execute' and not self.callback:
            self.callback=True
            return {'id':self.request['id'],'concurrency':1,'batch':[
                {'call_id':'one','capability_id':'read','resource_id':'fixture','arguments':{}}]}
        return super().receive(deadline=deadline)
    def close(self):
        super().close(); self.release.set()


@pytest.mark.parametrize('phase',['initial','callback'])
def test_cancellation_interrupts_exact_job_write(monkeypatch,tmp_path,phase):
    store,snapshot,_=setup(tmp_path); transport=BlockedSend(phase); runner=Runner()
    runner.transport=transport
    def factory(identity,checkpoint=None):
        port,_,_=open_workspace(monkeypatch,identity=identity,runner=runner)
        port._batch=SimpleNamespace(session=identity.session,
            run=lambda *a,**kw:SimpleNamespace(summary=lambda:{},values=[]))
        return port
    workspace=RuntimeWorkspace.open(store,snapshot,'work',factory);tools=WorkspaceTools(workspace)
    def call(action,**fields):
        return tools.invoke('workspace.job',{'expected':workspace.identity.to_dict(),
            'action':action,'job_id':'write',**fields})
    try:
        with workspace.turn_scope(time.monotonic()+30):
            call('start',code='value=1',expected_seconds=20)
            assert transport.waiting.wait(1)
            result=call('cancel')
            assert result['result']['cancellation']['cancel_requested'] is True
            slot=workspace.jobs._workers['write']
            assert slot['done'].wait(1), 'cancelled writer retained the execution lock'
            assert slot['lease'].stopped.is_set()
            assert transport.closed
    finally:
        transport.release.set()
        workspace.close()
