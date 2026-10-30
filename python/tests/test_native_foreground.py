"""Linux composition with real processes and a deterministic protocol peer.

This qualifies AHC integration, not a model response or pinned Codex executable.
"""

import json
import os
import sys

import pytest
from skill_test_support import goal
from test_foreground_config_native import native_config
from test_linux_native import native_owner as _native_owner

from ahc.cli import foreground_trusted
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native foreground lifecycle")
native_owner = _native_owner


@pytest.fixture(params=("linux-python", "linux-ipython"))
def native_workspace(request):
    interpreter = (os.environ.get("AHC_TEST_IPYTHON_PYTHON", sys.executable)
                   if request.param == "linux-ipython" else sys.executable)
    return request.param, interpreter

PEER = r'''
import json,sys,pathlib,uuid
p=json.loads(sys.argv[1]); expected=p['expected']; thread='thread'; turn=uuid.uuid4().hex
def send(value):
    print(json.dumps(value),flush=True)
def completed(text):
    send({'method':'turn/completed','params':{'threadId':thread,'turn':{
        'id':turn,'status':'completed','error':None,'items':[{
        'type':'agentMessage','id':'answer','phase':'final_answer','text':text}]}}})
def call(number,tool,arguments):
    send({'id':number,'method':'item/tool/call','params':{'threadId':thread,'turnId':turn,
          'namespace':'workspace','tool':tool,'callId':'call'+str(number),'arguments':arguments}})
tools=False
for line in sys.stdin:
    message=json.loads(line); method=message.get('method')
    if method=='initialize':
        send({'id':1,'result':{'codexHome':'fixture','platformFamily':'unix',
                              'platformOs':'linux','userAgent':'controlled-peer'}})
    elif method in ('thread/start','thread/resume'):
        v=message['params']; tools=bool(v.get('dynamicTools'))
        assert v['sandbox']=='danger-full-access' and v['approvalPolicy']=='never'
        send({'id':2,'result':{'cwd':v['cwd'],'model':v['model'],'modelProvider':'openai',
            'approvalPolicy':v['approvalPolicy'],'sandbox':{'type':'dangerFullAccess'},
            'thread':{'id':thread}}})
    elif method=='turn/start':
        assert message['params']['effort']=='low'
        with open(p['starts'],'a') as out: out.write(('parent' if tools else 'child')+'\n')
        send({'id':3,'result':{'turn':{'id':turn,'status':'inProgress'}}})
        if tools:
            call(7,'execute',{'expected':expected,'code':"count=globals().get('count',0)+1;print(count)"})
        else:
            completed('child done')
    elif message.get('id')==7:
        assert message['result']['success'],message
        result=json.loads(message['result']['contentItems'][0]['text'])
        assert result['disposition']=='observed',result
        expected=result['result']['identity']
        if p['operation']=='first':
            call(8,'child',{'expected':expected,'child_id':'bounded-child','task_text':'return child done',
                            'timeout_ms':15000})
        else:
            completed('parent done')
    elif message.get('id')==8:
        assert message['result']['success'],message
        result=json.loads(message['result']['contentItems'][0]['text'])
        assert result['disposition']=='observed' and result['result']['state']=='terminal',result
        completed('parent and child done')
'''


def test_native_continuation_without_windows_entry(native_owner, native_workspace, tmp_path, monkeypatch):
    from ahc.adapters import wsl_stream_runner, wsl_unit_runner
    from ahc.cli import trusted_codex_backend

    def unavailable(*_args, **_kwargs):
        raise AssertionError("Windows entry unavailable; native work must continue")

    monkeypatch.setenv("WSL_INTEROP", "/ahc-unavailable/interop.sock")
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", unavailable)
    monkeypatch.setattr(wsl_unit_runner, "WslUnitRunner", unavailable)
    monkeypatch.setattr(trusted_codex_backend, "WslStreamRunner", unavailable)
    monkeypatch.setattr(trusted_codex_backend, "WslUnitRunner", unavailable)
    test_native_composition_sequential_child_checkpoint_restart(
        native_owner, native_workspace, tmp_path, monkeypatch)


def test_native_composition_sequential_child_checkpoint_restart(
        native_owner, native_workspace, tmp_path, monkeypatch):
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    current = store.create(WorkingSetSnapshot.initial(session, goal()))
    value = native_config()
    value.update(runtime_root=str(store.root), session=session.to_dict())
    kind, interpreter = native_workspace
    value["workspace"].update(kind=kind, cwd=str(tmp_path), interpreter=interpreter,
                              lifetime_mode="foreground")
    value["backend"].update(enabled=True, model="gpt-5.6-luna", reasoning_effort="low")
    value["policy"].update(approval_mode="yolo", sandbox_mode="danger-full-access")
    value["symbolic_canvas"] = {"enabled": True, "min_turns": 2, "trigger_mode": "all"}
    starts = tmp_path / "starts"

    class ControlledFactory(TrustedCodexBackendFactory):
        def __call__(self, request, tools, *, cancelled=lambda: False):
            self._command = (sys.executable, "-I", "-u", "-c", PEER, json.dumps({
                "expected": tools.workspace.identity.to_dict(), "operation": request.operation_id,
                "starts": str(starts)}))
            return super().__call__(request, tools, cancelled=cancelled)

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", ControlledFactory)
    runtime = compose_foreground_document(value)
    inbox = RuntimeInbox(store)
    runtime.start()
    try:
        assert runtime.admission_status()["ready"], runtime.channel_status()
        assert runtime.backend_name == "codex-linux-trusted"
        for operation in ("first", "second"):
            inbox.submit(session, operation, current.goal.digest, "perform bounded fixture task")
            result = runtime.step()
            assert result.result["accepted"], result.result
            assert runtime.step() is None
        assert runtime.workspace.identity.revision == 2
        canvas_status = runtime.channel_status()["symbolic_canvas"]
        assert canvas_status["status"] == "written" and canvas_status["turns"] == 2
        checkpoint = runtime.workspace.checkpoint(runtime.workspace.identity, ("count",))
        assert json.loads(checkpoint.values_json) == {"count": 2}
    finally:
        runtime.close()
    value["checkpoint_digest"] = checkpoint.digest
    restored = compose_foreground_document(value)
    restored.start()
    try:
        inbox.submit(session, "third", current.goal.digest, "continue restored state")
        assert restored.step().result["accepted"]
        assert restored.step() is None
        after = restored.workspace.checkpoint(restored.workspace.identity, ("count",))
        assert json.loads(after.values_json) == {"count": 3}
        assert restored.channel_status()["symbolic_canvas"]["turns"] == 3
    finally:
        restored.close()
    canvas_files = {str(path): path.read_bytes()
                    for path in (store.root / "symbolic-canvas-auto").rglob("*.*")}
    assert len(canvas_files) == 4  # Second and third terminal-turn pairs.
    value["symbolic_canvas"]["enabled"] = False
    value["checkpoint_digest"] = after.digest
    disabled = compose_foreground_document(value)
    disabled.start()
    try:
        assert disabled.canvas is None
        inbox.submit(session, "fourth", current.goal.digest, "continue with maps disabled")
        assert disabled.step().result["accepted"]
    finally:
        disabled.close()
    assert canvas_files == {str(path): path.read_bytes()
                            for path in (store.root / "symbolic-canvas-auto").rglob("*.*")}
    assert starts.read_text().splitlines() == ["parent", "child", "parent", "parent", "parent"]
    for operation in ("first", "second", "third", "fourth"):
        assert inbox.query(session, operation)["status"] == "terminal"
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0] == 0
