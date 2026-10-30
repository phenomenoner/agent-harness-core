"""Real foreground protocol calls use configured isolation across reopen/disable.

The protocol peer is deterministic. This does not claim actual model usefulness.
"""

import json
import sys

import pytest
from skill_test_support import goal
from test_foreground_config_native import native_config
from test_foreground_evaluation_config import evaluation_profile
from test_linux_native import native_owner
from test_linux_workspace_boundary import boundary

from ahc.cli import foreground_trusted
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.skill_repository import SkillRepository

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native foreground learning")
__all__ = ["native_owner", "boundary"]

PEER = r'''
import json,sys,uuid
p=json.loads(sys.argv[1]); expected=p['expected']; thread='peer'; turn=uuid.uuid4().hex
def send(v):print(json.dumps(v),flush=True)
def call(n,action,payload):
 send({'id':n,'method':'item/tool/call','params':{'threadId':thread,'turnId':turn,
  'namespace':'workspace','tool':'learning','callId':str(n),
  'arguments':{'expected':expected,'action':action,'payload':payload}}})
def evaluate():call(12,'evaluate',{'run_id':'configured-case','trial_id':trial,'code':p['code']})
for line in sys.stdin:
 m=json.loads(line); method=m.get('method')
 if method=='initialize':
  send({'id':1,'result':{'codexHome':'fixture','platformFamily':'unix',
   'platformOs':'linux','userAgent':'controlled-peer'}})
 elif method in ('thread/start','thread/resume'):
  v=m['params']; assert v.get('dynamicTools')
  send({'id':2,'result':{'cwd':v['cwd'],'model':v['model'],'modelProvider':'openai',
   'approvalPolicy':v['approvalPolicy'],'sandbox':{'type':'dangerFullAccess'},
   'thread':{'id':thread}}})
 elif method=='turn/start':
  send({'id':3,'result':{'turn':{'id':turn,'status':'inProgress'}}});call(10,'status',{})
 elif 'id' in m and m['id']>=10:
  result=json.loads(m['result']['contentItems'][0]['text']); n=m['id']
  if n==10:
   assert result['disposition']=='observed',result
   status=result['result'];assert not status['automatic_enabled'],status
   assert status['isolated_evaluation_available']==p['enabled'],status
   if status['trials']:
    trial=status['trials'][0]['trial_id'];evaluate()
   else:
    call(11,'propose',{'skill_name':'runtime-procedure','body':'# Procedure\nInspect evidence.',
     'description':'Inspect evidence before deciding.','source_ref':status['experiences'][0]['ref'],
     'cause':'observed gap','hypothesis':'May improve grounding'})
  elif n==11:
   assert result['disposition']=='observed',result;trial=result['result']['trial_id'];evaluate()
  elif n==12:
   if p['enabled']:
    assert result['disposition']=='observed' and result['result']['result']['ok'],result
    assert 'configured isolation' in result['result']['result']['output'],result
    observation=result;call(13,'evaluation_get',{'run_id':'configured-case'})
   else:
    assert result['disposition']=='rejected',result;call(14,'promote',{'trial_id':trial})
  elif n==13:
   assert result['disposition']=='observed',result
   assert result['result']=={k:v for k,v in observation['result'].items() if k!='source_ref'}
   call(14,'promote',{'trial_id':trial})
  elif n==14:
   assert result['disposition']=='rejected',result
   send({'id':15,'method':'item/tool/call','params':{'threadId':thread,'turnId':turn,
    'namespace':'workspace','tool':'execute','callId':'15',
    'arguments':{'expected':expected,'code':'print("parent continues")'}}})
  elif n==15:
   assert result['disposition']=='observed' and result['result']['ok'],result
   send({'method':'turn/completed','params':{'threadId':thread,'turn':{
    'id':turn,'status':'completed','error':None,'items':[{'type':'agentMessage',
    'id':'answer','phase':'final_answer','text':'evaluation lifecycle observed'}]}}})
'''


def test_configured_foreground_learning_reopen_and_disable(native_owner, boundary,
                                                          tmp_path, monkeypatch, skill_root):
    sandbox, policy = boundary
    store = RuntimeRepository.initialize(tmp_path / "runtime")
    skills = SkillRepository.create(skill_root)
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    current = store.create(WorkingSetSnapshot.initial(session, goal()))
    value = native_config()
    value.update(runtime_root=str(store.root), session=session.to_dict(),
                 skill_root=str(skills.root))
    value["workspace"].update(kind="linux-python", cwd=str(tmp_path), interpreter=sys.executable,
                              lifetime_mode="foreground")
    value["policy"].update(approval_mode="yolo", sandbox_mode="danger-full-access")
    profile = evaluation_profile()
    profile.update(executable=str(sandbox.executable), sha256=sandbox.sha256,
                   workspace=str(sandbox.workspace), protected_roots=[str(policy.parent)])
    value["learning"] = {"mode": "proposal-only", "protected_skills": ["pmo-policy"],
                         "evaluation": profile}
    code = ("from pathlib import Path\n"
            f"assert not Path({str(store.root)!r}).exists()\n"
            f"assert not Path({str(skills.root)!r}).exists()\n"
            f"assert not Path({str(policy)!r}).exists()\n"
            "assert candidate['skill_name']=='runtime-procedure'\n"
            "print('configured isolation')")

    class ControlledFactory(TrustedCodexBackendFactory):
        def __call__(self, request, tools, *, cancelled=lambda: False):
            self._command = (sys.executable, "-I", "-u", "-c", PEER, json.dumps({
                "expected": tools.workspace.identity.to_dict(), "code": code,
                "enabled": "evaluation" in self.config.to_dict()["learning"]}))
            return super().__call__(request, tools, cancelled=cancelled)

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", ControlledFactory)
    # Disabled standby must not compose an evaluator or launch a process.
    value["backend"]["enabled"] = False
    standby = compose_foreground_document(value)
    assert standby.learning is None
    value["backend"]["enabled"] = True
    inbox = RuntimeInbox(store)
    for operation in ("first", "reopened", "disabled"):
        if operation == "disabled":
            del value["learning"]["evaluation"]
        runtime = compose_foreground_document(value)
        runtime.start()
        try:
            inbox.submit(session, operation, current.goal.digest, "inspect relevant evidence")
            result = runtime.step()
            assert result is not None, runtime.admission_status()
            assert result.result["accepted"], json.dumps(result.result, ensure_ascii=False)
            assert runtime.step() is None
        finally:
            runtime.close()
        assert skills.get_active("runtime-procedure") is None
    with skills._read() as db:
        assert db.execute("SELECT count(*) FROM learning_evaluation_runs").fetchone()[0] == 1
        assert db.execute("SELECT state FROM learning_evaluation_runs").fetchone()[0] == "observed"
    with store._read() as db:
        rows = db.execute("SELECT generation,revision FROM workspace_heads "
                          "WHERE workspace_id LIKE 'eval-%'").fetchall()
        assert len(rows) == 1 and rows[0][0] == 1  # Reopen reads instead of dispatching again.
        held = db.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0]
        assert held == 0
