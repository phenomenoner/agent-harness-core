"""Real protected Python engines and trial recovery, with an inert protocol peer."""
import hashlib
import json
import os
from pathlib import Path
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
from ahc.infrastructure.skill_repository import SkillRepository

pytestmark=pytest.mark.skipif(sys.platform!='linux',reason='native protected learning')
native_owner=_native_owner

PEER=r'''
import json,sys,uuid
from pathlib import Path
p=json.loads(sys.argv[1]);expected=p['expected'];thread='peer';turn=uuid.uuid4().hex
assert not Path(p['sentinel']).exists()
try:Path(p['sentinel']).write_text('escape')
except OSError:pass
else:raise AssertionError('backend escaped')
def send(v):print(json.dumps(v),flush=True)
def call(n,tool,args):
 send({'id':n,'method':'item/tool/call','params':{'threadId':thread,'turnId':turn,
  'namespace':'workspace','tool':tool,'callId':str(n),'arguments':dict(expected=expected,**args)}})
def learning(n,action,payload):call(n,'learning',dict(action=action,payload=payload))
def execute():call(15,'execute',{'code':p['code']})
for line in sys.stdin:
 m=json.loads(line);method=m.get('method')
 if method=='initialize':send({'id':1,'result':{'codexHome':'fixture','platformFamily':'unix',
  'platformOs':'linux','userAgent':'controlled-peer'}})
 elif method in ('thread/start','thread/resume'):
  v=m['params'];send({'id':2,'result':{'cwd':v['cwd'],'model':v['model'],'modelProvider':'openai',
   'approvalPolicy':v['approvalPolicy'],'sandbox':{'type':'dangerFullAccess'},'thread':{'id':thread}}})
 elif method=='turn/start':
  send({'id':3,'result':{'turn':{'id':turn,'status':'inProgress'}}});learning(10,'status',{})
 elif 'id' in m and m['id']>=10:
  n=m['id'];r=json.loads(m['result']['contentItems'][0]['text'])
  if n==10:
   assert r['disposition']=='observed',r;s=r['result']
   assert s['automatic_enabled']==(p['operation']!='stopped'),s
   if p['operation']=='first':
    learning(11,'propose',{'skill_name':'runtime-wording','body':'# Procedure\nUse ACTIVE wording.',
      'description':'Use ACTIVE wording for this bounded fixture.','source_ref':s['experiences'][0]['ref'],
      'cause':'user terminology','hypothesis':'Follow the user terminology'})
   elif p['operation']=='reopened':
    trial=s['trials'][0]['trial_id'];call(13,'skills_read',{'revision_digest':s['trials'][0]['candidate']})
   else:
    trial=next(t['trial_id'] for t in s['trials'] if t['skill_name']=='runtime-wording')
    learning(14,'rollback',{'trial_id':trial})
  elif n==11:
   assert r['disposition']=='observed',r;trial=r['result']['trial_id']
   learning(12,'promote',{'trial_id':trial})
  elif n==12:
   assert r['disposition']=='observed',r;execute()
  elif n==13:
   assert r['disposition']=='observed' and 'ACTIVE' in r['result']['text'],r;execute()
  elif n==14:
   assert r['disposition']=='observed',r;execute()
  elif n==15:
   assert r['disposition']=='observed' and r['result']['ok'],r
   assert 'workspace-protected' in r['result']['output'],r
   send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,
    'status':'completed','error':None,'items':[{'type':'agentMessage','id':'answer',
    'phase':'final_answer','text':'protected learning lifecycle observed'}]}}})
'''


@pytest.mark.parametrize('engine',['linux-python','linux-ipython'])
@pytest.mark.parametrize('nested_controls',[False,True])
def test_protected_revision_uptake_reopen_stop_and_rollback(native_owner,tmp_path,monkeypatch,engine,nested_controls,skill_root):
    if engine == "linux-ipython" and not os.environ.get("AHC_TEST_IPYTHON_PYTHON"):
        import importlib.util
        if importlib.util.find_spec("IPython") is None:
            pytest.skip("IPython test interpreter is not configured")
    store=RuntimeRepository.initialize(tmp_path/'runtime')
    skills=SkillRepository.create(skill_root)
    session=VirtualSessionIdentity.initial('session','agent','route')
    current=store.create(WorkingSetSnapshot.initial(session,goal()))
    work=tmp_path/'work';work.mkdir()
    sentinel=store.root/'authority';sentinel.write_text('unchanged')
    hidden=work/'.ahc'
    home=hidden/'codex-home' if nested_controls else tmp_path/'backend-home'
    home.mkdir(parents=True)
    if nested_controls:
        sentinel=hidden/'authority';sentinel.write_text('unchanged')
    for name in ('sessions','archived_sessions','state'):(home/name).mkdir()
    bwrap=Path(os.environ['AHC_TEST_BWRAP']).resolve()
    interpreter=os.environ.get('AHC_TEST_IPYTHON_PYTHON',sys.executable) if engine=='linux-ipython' else sys.executable
    roots=sorted({'/usr',str(Path(sys.executable).resolve().parent.parent),
        str(Path(sys.prefix).resolve()),str(Path(interpreter).resolve().parent.parent),
        str(Path(interpreter).parent.parent.resolve())})
    profile={'executable':str(bwrap),'sha256':hashlib.sha256(bwrap.read_bytes()).hexdigest(),
        'readonly_roots':roots,'protected_roots':[str(store.root),str(skills.root)]+(
            [str(hidden)] if nested_controls else []),
        'resource_limits':{'memory_bytes':512*1024**2,'pids':64,'cpu_quota_us':100000,
            'cpu_period_us':100000,'swap_bytes':0}}
    value=native_config()
    value.update(runtime_root=str(store.root),session=session.to_dict(),skill_root=str(skills.root))
    value['workspace'].update(kind=engine,cwd=str(work),interpreter=interpreter,
        lifetime_mode='foreground',protection=profile)
    value['backend'].update(enabled=True,codex_home=str(home),protection=profile | {'network_mode':'shared',
        'home_mode':'sealed-history','home_bytes':8*1024**2,'temporary_bytes':8*1024**2})
    value['policy'].update(approval_mode='yolo',sandbox_mode='danger-full-access')
    value['learning']={'mode':'reversible-trials','protected_skills':['pmo-policy'],'daily_limit':2}
    code=f"from pathlib import Path\nassert not Path({str(sentinel)!r}).exists()\nprint('workspace-protected')"

    class ControlledFactory(TrustedCodexBackendFactory):
        def __call__(self,request,tools,*,cancelled=lambda:False):
            self._command=(sys.executable,'-I','-u','-c',PEER,json.dumps({
                'expected':tools.workspace.identity.to_dict(),'operation':request.operation_id,
                'sentinel':str(sentinel),'code':code}))
            return super().__call__(request,tools,cancelled=cancelled)

    monkeypatch.setattr(foreground_trusted,'TrustedCodexBackendFactory',ControlledFactory)
    inbox=RuntimeInbox(store)
    for operation in ('first','reopened','stopped'):
        if operation=='reopened':
            # An active revision changes availability for the next generation.
            # Restarting the same generation must preserve its original pins.
            current=store.read_current(session.ref.virtual_session_id)
            session=session.successor()
            current=store.append(current,current.continue_as(session,'backend-rollover'))
            value['session']=session.to_dict()
        runtime=compose_foreground_document(value)
        if operation=='stopped':
            runtime.learning.experiments.configure(enabled=False,daily_limit=2)
            runtime.close()
            runtime=compose_foreground_document(value)
        runtime.start()
        try:
            inbox.submit(session,operation,current.goal.digest,'use the requested wording')
            turn=runtime.step()
            assert turn is not None, runtime.workspace_status()
            assert turn.result['accepted'],turn.result
            if operation=='first':assert skills.get_active('runtime-wording') is not None
            if operation=='reopened':assert runtime.learning._trials({})['trials'][0]['state']=='active'
        finally:runtime.close()
    assert skills.get_active('runtime-wording') is None
    assert sentinel.read_text()=='unchanged'
    with skills._read() as db:
        assert db.execute('SELECT enabled FROM learning_control').fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM learning_experiences WHERE kind='task-result'").fetchone()[0]==3
        assert db.execute('SELECT state FROM learning_trials').fetchone()[0]=='rolled-back'
    with store._read() as db:
        assert db.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0]==0
