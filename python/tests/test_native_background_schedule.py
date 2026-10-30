"""Real native workspaces and app-server peers overlap without any platform send."""

import json
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from skill_test_support import goal
from test_foreground_config_native import native_config
from test_linux_native import native_owner

from ahc.cli import foreground_trusted
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.schedule_tick import ScheduleTick

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native lifecycle")

PEER = r'''
import json,sys,pathlib,time,uuid
p=json.loads(sys.argv[1]); thread=uuid.uuid4().hex; turn=uuid.uuid4().hex
def send(v): print(json.dumps(v),flush=True)
for line in sys.stdin:
 m=json.loads(line); method=m.get('method')
 if method=='initialize':
  send({'id':1,'result':{'codexHome':'fixture','platformFamily':'unix','platformOs':'linux','userAgent':'fixture'}})
 elif method in ('thread/start','thread/resume'):
  v=m['params']; assert v['approvalPolicy']=='never' and v['sandbox']=='danger-full-access'
  send({'id':2,'result':{'cwd':v['cwd'],'model':v['model'],'modelProvider':'openai','approvalPolicy':v['approvalPolicy'],'sandbox':{'type':'dangerFullAccess'},'thread':{'id':thread}}})
 elif method=='turn/start':
  assert m['params']['effort']=='low'
  send({'id':3,'result':{'turn':{'id':turn,'status':'inProgress'}}})
  send({'id':7,'method':'item/tool/call','params':{'threadId':thread,'turnId':turn,'namespace':'workspace','tool':'execute','callId':'call7','arguments':{'expected':p['expected'],'code':"count=globals().get('count',0)+1;print(count)"}}})
 elif m.get('id')==7:
  assert m['result']['success'],m
  result=json.loads(m['result']['contentItems'][0]['text']); assert result['disposition']=='observed',result
  if p['background']:
   pathlib.Path(p['entered']).write_text('ready')
   end=time.monotonic()+35
   while not pathlib.Path(p['release']).exists() and time.monotonic()<end: time.sleep(.01)
   assert pathlib.Path(p['release']).exists()
  send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed','error':None,'items':[{'type':'agentMessage','id':'answer','phase':'final_answer','text':'background done' if p['background'] else 'foreground done'}]}}})
'''


def test_native_blocked_schedule_foreground_new_and_next_turn(native_owner, tmp_path, monkeypatch):
    from ahc.cli.foreground_channels import BotTextTransport

    store = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    current = store.create(WorkingSetSnapshot.initial(session, goal()))
    route = ChannelRoute("telegram", "1", "2", "3", session)
    value = native_config()
    value.update(runtime_root=str(store.root), session=session.to_dict(),
                 execution_profile="user-full-access")
    value["workspace"].update(kind="linux-python", cwd=str(tmp_path), interpreter=sys.executable,
                              lifetime_mode="foreground")
    value["backend"].update(enabled=True, model="gpt-test", reasoning_effort="low")
    value["policy"].update(approval_mode="yolo", sandbox_mode="danger-full-access")
    value["channels"] = [{"route": route.to_dict(), "token_env": "AHC_SCHEDULE_FIXTURE", "enabled": True}]
    value["channel_control"] = {"enabled": True,
        "models": {"gpt-test": {"efforts": ["low"], "priority": True}}, "aliases": {}}
    monkeypatch.setenv("AHC_SCHEDULE_FIXTURE", "fixture")
    entered, release = tmp_path / "entered", tmp_path / "release"
    arrived, sent = threading.Event(), []

    class ControlledFactory(TrustedCodexBackendFactory):
        def __call__(self, request, tools, *, cancelled=lambda: False):
            assert self.config.to_dict()["execution_profile"] == "user-full-access"
            assert self.config.effective_policy.approval_mode == "yolo"
            self._command = (sys.executable, "-I", "-u", "-c", PEER, json.dumps({
                "expected": tools.workspace.identity.to_dict(), "background": request.session != session,
                "entered": str(entered), "release": str(release)}))
            return super().__call__(request, tools, cancelled=cancelled)

    def send(self, destination, payload, operation):
        assert (destination.platform, destination.account_id, destination.channel_id, destination.user_id) == (
            route.platform, route.account_id, route.channel_id, route.user_id)
        sent.append(payload["output_text"])
        if payload["output_text"] == "⏰ **定時任務**\n\nbackground done":
            arrived.set()
        return "controlled-receipt"

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", ControlledFactory)
    monkeypatch.setattr(BotTextTransport, "send", send)
    runtime = compose_foreground_document(value)
    inbox = RuntimeInbox(runtime.store)
    def submit(text, event_id):
        event = ChannelInboundEvent(route, event_id, text)
        inbox.submit(session, event.operation_id, current.goal.digest, text, channel_event=event)
        return event

    submit("/cron add every 10m background fixture", "schedule")
    with runtime.store._read() as c:
        schedule_id = c.execute("SELECT schedule_id FROM runtime_schedules").fetchone()[0]
    schedule = ScheduleRepository(runtime.store).read(session, schedule_id)
    ScheduleTick(runtime.store).tick(schedule, schedule.start_ms + schedule.interval_ms, limit=1)
    # Observe peer admission with a bounded event waiter, without calling the main loop.
    def observe():
        end = time.monotonic() + 15
        while time.monotonic() < end:
            if entered.exists():
                arrived.set()
                return
            threading.Event().wait(.02)
    observer = threading.Thread(target=observe, daemon=True)
    runtime.start()
    observer.start()
    try:
        assert arrived.wait(16), runtime.schedule_status()
        arrived.clear()
        assert runtime.schedule_status()["running"] == 1
        first = submit("answer foreground", "front-one")
        assert runtime.step().result["accepted"], runtime.channel_status()
        assert not release.exists()
        reset = submit("/new", "new-while-scheduled")
        with runtime.store._read() as c:
            assert "已開啟新的對話" in c.execute("SELECT reply FROM channel_controls WHERE operation_id=?",
                (reset.operation_id,)).fetchone()[0]
        release.write_text("complete")
        assert arrived.wait(15), runtime.schedule_status()
        following = submit("continue foreground", "front-two")
        assert runtime.step().result["accepted"]
        assert runtime.workspace.identity.revision == 2
        assert len([text for text in sent if text.startswith("⏰ **定時任務**")]) == 1
    finally:
        release.write_text("complete")
        runtime.close()
        observer.join(timeout=1)
    with runtime.store._read() as c:
        assert c.execute("SELECT count(*) FROM backend_custody WHERE state='held'").fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM runtime_background_dispatch").fetchone()[0] == 1
