"""Review-derived failures at blocking send and borrowed read budgets."""
import io
import threading
import time
from types import SimpleNamespace
from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.application.adaptive_time_budget import AdaptiveTimeBudget
from ahc.domain.adaptive_time_budget import time_budget_policy

def test_blocked_original_write_renews_once_without_close_or_resend():
    release, calls, writes, closed = threading.Event(), [], [], []
    class Input:
        def write(self, value):
            writes.append(value)
            assert release.wait(2)
        def flush(self):
            pass
    wire = object.__new__(CodexStdioTransport)
    wire._closed = False
    wire._max_wait = None
    wire._write_lock, wire._metrics_lock = threading.Lock(), threading.Lock()
    wire._metrics = {"write_attempts": 0, "write_returns": 0, "stdin_bytes": 0}
    wire.owned = SimpleNamespace(process=SimpleNamespace(stdin=Input()))
    wire.close = lambda: closed.append(True)
    budget = AdaptiveTimeBudget(time.monotonic() + .06,
        time_budget_policy({"enabled": True}),
        lambda value: calls.append(value) or {"continue": True, "reason": "write is making progress"},
        lambda: {"active": True})
    timer = threading.Timer(.18, release.set)
    timer.start()
    try:
        wire.send({"result": "once"}, deadline=budget.current_deadline)
    finally:
        release.set()
        timer.join()
    assert len(writes) == len(calls) == 1
    assert closed == []

def test_read_worker_borrows_renewed_deadline(monkeypatch):
    from ahc.adapters import memory_read_budget as module
    now, calls = [10], []
    budget = AdaptiveTimeBudget(10, time_budget_policy({"enabled": True}),
        lambda value: calls.append(value) or {"continue": True, "reason": "continued"},
        lambda: {"active": True}, clock=lambda: now[0])
    class Process:
        stdin = stdout = stderr = None
        returncode = 0
        def communicate(self, **kwargs):
            return b"ok", b""
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: Process())
    with module.memory_read_budget(budget.current_deadline, clock=lambda: now[0]):
        result = module.run_read_worker(["read"], timeout=10, input=b"{}", capture_output=True)
    assert result.stdout == b"ok"
    assert len(calls) == 1


import pytest
@pytest.mark.parametrize("stdio,extension,cap", [(False, 1800, 300), (True,7200,3600)])
def test_new_mcp_after_renewal_keeps_transport_operation_cap(monkeypatch,tmp_path,stdio,extension,cap):
    from ahc.infrastructure import runtime_mcp_tools as module
    from ahc.domain.mcp_profiles import McpStdioProfile
    from ahc.domain.mcp_http_profiles import McpHttpProfile
    from ahc.domain.sessions import VirtualSessionIdentity
    from ahc.domain.workspace import WorkspaceIdentity
    session=VirtualSessionIdentity.initial("session","agent","route")
    identity=WorkspaceIdentity(session,"work",1,0)
    request=SimpleNamespace(session=session,effective_deadline=time.monotonic()+extension,
                            deadline=time.monotonic()-10)
    if stdio:
        profile=McpStdioProfile("server","agent",("/usr/bin/python3",),str(tmp_path),(),("echo",),"a"*64,True)
    else:
        profile=McpHttpProfile("server","agent","https://example.com/mcp",None,("echo",),"a"*64,True)
    deadlines=[]
    class Client:
        def invoke(self,*a,deadline,**kw):
            deadlines.append(deadline)
            assert 0 < deadline-time.monotonic() <= cap
            return {"status":"observed","result":{}}
        def close(self):pass
    def connect(*a,deadline,**kw):
        deadlines.append(deadline)
        assert 0 < deadline-time.monotonic() <= cap
        return Client(), None
    monkeypatch.setattr(module,"workspace_request",lambda *a:request)
    monkeypatch.setattr(module,"connect_mcp_profile",connect)
    monkeypatch.setattr(module,"connect_mcp_http_profile",connect)
    result=module.RuntimeMcpTools(object(),(profile,)).invoke(identity,
        {"expected":identity.to_dict(),"server_id":"server","tool":"echo","call_id":"one","arguments":{}})
    assert result["disposition"] == "observed"
    assert len(deadlines)==2 and deadlines[0]==deadlines[1]
