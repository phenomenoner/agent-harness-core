"""Rich host telemetry stays inside the existing child input contract."""
import json
from dataclasses import dataclass
from types import SimpleNamespace
import pytest
from ahc.cli.foreground_time_budget import bind_time_budget
from ahc.cli.host_model_calls import HostModelCalls
from ahc.domain.adaptive_time_budget import time_budget_policy
from ahc.domain.child_work import _task_text

@dataclass
class Request:
    deadline: float
    input_text: str
    time_budget: object = None

@pytest.mark.parametrize("task", ["normal task " * 400, '"\\\n' * 4096, "界" * 4096], ids=["ordinary", "escaped", "unicode"])
def test_rich_progress_is_admitted_and_preserves_comparison(monkeypatch, task):
    prompts = []
    def call(self, call_id, prompt, **kwargs):
        _task_text(prompt)  # The real ChildWorkEnvelope admission contract.
        prompts.append(prompt)
        return '{"continue":true,"reason":"native CPU and revision advanced"}'
    monkeypatch.setattr(HostModelCalls, "run", call)
    policy = time_budget_policy({"enabled": True})
    factory = SimpleNamespace(config=SimpleNamespace(backend={"time_budget": policy}))
    tools = SimpleNamespace(workspace=SimpleNamespace(
        identity=SimpleNamespace(revision=4, workspace_id="work")))
    bound = bind_time_budget(factory, Request(1, task), tools)
    old = {"active": True, "task": task, "workspace_revision": 3,
           "frames": 2, "tool_activity": True,
           "native_progress": {"start_ticks": 99, "cpu_ticks": 10},
           "native_owner": {"ancestors": [{"path": "x"*1000}] * 32,
                            "identity_verified": True, "pid": 12},
           "tokens": {"output_tokens": 3}}
    current = {**old, "workspace_revision": 4,
               "native_progress": {"start_ticks": 99, "cpu_ticks": 20},
               "tokens": {"output_tokens": 7}}
    assert bound.time_budget.judge({"current": current, "previous": old,
                                   "renewals": 1})["continue"]
    assert len(prompts[0]) <= 4096
    payload = json.loads(prompts[0][prompts[0].index("\n") + 1:])
    assert payload["current"]["native_progress"]["cpu_ticks"] == 20
    assert payload["previous"]["native_progress"]["cpu_ticks"] == 10
    assert payload["previous"]["workspace_revision"] == 3
    assert payload["current"]["workspace_revision"] == 4
    assert "ancestors" not in prompts[0]
