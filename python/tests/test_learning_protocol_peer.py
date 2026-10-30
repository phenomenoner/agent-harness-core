"""The test peer respects publication versus read-only observation envelopes."""

import io
import json
import sys

import pytest
from test_native_foreground_learning import PEER


@pytest.mark.parametrize("mode", ["first", "reopened", "disabled"])
def test_peer_accepts_readback_without_requiring_publication_field(monkeypatch, mode):
    enabled = mode != "disabled"
    status = {"automatic_enabled": False, "isolated_evaluation_available": enabled,
              "trials": [] if mode == "first" else [{"trial_id": "trial"}],
              "experiences": [{"ref": "source"}]}
    observation = {"state": "observed", "result": {"ok": True,
                   "output": "configured isolation"}, "request": {"trial_id": "trial"}}

    def response(number, result):
        return {"id": number, "result": {"contentItems": [{"text": json.dumps(result)}]}}

    messages = [{"method": "initialize"}, {"method": "thread/start", "params": {
        "dynamicTools": ["learning"], "cwd": "/work", "model": "fixture",
        "approvalPolicy": "never"}}, {"method": "turn/start"},
        response(10, {"disposition": "observed", "result": status})]
    if mode == "first":
        messages.append(response(11, {"disposition": "observed", "result": {"trial_id": "trial"}}))
    messages.append(response(12, {"disposition": "observed" if enabled else "rejected",
                                 "result": {**observation, "source_ref": "published"}}))
    if enabled:
        messages.append(response(13, {"disposition": "observed", "result": observation}))
    messages.extend([response(14, {"disposition": "rejected"}),
                     response(15, {"disposition": "observed", "result": {"ok": True}})])
    output = io.StringIO()
    monkeypatch.setattr(sys, "argv", ["peer", json.dumps({"expected": {}, "enabled": enabled,
                                                        "code": "pass"})])
    monkeypatch.setattr(sys, "stdin", io.StringIO("\n".join(map(json.dumps, messages)) + "\n"))
    monkeypatch.setattr(sys, "stdout", output)
    exec(compile(PEER, "learning-peer", "exec"), {})
    frames = [json.loads(line) for line in output.getvalue().splitlines()]
    assert frames[-1]["method"] == "turn/completed"
    assert frames[-1]["params"]["turn"]["status"] == "completed"
