"""Native staged-cache containment and pinned vendor loading; synthetic auth only."""

import base64
import json
import os
import select
import sys
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest
from test_linux_backend_boundary import protected as _protected
from test_linux_backend_boundary import runner_for
from test_linux_native import native_owner as _native_owner

from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.adapters.trusted_codex_launch import trusted_codex_command
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.linux_backend_boundary import (
    BACKEND_HOME,
)

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native credential mount")
native_owner = _native_owner
protected = _protected


def stage_fake_auth(boundary):
    def jwt(claims):
        body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return "e30." + body + ".fixture"

    now = datetime.now(UTC)
    token = jwt({"email": "fixture@example.invalid", "exp": int(now.timestamp()) + 3600,
                 "https://api.openai.com/auth": {"chatgpt_plan_type": "pro"}})
    value = {"auth_mode": "chatgpt", "OPENAI_API_KEY": None,
             "tokens": {"id_token": token, "access_token": token,
                        "refresh_token": "SYNTHETIC_NO_REFRESH", "account_id": "fixture"},
             "last_refresh": now.isoformat()}
    boundary.state_root.chmod(0o700)
    path = boundary.state_root / "auth.json"
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return path


def test_only_model_sees_one_writable_cache_and_config_remains_sealed(native_owner, protected):
    boundary, sentinel = protected
    auth = stage_fake_auth(boundary)
    original = auth.read_bytes()
    boundary.staged_authentication = True
    code = f"""import pathlib,json,os
home=pathlib.Path({BACKEND_HOME!r});auth=home/'auth.json'
value=json.loads(auth.read_text());assert value['tokens']['account_id']=='fixture'
value['last_refresh']='synthetic-storage-write'
with auth.open('w') as output: json.dump(value,output)
assert not pathlib.Path({str(auth)!r}).exists()
assert not pathlib.Path({str(sentinel)!r}).exists()
for name in ('config.toml','hooks.json','plugins/new-config'):
 try: (home/name).write_text('unsafe')
 except OSError: pass
 else: raise AssertionError('configuration escaped')
print('AUTH_STORAGE_AND_CONFIGURATION_PASS',flush=True)
"""
    runner = runner_for(boundary)
    command = ("/usr/bin/python3", "-I", "-u", "-c", code)
    attempt = runner.prepare_creation(command, lifetime_seconds=20)
    try:
        runner.create_attempt(attempt, command, lifetime_seconds=20)
        assert select.select([runner._owned.process.stdout], [], [], 15)[0]
        assert (runner._owned.process.stdout.readline().strip()
                == b"AUTH_STORAGE_AND_CONFIGURATION_PASS")
        assert runner._owned.process.wait(timeout=5) == 0
    finally:
        runner.close()
    assert runner.observe(attempt.identity).state == "absent"
    assert auth.read_bytes() != original
    assert sentinel.read_text() == "unchanged-authority"
    boundary.staged_authentication = False
    runner = runner_for(boundary)
    code = f"import pathlib;assert not pathlib.Path({BACKEND_HOME + '/auth.json'!r}).exists()"
    command = ("/usr/bin/python3", "-I", "-c", code)
    attempt = runner.prepare_creation(command, lifetime_seconds=15)
    try:
        runner.create_attempt(attempt, command, lifetime_seconds=15)
        assert runner._owned.process.wait(timeout=10) == 0
    finally:
        runner.close()
    assert runner.observe(attempt.identity).state == "absent"


@pytest.mark.parametrize("case", ["permissions", "hardlink", "oversized", "parent-permissions"])
def test_invalid_cache_refuses_before_owned_process_creation(native_owner, protected, case):
    boundary, _ = protected
    auth = stage_fake_auth(boundary)
    if case == "permissions":
        auth.chmod(0o644)
    elif case == "hardlink":
        os.link(auth, boundary.state_root / "alias")
    elif case == "oversized":
        auth.write_bytes(b"x" * 16385)
    else:
        boundary.state_root.chmod(0o755)
    boundary.staged_authentication = True
    runner = runner_for(boundary)
    try:
        with pytest.raises(ContractError, match="authentication"):
            runner.prepare_creation(("/usr/bin/python3", "-c", "pass"), lifetime_seconds=15)
        assert runner._group is None
    finally:
        runner.close()


def test_actual_vendor_loads_synthetic_managed_cache_without_refresh(native_owner, protected):
    boundary, sentinel = protected
    auth = stage_fake_auth(boundary)
    initial = auth.read_bytes()
    bundle = Path(os.environ["AHC_TEST_CODEX_BUNDLE"]).resolve()
    boundary.readonly_roots = (*boundary.readonly_roots, bundle)
    boundary.staged_authentication = True
    command = trusted_codex_command(bundle=str(bundle), codex_home=BACKEND_HOME,
                                   cwd=str(boundary.workspace), interpreter="/usr/bin/python3",
                                   approval_policy="never", sealed_history=True,
                                   staged_authentication=True)
    runner = runner_for(boundary)
    attempt = runner.prepare_creation(command, lifetime_seconds=75)
    wire = None
    try:
        runner.create_attempt(attempt, command, lifetime_seconds=75)
        wire = CodexStdioTransport(runner._owned, streaming=True, max_wait=60)
        deadline = time.monotonic() + 55

        def rpc(identity, method, params):
            wire.send({"id": identity, "method": method, "params": params}, deadline=deadline)
            for _ in range(128):
                frame = wire.receive(deadline=deadline)
                if frame.get("id") == identity:
                    assert "result" in frame, "synthetic vendor RPC rejected"
                    return frame["result"]
            pytest.fail("synthetic RPC frame bound")

        initialized = rpc(1, "initialize", {
            "clientInfo": {"name": "ahc_auth_gate", "version": "1.0"},
            "capabilities": {"experimentalApi": True}})
        assert initialized["codexHome"] == BACKEND_HOME
        wire.send({"method": "initialized", "params": {}}, deadline=deadline)
        account = rpc(10, "account/read", {"refreshToken": False})
        assert account["account"]["type"] == "chatgpt"
        assert account["account"]["planType"] == "pro"
        config = rpc(11, "config/read", {"includeLayers": False})["config"]
        assert not config.get("mcp_servers")
        assert config["cli_auth_credentials_store"] == "file"
        assert config["features"]["shell_tool"] is False
        assert config["features"]["code_mode"] is False
    finally:
        if wire is not None:
            wire.close()
        runner.close()
    assert runner.observe(attempt.identity).state == "absent"
    assert auth.read_bytes() == initial
    assert sentinel.read_text() == "unchanged-authority"


def test_pinned_authenticated_launch_exposes_no_native_execution_tools(native_owner, protected):
    boundary, _ = protected
    stage_fake_auth(boundary)
    tool_specs = []
    submitted_inputs = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            assert 0 < size <= 2 * 1024**2
            value = json.loads(self.rfile.read(size))
            tool_specs.append(value.get("tools", []))
            submitted_inputs.append(value.get("input", []))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            item = {"id": "msg_fixture", "type": "message", "role": "assistant",
                    "status": "completed", "content": [{"type": "output_text",
                    "text": "synthetic local fixture", "annotations": []}]}
            output = [item]
            if len(tool_specs) == 1:
                calls = [
                    ("list_orchestrator", "list", {"authority": {"kind": "orchestrator"}}),
                    ("list_executor", "list", {"authority": {"kind": "executor"}}),
                    ("read_auth", "read", {"package": BACKEND_HOME + "/auth.json",
                                           "resource": BACKEND_HOME + "/auth.json"}),
                ]
                output = [{"id": "fc_" + identity, "type": "function_call",
                           "call_id": identity, "namespace": "skills", "name": name,
                           "arguments": json.dumps(arguments)}
                          for identity, name, arguments in calls]
            response = {"id": "resp_fixture", "object": "response", "status": "completed",
                        "output": output, "usage": {"input_tokens": 1, "output_tokens": 1,
                        "total_tokens": 2, "input_tokens_details": {"cached_tokens": 0}}}
            events = [
                {"type": "response.created", "response": {
                    **response, "status": "in_progress", "output": []}},
            ]
            events.extend({"type": "response.output_item.done", "output_index": i,
                           "item": entry} for i, entry in enumerate(output))
            events.append({"type": "response.completed", "response": response})
            for event in events:
                self.wfile.write(("event: " + event["type"] + "\ndata: "
                                  + json.dumps(event) + "\n\n").encode())
            self.wfile.flush()
            self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    bundle = Path(os.environ["AHC_TEST_CODEX_BUNDLE"]).resolve()
    boundary.readonly_roots = (*boundary.readonly_roots, bundle)
    boundary.staged_authentication = True
    command = trusted_codex_command(bundle=str(bundle), codex_home=BACKEND_HOME,
                                   cwd=str(boundary.workspace), interpreter="/usr/bin/python3",
                                   approval_policy="never", sealed_history=True,
                                   staged_authentication=True)
    runner = runner_for(boundary)
    attempt = runner.prepare_creation(command, lifetime_seconds=90)
    wire = None
    try:
        runner.create_attempt(attempt, command, lifetime_seconds=90)
        wire = CodexStdioTransport(runner._owned, streaming=True, max_wait=80)
        deadline = time.monotonic() + 75

        def rpc(identity, method, params):
            wire.send({"id": identity, "method": method, "params": params}, deadline=deadline)
            for _ in range(128):
                frame = wire.receive(deadline=deadline)
                if frame.get("id") == identity:
                    assert "result" in frame, "local synthetic RPC rejected"
                    return frame["result"]
            pytest.fail("local synthetic RPC frame bound")

        rpc(1, "initialize", {"clientInfo": {"name": "ahc_auth_surface", "version": "1.0"},
                              "capabilities": {"experimentalApi": True}})
        wire.send({"method": "initialized", "params": {}}, deadline=deadline)
        config = {"model_providers.local_fixture.name": "Synthetic local provider",
                  "model_providers.local_fixture.base_url":
                      f"http://127.0.0.1:{server.server_port}/v1",
                  "model_providers.local_fixture.wire_api": "responses",
                  "model_providers.local_fixture.requires_openai_auth": False}
        started = rpc(2, "thread/start", {
            "cwd": str(boundary.workspace), "model": "fixture", "modelProvider": "local_fixture",
            "approvalPolicy": "never", "sandbox": "read-only", "environments": [],
            "config": config, "dynamicTools": [{"name": "ahc_auth_probe",
                "description": "A harness-owned synthetic observation tool",
                "inputSchema": {"type": "object", "properties": {},
                                "additionalProperties": False}}]})
        rpc(3, "turn/start", {"threadId": started["thread"]["id"], "environments": [],
                              "input": [{"type": "text", "text": "Return synthetic fixture."}]})
        for _ in range(256):
            frame = wire.receive(deadline=deadline)
            if frame.get("method") == "turn/completed":
                assert frame["params"]["turn"]["status"] == "completed"
                break
        else:
            pytest.fail("synthetic completion bound")
        assert len(tool_specs) == 2, "bounded local synthetic requests"
        for specifications in tool_specs:
            by_name = {tool.get("name", tool.get("type")): tool for tool in specifications}
            assert set(by_name) == {"ahc_auth_probe", "request_user_input", "skills"}, by_name
            assert {tool["name"] for tool in by_name["skills"]["tools"]} == {"list", "read"}
        outputs = {item["call_id"]: item["output"] for item in submitted_inputs[-1]
                   if item.get("type") == "function_call_output"}
        assert set(outputs) == {"list_orchestrator", "list_executor", "read_auth"}, outputs
        for identity in ("list_orchestrator", "list_executor"):
            rendered = json.dumps(outputs[identity]).replace('\\"', '"')
            assert '"skills":[]' in rendered.replace(" ", ""), rendered
        rejected = json.dumps(outputs["read_auth"])
        assert "skill package is not available" in rejected, rejected
        assert "SYNTHETIC_NO_REFRESH" not in json.dumps(submitted_inputs)
    finally:
        if wire is not None:
            wire.close()
        runner.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert runner.observe(attempt.identity).state == "absent"
    assert not thread.is_alive()
