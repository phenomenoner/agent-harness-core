"""Real protected stdio effects through an admitted deterministic foreground.

The caller is an authored fixture; no vendor/model or real AAR server is implied.
"""

import json
import sys
from dataclasses import replace

import pytest
from test_foreground_backend_protection import protected_config
from test_foreground_runtime import setup
from test_linux_backend_boundary import protected
from test_linux_native import native_owner

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_mcp import compose_mcp
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.mcp_receipts import read_mcp_receipts

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="native namespace and cgroup")
__all__ = ["native_owner", "protected"]

SERVER = r'''
import json,sys,pathlib,os,time
p=json.loads(sys.argv[1])
for line in sys.stdin:
 m=json.loads(line)
 if 'id' not in m: continue
 method=m['method']
 if method=='initialize':
  result={'protocolVersion':'2025-11-25','capabilities':{'tools':{}}}
 elif method=='tools/list':
  result={'tools':[{'name':'canary','inputSchema':{'type':'object'}}]}
 else:
  assert not pathlib.Path(p['controls']).exists()
  assert not pathlib.Path(p['runtime']).exists()
  for path in [p['authority'],'/tmp/ahc-backend-home/config.toml',
               str(pathlib.Path.cwd()/'.codex/config.toml')]:
   try: pathlib.Path(path).write_text('forbidden')
   except OSError: pass
   else: raise AssertionError('protected write succeeded')
  marker=pathlib.Path('allowed-effect-count')
  marker.write_text(str(int(marker.read_text())+1) if marker.exists() else '1')
  child=os.fork()
  if child==0:
   os.setsid();time.sleep(60);os._exit(0)
  if p['mode']=='lost': os._exit(23)
  result={'content':[{'type':'text','text':'allowed computation; controls denied'}]}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
'''


@pytest.mark.parametrize("mode", ["observed", "lost"])
def test_configured_stdio_contains_effects_and_preserves_no_replay(
    native_owner, protected, mode
):
    boundary, authority = protected
    runtime, requests, _, request = setup(authority.parent)
    tools = [{"name": "canary", "inputSchema": {"type": "object"}}]
    capability = digest_json({"server_id": "fixture", "protocol": "2025-11-25",
                              "tools": tools})
    payload = {"mode": mode, "authority": str(authority),
               "controls": str(authority.parent), "runtime": str(runtime.store.root)}
    profile = McpStdioProfile("fixture", request.session.agent_id,
        ("/usr/bin/python3", "-I", "-u", "-c", SERVER, json.dumps(payload)),
        str(boundary.workspace), (), ("canary",), capability, True)
    value = protected_config()
    value.update(runtime_root=str(runtime.store.root), session=request.session.to_dict())
    value["workspace"].update(cwd=str(boundary.workspace), interpreter="/usr/bin/python3")
    value["backend"]["codex_home"] = str(boundary.state_root)
    for native in (value["workspace"]["protection"], value["backend"]["protection"]):
        native.update(executable=str(boundary.executable), sha256=boundary.sha256,
                      readonly_roots=["/usr"],
                      protected_roots=[str(authority.parent), str(runtime.store.root)])
    value["backend"]["protection"]["resource_limits"].update(
        memory_bytes=512 * 1024**2, pids=64, cpu_quota_us=100000)
    value["mcp"] = {"profiles": [profile.to_dict()], "credential_environment": {}}
    config = parse_foreground_config_native(value)
    runtime.mcp = compose_mcp(config, runtime.store, cancelled=lambda: False)
    outcomes = []

    def factory(bound, workspace_tools, *, cancelled):
        class Backend:
            def invoke(self, admitted):
                arguments = {"expected": workspace_tools.workspace.identity.to_dict(),
                    "server_id": "fixture", "tool": "canary", "call_id": "owned-effect",
                    "arguments": {}}
                outcomes.append(workspace_tools.invoke("workspace.mcp", arguments))
                outcomes.append(workspace_tools.invoke("workspace.mcp", arguments))
                return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                            "thread", "turn", "fixture complete")
        return Backend()

    runtime.backend_factory = factory
    runtime.start()
    try:
        requests.append(replace(request, capabilities=tuple(runtime.tools.capabilities())))
        assert runtime.step().result["accepted"]
        assert outcomes[0]["disposition"] == ("observed" if mode == "observed" else "unknown")
        assert outcomes[0]["unresolved_effects"] is (mode == "lost")
        assert outcomes[1]["disposition"] == "rejected"
        rows = read_mcp_receipts(runtime.store, request.session, request.operation_id)
        assert len(rows) == 1 and rows[0]["status"] == (
            "observed" if mode == "observed" else "unknown")
    finally:
        runtime.close()
    assert (boundary.workspace / "allowed-effect-count").read_text() == "1"
    assert authority.read_text() == "unchanged-authority"
