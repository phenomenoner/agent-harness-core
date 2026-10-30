import os
import sys
import threading
import time

import pytest

from ahc.adapters.mcp_stdio import McpStdioTransport
from ahc.application.mcp_client import McpClient
from ahc.domain.sessions import VirtualSessionIdentity

SERVER = """
import json,sys,time
for line in sys.stdin:
    msg=json.loads(line)
    if 'id' not in msg: continue
    method=msg['method']
    if method=='initialize':
        result={'protocolVersion':'2025-11-25','capabilities':{'tools':{}}}
    elif method=='tools/list':
        result={'tools':[{'name':'echo','inputSchema':{'type':'object'}}]}
    else:
        if sys.argv[1]=='exit': sys.exit(23)
        if sys.argv[1]=='stall': time.sleep(30)
        result={'content':[{'type':'text','text':msg['params']['arguments']['text']}]}
    identity=msg['id']+1 if method=='tools/call' and sys.argv[1]=='wrong' else msg['id']
    print(json.dumps({'jsonrpc':'2.0','id':identity,'result':result}),flush=True)
"""


@pytest.mark.skipif(os.name != "nt", reason="owned transport uses Windows Jobs")
@pytest.mark.parametrize('remaining', [5, 330])
def test_real_stdio_honors_admitted_long_turn_deadline(tmp_path, remaining):
    env = {key: os.environ[key] for key in ('SystemRoot', 'WINDIR') if key in os.environ}
    transport = McpStdioTransport.launch(
        (sys.executable, '-B', '-u', '-c', SERVER, 'echo'), cwd=tmp_path, env=env)
    session = VirtualSessionIdentity.initial('s', 'a', 'r')
    client = McpClient(session, 'fixture', transport, authorize=lambda *a: True,
                       validate=lambda *a: True)
    try:
        client.discover(deadline=time.monotonic() + remaining)
        result = client.invoke(session, 'long-turn', 'echo', {'text': 'qualified'},
                               deadline=time.monotonic() + remaining)
        assert result['status'] == 'observed'
    finally:
        client.close()
    assert transport.wire.owned.process.poll() is not None


@pytest.mark.skipif(os.name != "nt", reason="owned transport uses Windows Jobs")
@pytest.mark.parametrize("description_size,accepted", [(20000, True), (50000, False)])
def test_bounded_large_discovery_page_over_real_stdio(tmp_path, description_size, accepted):
    from ahc.domain.sessions import ContractError

    server = SERVER.replace(
        "result={'tools':[{'name':'echo','inputSchema':{'type':'object'}}]}",
        "result={'tools':[{'name':'tool'+str(i),'inputSchema':{'type':'object',"
        "'description':'x'*int(sys.argv[1])}} for i in range(12)]}")
    env = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
    transport = McpStdioTransport.launch(
        (sys.executable, "-u", "-c", server, str(description_size)), cwd=tmp_path, env=env)
    client = McpClient(VirtualSessionIdentity.initial("s", "a", "r"), "fixture", transport,
                       authorize=lambda *a: True, validate=lambda *a: True)
    try:
        if accepted:
            assert len(client.discover(deadline=time.monotonic() + 5)["tools"]) == 12
            # Larger discovery allowance does not widen ordinary result decoding.
            with pytest.raises(ContractError, match="payload limit"):
                client._json({"content": "x" * 65536})
        else:
            with pytest.raises(ContractError, match="discovery failed"):
                client.discover(deadline=time.monotonic() + 5)
    finally:
        client.close()
    assert transport.wire.owned.process.poll() is not None


@pytest.mark.skipif(os.name != "nt", reason="owned transport uses Windows Jobs")
@pytest.mark.parametrize("mode", ["echo", "stall", "wrong", "cancel", "exit"])
def test_actual_stdio_roundtrip_and_owned_cleanup(tmp_path, mode):
    env = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
    transport = McpStdioTransport.launch(
        (sys.executable, "-u", "-c", SERVER, "stall" if mode == "cancel" else mode),
        cwd=tmp_path, env=env)
    session = VirtualSessionIdentity.initial("s", "a", "r")
    client = McpClient(session, "fixture", transport, authorize=lambda *a: True,
                       validate=lambda schema, args: type(args.get("text")) is str)
    event = threading.Event()
    timer = None
    try:
        assert client.discover(deadline=time.monotonic() + 5)["tools"] == ["echo"]
        if mode == "cancel":
            timer = threading.Timer(0.1, event.set)
            timer.start()
        result = client.invoke(session, "call", "echo", {"text": "real-pipe"},
            deadline=time.monotonic() + (0.2 if mode == "stall" else 5),
            cancelled=event.is_set)
        if mode == "echo":
            assert result["status"] == "observed"
            assert result["result"]["content"][0]["text"] == "real-pipe"
        else:
            assert result["status"] == "unknown" and client.state == "degraded"
        if mode == "exit":
            from ahc.domain.sessions import ContractError

            assert transport.wire.owned.process.wait(timeout=2) == 23
            with pytest.raises(ContractError):
                client.invoke(session, "call", "echo", {"text": "real-pipe"},
                              deadline=time.monotonic() + 1)
    finally:
        if timer is not None:
            timer.cancel()
            timer.join()
        client.close()
    assert transport.wire.owned.process.poll() is not None
    cleanup = transport.wire.owned.cleanup(1)
    assert cleanup.complete and cleanup.remaining_count == 0
