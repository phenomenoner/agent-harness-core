"""Actual optional WebSocket library; only the endpoint is redirected to loopback."""

import json
import queue
import threading

import pytest

from ahc.adapters.discord_websocket import DiscordWebSocket
from ahc.domain.sessions import ContractError


def test_real_library_framing_and_clean_close(monkeypatch):
    pytest.importorskip("websockets")
    from websockets.exceptions import ConnectionClosed
    from websockets.sync import client
    from websockets.sync.server import serve

    observed = queue.Queue()

    def handler(connection):
        try:
            observed.put(("path", connection.request.path))
            connection.send('{"op":10,"d":{"heartbeat_interval":45000}}')
            observed.put(("frame", json.loads(connection.recv(timeout=2))))
            connection.send('{"op":11}')
            connection.send('{"op":0,"op":1}')
            try:
                connection.recv(timeout=2)
            except ConnectionClosed:
                pass
            observed.put(("closed", connection.close_code))
        except Exception as error:
            observed.put(("error", type(error).__name__))

    real_connect = client.connect
    with serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.socket.getsockname()[1]
        runner = threading.Thread(target=server.serve_forever, daemon=True)
        runner.start()

        def redirect(url, **options):
            assert url == "wss://gateway.discord.gg/?v=10&encoding=json"
            assert options["proxy"] is None and options["compression"] is None
            assert options["ping_interval"] is None
            assert options["max_size"] == 1048576 and options["max_queue"] == 16
            return real_connect(f"ws://127.0.0.1:{port}/?v=10&encoding=json", **options)

        monkeypatch.setattr(client, "connect", redirect)
        peer = DiscordWebSocket()
        try:
            assert peer.receive(timeout=2)["op"] == 10
            peer.send({"op": 1, "d": None})
            assert peer.receive(timeout=2) == {"op": 11}
            with pytest.raises(ContractError, match="incoming frame invalid"):
                peer.receive(timeout=2)
            assert observed.get(timeout=2) == ("path", "/?v=10&encoding=json")
            assert observed.get(timeout=2) == ("frame", {"op": 1, "d": None})
            assert observed.get(timeout=2) == ("closed", 4000)
        finally:
            peer.close()
            server.shutdown()
            runner.join(timeout=2)
        assert not runner.is_alive()
