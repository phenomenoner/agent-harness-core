"""Kernel-level discriminator for exact-port restart after active TCP close."""
import errno
import os
import socket

import pytest
from qdrant_test_support import reserve_qdrant_port

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Linux TCP restart contract")


def test_restart_reservation_reuses_time_wait():
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        address = server.getsockname()
        server.listen()
        with socket.create_connection(address, timeout=2) as client:
            accepted, _ = server.accept()
            with accepted:
                accepted.shutdown(socket.SHUT_WR)
                assert client.recv(1) == b""
    # The old reservation fails on this real kernel state.
    with socket.socket() as plain:
        with pytest.raises(OSError) as observed:
            plain.bind(address)
        assert observed.value.errno == errno.EADDRINUSE
    assert reserve_qdrant_port(address[1]) == address[1]


def test_restart_reservation_refuses_a_live_listener():
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen()
        with pytest.raises(OSError) as observed:
            reserve_qdrant_port(server.getsockname()[1])
        assert observed.value.errno == errno.EADDRINUSE
