"""One test-owned loopback Qdrant child; no service, container or live-store control."""

import json
import os
import secrets
import socket
import subprocess
import time
import urllib.request
from contextlib import contextmanager


def reserve_qdrant_port(port):
    """Reuse Linux TIME_WAIT for an exact restart, while refusing live listeners."""
    with socket.socket() as reservation:
        if port and os.name == "posix":
            reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        reservation.bind(("127.0.0.1", port))
        return reservation.getsockname()[1]


@contextmanager
def qdrant_process(executable, root, *, storage_root=None, port=0):
    if type(port) is not int or not (port == 0 or 1024 <= port <= 65535):
        raise ValueError("test Qdrant port invalid")
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    port = reserve_qdrant_port(port)
    token = secrets.token_hex(24)
    config = root / "config.yaml"
    config.write_text(
        json.dumps(
            {
                "service": {
                    "host": "127.0.0.1",
                    "http_port": port,
                    "grpc_port": None,
                    "api_key": token,
                    "max_workers": 2,
                },
                "storage": {
                    "storage_path": str(root / "storage" if storage_root is None else storage_root),
                    "snapshots_path": str(root / "snapshots"),
                    "performance": {"max_search_threads": 2},
                },
                "cluster": {"enabled": False},
                "log_level": "WARN",
            }
        ),
        encoding="utf-8",
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("QDRANT")}
    with (root / "server.log").open("wb") as log:
        child = None
        try:
            child = subprocess.Popen(
                [str(executable), "--config-path", str(config), "--disable-telemetry"],
                cwd=root,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            deadline = time.monotonic() + 30
            url = f"http://127.0.0.1:{port}"
            request = urllib.request.Request(url + "/collections", headers={"api-key": token})
            while True:
                if child.poll() is not None:
                    raise RuntimeError("test Qdrant exited before ready")
                try:
                    with urllib.request.urlopen(request, timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("test Qdrant startup deadline") from None
                    time.sleep(0.1)
            yield {"url": url, "api_key": token, "namespace": "ahc-test", "timeout": 10}
        finally:
            if child is not None:
                if child.poll() is None:
                    child.terminate()
                child.wait(timeout=10)
            config.unlink()  # disposable generated token, not provider/user credentials
