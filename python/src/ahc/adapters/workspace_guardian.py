"""Private per-workspace custody server; EOF retires its own created container.

This is a child-process entrypoint, not a daemon or a discovery/cleanup service.
Only the local factory creates custody; messages cannot supply a container ID.
"""

import json
import queue
import sys
import threading

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceCheckpoint, WorkspaceIdentity

FRAME_LIMIT = 65536


def decode_frame(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError()
            result[key] = value
        return result

    if not raw or len(raw) > FRAME_LIMIT or not raw.endswith(b"\n"):
        raise ContractError("guardian frame invalid")
    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError, RecursionError):
        raise ContractError("guardian frame invalid") from None
    if type(value) is not dict:
        raise ContractError("guardian frame must be an object")
    return value


def write_frame(stream, value):
    encoded = json.dumps(value, allow_nan=False).encode("utf-8") + b"\n"
    if len(encoded) > FRAME_LIMIT:
        raise ContractError("guardian response exceeds bound")
    remaining = memoryview(encoded)
    while remaining:
        written = stream.write(remaining)
        if type(written) is not int or written <= 0:
            raise OSError("guardian pipe write failed")
        remaining = remaining[written:]
    stream.flush()


def checkpoint_from(value):
    if type(value) is not dict or set(value) != {"origin", "values_json"}:
        raise ContractError("guardian checkpoint fields invalid")
    return WorkspaceCheckpoint(WorkspaceIdentity.from_dict(value["origin"]), value["values_json"])


def dispatch(port, request, sequence):
    if (
        set(request) != {"id", "operation", "expected", "value"}
        or type(request["id"]) is not int
        or request["id"] != sequence
    ):
        raise ContractError("guardian request correlation invalid")
    expected = WorkspaceIdentity.from_dict(request["expected"])
    if expected != port.identity:
        raise ContractError("guardian workspace identity is stale")
    operation, value = request["operation"], request["value"]
    if operation == "execute":
        return port.execute(expected, value)
    if operation == "inspect":
        if type(value) is not list:
            raise ContractError("guardian inspection names invalid")
        return port.inspect(expected, tuple(value))
    if operation == "checkpoint":
        if type(value) is not list:
            raise ContractError("guardian checkpoint names invalid")
        checkpoint = port.checkpoint(expected, tuple(value))
        return {"origin": checkpoint.origin.to_dict(), "values_json": checkpoint.values_json}
    if operation == "restore":
        return port.restore(expected, checkpoint_from(value)).to_dict()
    raise ContractError("guardian operation unsupported")


def serve(port, source, destination):
    """Own port until close/EOF/error. A bounded port operation must terminate itself.

    A reader observes EOF independently while an operation is executing. Cleanup
    waits for that operation's bounded return; it never races a second remover.
    """
    incoming = queue.Queue(maxsize=2)
    disconnected = threading.Event()
    fault = threading.Event()

    def read():
        try:
            while True:
                raw = source.readline(FRAME_LIMIT + 1)
                if not raw:
                    disconnected.set()
                    incoming.put_nowait(None)
                    return
                incoming.put_nowait(decode_frame(raw))
        except Exception:
            fault.set()
            disconnected.set()
            try:
                incoming.put_nowait(None)
            except queue.Full:
                pass

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    sequence = 0
    closed = False
    try:
        write_frame(destination, {"id": 0, "ok": True, "identity": port.identity.to_dict()})
        while True:
            request = incoming.get()
            if fault.is_set():
                raise ContractError("guardian input failed")
            if request is None or disconnected.is_set():
                return 0
            sequence += 1
            if request == {"id": sequence, "operation": "close"} and type(request["id"]) is int:
                port.close()
                closed = True
                write_frame(destination, {"id": sequence, "ok": True, "closed": True})
                return 0
            result = dispatch(port, request, sequence)
            if disconnected.is_set():
                return 0
            write_frame(destination, {"id": sequence, "ok": True, "result": result})
    except Exception:
        return 1
    finally:
        if not closed:
            port.close()  # Failure exits nonzero; it is never a successful close receipt.


def main():
    from .docker_workspace import DockerRunner, StatefulDockerWorkspace

    try:
        # The EOF reader can remain in ReadFile during orderly close. Do not hold
        # BufferedReader's Python lock across that call at interpreter shutdown.
        source = sys.stdin.buffer.raw
        boot = decode_frame(source.readline(FRAME_LIMIT + 1))
        if set(boot) != {"identity", "checkpoint", "docker_executable", "client_config", "timeout"}:
            raise ContractError("guardian startup fields invalid")
        identity = WorkspaceIdentity.from_dict(boot["identity"])
        checkpoint = None if boot["checkpoint"] is None else checkpoint_from(boot["checkpoint"])
        runner = DockerRunner(boot["docker_executable"], client_config=boot["client_config"])
        port = StatefulDockerWorkspace(
            runner, identity, timeout=boot["timeout"], checkpoint=checkpoint
        )
        return serve(port, source, sys.stdout.buffer)
    except Exception:
        return 1  # Never render exception values, configuration paths or vendor output.


if __name__ == "__main__":
    raise SystemExit(main())
