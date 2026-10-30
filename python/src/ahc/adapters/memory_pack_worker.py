"""One public Pack call; environment and offline snapshot are supplied by the owner."""

import contextlib
import importlib.machinery
import json
import os
import sys


def main():
    request = json.loads(sys.stdin.buffer.read(65537))
    if importlib.machinery.PathFinder.find_spec("openclaw_mem", [request["provider_root"]]) is None:
        raise ImportError("explicit memory provider unavailable")
    sys.path.insert(0, request["provider_root"])

    def no_external(event, args):
        if event in ("socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"):
            raise RuntimeError("memory Pack external effect unavailable")

    sys.addaudithook(no_external)
    # Provider diagnostics never become protocol data or host logs.
    with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), (
        contextlib.redirect_stderr(sink)
    ):
        from openclaw_mem.core.api import connect, pack, search

        connection = connect(request["snapshot"])
        try:
            if request["operation"] == "pack":
                value = pack(connection, request["query"], model="ahc-lexical-only",
                             **request["options"])
            elif request["operation"] == "search":
                value = search(connection, request["query"], **request["options"])
            else:
                raise ValueError("unsupported memory operation")
        finally:
            connection.close()
    encoded = json.dumps(value, ensure_ascii=False).encode("utf-8")
    if len(encoded) > 262144:
        raise ValueError("memory Pack output limit")
    sys.stdout.buffer.write(encoded)
