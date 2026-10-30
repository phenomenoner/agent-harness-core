"""One digest-checked public native operation in a disposable process."""

import contextlib
import hashlib
import json
import os
import sys

from .memory_provider_identity import provider_digest


def main():
    raw = b""
    provider_entered = False
    try:
        raw = sys.stdin.buffer.read(70001)
        if len(raw) > 70000:
            raise ValueError()
        request = json.loads(raw)
        if provider_digest(request["provider_root"]) != request["provider_digest"]:
            raise ValueError()
        operation = request["operation"]
        fields = {
            "backup_native": {"destination"},
            "inspect_native_backup": {"destination", "expected_digest"},
            "observe_status": set(),
            "resolve_citation": {"scope", "row_id"},
            "observe_get": {"scope", "record_ref"},
            "store_committed": {"scope", "operation_id", "observation"},
            "reconcile_store": {"scope", "operation_id", "request_digest"},
        }
        if operation not in fields or set(request["options"]) != fields[operation]:
            raise ValueError()

        def no_external(event, args):
            if event in ("socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"):
                raise RuntimeError("memory external effect forbidden")

        sys.addaudithook(no_external)
        sys.path.insert(0, request["provider_root"])
        with (
            open(os.devnull, "w") as sink,
            contextlib.redirect_stdout(sink),
            contextlib.redirect_stderr(sink),
        ):
            # Import can itself have effects; only earlier failures prove no dispatch.
            provider_entered = True
            from openclaw_mem.core import api

            if operation == "inspect_native_backup":
                value = api.inspect_native_backup(**request["options"])
            else:
                value = getattr(api, operation)(request["database"], **request["options"])
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(encoded) > 131072:
            raise ValueError()
        sys.stdout.buffer.write(encoded)
    except Exception:
        failure = ({"transport": "not-dispatched",
                    "request_digest": hashlib.sha256(raw).hexdigest()}
                   if not provider_entered else {"transport": "unavailable"})
        sys.stdout.buffer.write(json.dumps(failure).encode("utf-8"))
        raise SystemExit(2) from None
