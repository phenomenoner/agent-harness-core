"""Sanitized bounded process transport for one trusted MemPalace owner binding."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

import ahc

from .memory_native_process import (
    NativeMemoryNotDispatched,
    NativeMemoryProcess,
    NativeMemoryUnavailable,
)

INTEGRITY_TIMEOUT_SECONDS = 900.0


class MemPalaceMemoryProcess(NativeMemoryProcess):
    def __init__(self, *, binding, scratch_root):
        self.binding = binding
        self.timeout, self.scratch_root = binding.timeout, scratch_root

    def search(self, query, *, scope, limit=8, **options):
        return self._call("search", {"scope": scope, "query": query, "limit": limit})

    def __call__(self, query, *, scope, budget_tokens=1200, limit=8, **options):
        return self._call(
            "pack", {"scope": scope, "query": query, "budget_tokens": budget_tokens, "limit": limit}
        )

    def revise(self, **options):
        return self._call("revise", options)

    def sync(self, **options):
        return self._call("sync", options)

    def integrity_check(self):
        """Explicit operator maintenance; never widen routine/write deadlines."""
        return self._call("integrity_check", {})

    def backup(self, destination):
        return self._call("backup", {"destination": str(destination)})

    def inspect_backup(self, destination, *, expected_digest):
        return self._call("inspect_backup", {"destination": str(destination),
                                             "expected_digest": expected_digest})

    def _call(self, operation, options):
        operation = {
            "observe_status": "status",
            "observe_get": "get",
            "resolve_citation": "resolve",
            "store_committed": "store",
            "reconcile_store": "reconcile",
        }.get(operation, operation)
        try:
            request = json.dumps(
                {"config": asdict(self.binding), "operation": operation, "options": options},
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
            if len(request) > 70000:
                raise ValueError()
            with tempfile.TemporaryDirectory(
                prefix="palace-memory-", dir=self.scratch_root
            ) as temporary:
                env = {
                    key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ
                }
                env.update(
                    {
                        "HOME": temporary,
                        "USERPROFILE": temporary,
                        "TEMP": temporary,
                        "TMP": temporary,
                        "NO_PROXY": "127.0.0.1",
                        "HF_HUB_OFFLINE": "1",
                        "HF_HUB_DISABLE_TELEMETRY": "1",
                        "ANONYMIZED_TELEMETRY": "False",
                    }
                )
                bootstrap = (
                    "import sys;sys.path.insert(0,sys.argv[1]);"
                    "from ahc.adapters.mempalace_memory_worker import main;main()"
                )
                from .memory_read_budget import run_read_worker

                run = (run_read_worker if operation in {"search", "pack", "status", "get", "resolve"}
                       else subprocess.run)
                result = run(
                    [
                        sys.executable,
                        "-I",
                        "-B",
                        "-c",
                        bootstrap,
                        str(Path(ahc.__file__).parent.parent),
                    ],
                    input=request,
                    cwd=temporary,
                    env=env,
                    capture_output=True,
                    timeout=(INTEGRITY_TIMEOUT_SECONDS
                             if operation == "integrity_check" else self.timeout),
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            if len(result.stdout) > 131072:
                raise ValueError()
            value = json.loads(result.stdout)
            if result.returncode == 2 and value == {
                "transport": "not-dispatched",
                "request_digest": hashlib.sha256(request).hexdigest(),
            }:
                raise NativeMemoryNotDispatched("MemPalace operation not dispatched")
            if result.returncode != 0:
                raise ValueError()
            return value
        except NativeMemoryNotDispatched:
            raise
        except subprocess.TimeoutExpired:
            raise NativeMemoryUnavailable("MemPalace operation timed out") from None
        except Exception:
            raise NativeMemoryUnavailable("MemPalace operation unavailable") from None
