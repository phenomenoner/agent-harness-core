"""Explicit native provider ports with a sanitized per-call environment."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import ahc
from ahc.domain.skill_values import digest_json


class NativeMemoryUnavailable(RuntimeError):
    pass


class NativeMemoryNotDispatched(NativeMemoryUnavailable):
    """Exact worker receipt proves provider code was never entered."""


class NativeMemoryProcess:
    def __init__(self, *, database, provider_root, provider_digest, timeout, scratch_root):
        self.database, self.provider_root, self.provider_digest = (
            database,
            provider_root,
            provider_digest,
        )
        self.timeout, self.scratch_root = timeout, scratch_root

    def backup(self, destination):
        return self._call("backup_native", {"destination": str(destination)})

    def inspect_backup(self, destination, *, expected_digest):
        return self._call("inspect_native_backup", {
            "destination": str(destination), "expected_digest": expected_digest})

    def status(self):
        return self._call("observe_status", {})

    def get(self, **options):
        return self._call("observe_get", options)

    def resolve(self, **options):
        return self._call("resolve_citation", options)

    def store(self, **options):
        try:
            return self._call("store_committed", options)
        except NativeMemoryNotDispatched:
            return {"status": "failed", "code": "no-commit", "request_digest": digest_json(
                {"version": 1, "scope": options["scope"], "observation": options["observation"]})}

    def reconcile(self, **options):
        return self._call("reconcile_store", options)

    def _call(self, operation, options):
        try:
            request = json.dumps(
                {
                    "database": self.database,
                    "provider_root": self.provider_root,
                    "provider_digest": self.provider_digest,
                    "operation": operation,
                    "options": options,
                },
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if len(request) > 70000:
                raise ValueError()
            with tempfile.TemporaryDirectory(
                prefix="native-memory-", dir=self.scratch_root
            ) as temporary:
                environment = {
                    key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ
                }
                environment.update(
                    {
                        "HOME": temporary,
                        "USERPROFILE": temporary,
                        "TEMP": temporary,
                        "TMP": temporary,
                        "OPENCLAW_HOME": temporary,
                        "OPENCLAW_STATE_DIR": temporary,
                    }
                )
                bootstrap = (
                    "import sys;sys.path.insert(0,sys.argv[1]);"
                    "from ahc.adapters.memory_native_worker import main;main()"
                )
                result = subprocess.run(
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
                    env=environment,
                    capture_output=True,
                    timeout=self.timeout,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            if len(result.stdout) > 131072:
                raise ValueError()
            value = json.loads(result.stdout)
            if result.returncode == 2 and value == {
                "transport": "not-dispatched", "request_digest": hashlib.sha256(request).hexdigest()
            }:
                raise NativeMemoryNotDispatched("native memory operation unavailable")
            if result.returncode != 0:
                raise ValueError()
            return value
        except NativeMemoryNotDispatched:
            raise
        except Exception:
            raise NativeMemoryUnavailable("native memory operation unavailable") from None
