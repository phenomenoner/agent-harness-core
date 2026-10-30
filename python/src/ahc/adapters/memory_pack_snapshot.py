"""Explicit public-provider Pack over an offline snapshot in a disposable process."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import ahc
from ahc.application.memory_pack_service import MemoryPackUnavailable

_BOOTSTRAP = (
    "import sys;sys.path.insert(0,sys.argv[1]);"
    "from ahc.adapters.memory_pack_worker import main;main()"
)


class SnapshotPack:
    def __init__(self, *, snapshot, provider_root, timeout):
        self.snapshot = snapshot
        self.provider_root = provider_root
        self.timeout = timeout

    def __call__(self, query, **options):
        return self._request("pack", query, options)

    def search(self, query, **options):
        return self._request("search", query, options)

    def _request(self, operation, query, options):
        try:
            path = Path(self.snapshot)
            if not path.is_file() or any(Path(str(path) + suffix).exists()
                                         for suffix in ("-wal", "-shm", "-journal")):
                raise ValueError("offline snapshot required")
            with tempfile.TemporaryDirectory(prefix="ahc-memory-pack-") as temporary:
                root = Path(temporary)
                (root / "openclaw.json").write_text("{}", encoding="utf-8")
                (root / "memory.toml").write_text("", encoding="utf-8")
                environment = {k: os.environ[k] for k in ("SystemRoot", "WINDIR")
                               if k in os.environ}
                environment.update({
                    "HOME": temporary, "USERPROFILE": temporary, "TEMP": temporary,
                    "TMP": temporary, "OPENCLAW_HOME": temporary, "OPENCLAW_STATE_DIR": temporary,
                    "OPENCLAW_CONFIG_PATH": str(root / "openclaw.json"),
                    "OPENCLAW_MEM_CONFIG": str(root / "memory.toml"),
                    "OPENCLAW_MEM_READONLY_DB": "1",
                })
                result = subprocess.run(
                    [sys.executable, "-I", "-B", "-c", _BOOTSTRAP,
                     str(Path(ahc.__file__).parent.parent)],
                    input=json.dumps({"operation": operation, "snapshot": self.snapshot,
                                      "provider_root": self.provider_root,
                                      "query": query, "options": options},
                                     ensure_ascii=False).encode("utf-8"),
                    cwd=temporary, env=environment, capture_output=True, timeout=self.timeout,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            if result.returncode != 0 or len(result.stdout) > 262144:
                raise ValueError("Pack worker unavailable")
            return json.loads(result.stdout)
        except Exception:
            raise MemoryPackUnavailable("memory snapshot Pack unavailable") from None
