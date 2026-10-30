"""Read committed native memory through one disposable provider-owned snapshot."""

import hashlib
import tempfile
import time
from pathlib import Path

from ahc.application.memory_pack_service import MemoryPackUnavailable

from .memory_native_process import NativeMemoryProcess
from .memory_pack_snapshot import SnapshotPack


class LiveMemoryPack:
    def __init__(self, *, binding, scratch_root, timeout):
        self.binding, self.scratch_root, self.timeout = binding, scratch_root, timeout

    def __call__(self, query, **options):
        return self._read("pack", query, options)

    def search(self, query, **options):
        return self._read("search", query, options)

    def _read(self, operation, query, options):
        try:
            started = time.monotonic()
            with tempfile.TemporaryDirectory(
                prefix="memory-recall-", dir=self.scratch_root
            ) as temp:
                target = Path(temp) / "snapshot"
                adapter = NativeMemoryProcess(
                    database=self.binding.database, provider_root=self.binding.provider_root,
                    provider_digest=self.binding.provider_digest,
                    timeout=self.timeout, scratch_root=temp,
                )
                receipt = adapter.backup(target)
                database = target / "memory.sqlite"
                if (type(receipt) is not dict
                        or receipt.get("schema") != "openclaw-mem.native.backup.v1"
                        or receipt.get("database") != "memory.sqlite"
                        or receipt.get("activation_allowed") is not False
                        or not database.is_file() or database.is_symlink()
                        or not 0 < database.stat().st_size <= 2 * 1024**3
                        or receipt.get("byte_count") != database.stat().st_size):
                    raise ValueError("native snapshot receipt invalid")
                with database.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if digest != receipt.get("sha256"):
                    raise ValueError("native snapshot digest invalid")
                remaining = self.timeout - (time.monotonic() - started)
                if remaining <= 0:
                    raise ValueError("native recall deadline")
                reader = SnapshotPack(snapshot=str(database),
                                      provider_root=self.binding.provider_root, timeout=remaining)
                return reader(query, **options) if operation == "pack" else reader.search(
                    query, **options)
        except Exception:
            raise MemoryPackUnavailable("native memory recall unavailable") from None
