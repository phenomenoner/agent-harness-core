"""Standalone data-only bundle writer, copied into an admitted Linux workspace.

Every directory walk uses retained descriptors and no-follow opens. New exclusive
files never replace user data. Failed partial copies remain for diagnosis; this
does not confer protection on a trusted interpreter or execute imported scripts.
"""

import base64
import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import PurePosixPath


class SkillBundleWriter:
    def __init__(self, base, revision, files, size):
        self.base_fd = self.root_fd = self.file_fd = None
        self.rows = []
        self.names = set()
        self.written = 0
        self.current = None
        if (
            os.name != "posix" or os.open not in os.supports_dir_fd
            or type(base) is not str or not base.startswith("/")
            or str(PurePosixPath(base)) != base or ".." in PurePosixPath(base).parts
            or not re.fullmatch("[0-9a-f]{64}", revision)
            or type(files) is not int or not 1 <= files <= 257
            or type(size) is not int or not 0 <= size <= 64 * 1024**2 + 262144
        ):
            raise ValueError("bundle admission invalid")
        self.flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        self.base, self.revision, self.files, self.size = base, revision, files, size
        try:
            self.base_fd = os.open("/", self.flags)
            for part in PurePosixPath(base).parts[1:]:
                following = os.open(part, self.flags, dir_fd=self.base_fd)
                os.close(self.base_fd)
                self.base_fd = following
            self.name = ".ahc-skill-" + revision[:12] + "-" + secrets.token_hex(12)
            os.mkdir(self.name, 0o700, dir_fd=self.base_fd)
            self.root_fd = os.open(self.name, self.flags, dir_fd=self.base_fd)
        except BaseException:
            self.close()
            raise

    def _parent(self, path, create=False):
        descriptor = os.dup(self.root_fd)
        try:
            for part in path.split("/")[:-1]:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                following = os.open(part, self.flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = following
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _begin_file(self, path, size, checksum):
        if (
            self.current is not None or type(path) is not str or not 1 <= len(path) <= 240
            or type(size) is not int or not 0 <= size <= 16 * 1024**2
            or not re.fullmatch("[0-9a-f]{64}", checksum)
            or len(self.rows) >= self.files or self.written + size > self.size
        ):
            raise ValueError("bundle file invalid")
        parts = path.split("/")
        if any(not p or p in (".", "..") or p.rstrip(" .") != p
               or any(ord(c) < 32 or c in '\\:*?"<>|' for c in p) for p in parts):
            raise ValueError("bundle path invalid")
        folded = path.casefold()
        if any(folded == n or folded.startswith(n + "/") or n.startswith(folded + "/")
               for n in self.names):
            raise ValueError("bundle path collision")
        parent = self._parent(path, create=True)
        try:
            self.file_fd = os.open(parts[-1], os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                   0o600, dir_fd=parent)
        finally:
            os.close(parent)
        self.names.add(folded)
        self.current = {"path": path, "size": size, "sha256": checksum}
        self.offset = 0

    def _chunk(self, offset, encoded):
        if (self.current is None or type(offset) is not int or offset != self.offset
                or type(encoded) is not str or len(encoded) > 10924):
            raise ValueError("bundle chunk invalid")
        raw = base64.b64decode(encoded, validate=True)
        if not raw or len(raw) > 8192 or self.offset + len(raw) > self.current["size"]:
            raise ValueError("bundle chunk bound invalid")
        remaining = memoryview(raw)
        while remaining:
            count = os.write(self.file_fd, remaining)
            if count <= 0:
                raise OSError("bundle write incomplete")
            remaining = remaining[count:]
        self.offset += len(raw)
        self.written += len(raw)

    @staticmethod
    def _checksum(descriptor, size):
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != size:
            raise ValueError("bundle file shape changed")
        os.lseek(descriptor, 0, os.SEEK_SET)
        checksum = hashlib.sha256()
        remaining = size + 1
        while remaining and (chunk := os.read(descriptor, min(remaining, 65536))):
            checksum.update(chunk)
            remaining -= len(chunk)
        if remaining != 1:
            raise ValueError("bundle file length changed")
        return checksum.hexdigest()

    def _end_file(self):
        if (self.current is None or self.offset != self.current["size"]
                or self._checksum(self.file_fd, self.current["size"]) != self.current["sha256"]):
            raise ValueError("bundle file integrity mismatch")
        os.close(self.file_fd)
        self.file_fd = None
        self.rows.append(self.current)
        self.current = None

    def _finish(self):
        if self.current is not None or len(self.rows) != self.files or self.written != self.size:
            raise ValueError("bundle is incomplete")
        for row in self.rows:
            parent = self._parent(row["path"])
            descriptor = None
            try:
                descriptor = os.open(row["path"].split("/")[-1],
                                     os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=parent)
                if (os.fstat(descriptor).st_size != row["size"]
                        or self._checksum(descriptor, row["size"]) != row["sha256"]):
                    raise ValueError("bundle changed during materialization")
            finally:
                if descriptor is not None:
                    os.close(descriptor)
                os.close(parent)
        for path, parent, held in ((self.base, None, self.base_fd),
                                    (self.name, self.base_fd, self.root_fd)):
            actual = os.stat(path, dir_fd=parent, follow_symlinks=False)
            expected = os.fstat(held)
            if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
                raise ValueError("bundle directory changed")
        manifest = json.dumps(sorted(self.rows, key=lambda r: r["path"]),
                              sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        result = {"root": self.base.rstrip("/") + "/" + self.name,
                  "revision_digest": self.revision,
                  "manifest_digest": hashlib.sha256(manifest).hexdigest(),
                  "file_count": self.files, "byte_count": self.size}
        self.close()
        return result

    def call(self, action, *args):
        try:
            if self.root_fd is None:
                raise ValueError("bundle writer closed")
            actions = {"begin_file": self._begin_file, "chunk": self._chunk,
                       "end_file": self._end_file, "finish": self._finish}
            return actions[action](*args)
        except BaseException:
            self.close()
            raise

    def close(self):
        for name in ("file_fd", "root_fd", "base_fd"):
            descriptor = getattr(self, name, None)
            if descriptor is not None:
                setattr(self, name, None)
                os.close(descriptor)
