"""Atomic JSON replacement for small lifecycle observations."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from pathlib import Path


class JsonStoreError(RuntimeError):
    """Base class for snapshot/control store errors."""


class JsonStoreMissing(JsonStoreError):
    """The requested observation has not been published."""


class JsonStoreInvalid(JsonStoreError):
    """The file or value is not a strict JSON object observation."""


class AtomicJsonStore:
    """Write one strict JSON object through same-directory atomic replacement."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def write(self, value: Mapping[str, object]) -> None:
        payload = dict(value)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                try:
                    json.dump(
                        payload,
                        handle,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                except (TypeError, ValueError) as exc:
                    raise JsonStoreInvalid(f"value is not strict JSON for {self.path}") from exc
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._replace_with_bounded_retry(temporary)
            self._sync_directory()
        finally:
            temporary.unlink(missing_ok=True)

    def read(self) -> dict[str, object]:
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                value = json.load(
                    handle,
                    object_pairs_hook=_object_without_duplicates,
                    parse_constant=_reject_non_finite,
                )
        except FileNotFoundError as exc:
            raise JsonStoreMissing(str(self.path)) from exc
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise JsonStoreInvalid(f"cannot read strict JSON object {self.path}") from exc
        if not isinstance(value, dict):
            raise JsonStoreInvalid(f"JSON observation is not an object: {self.path}")
        return value

    def exists(self) -> bool:
        return self.path.is_file()

    def _sync_directory(self) -> None:
        try:
            descriptor = os.open(self.path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)

    def _replace_with_bounded_retry(self, temporary: Path) -> None:
        """Tolerate a brief Windows reader handle without hiding persistent failure."""

        deadline = time.monotonic() + 0.5
        while True:
            try:
                os.replace(temporary, self.path)
                return
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.005)


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> object:
    raise ValueError(f"non-finite JSON number: {value}")
