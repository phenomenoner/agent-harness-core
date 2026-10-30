"""Explicit owner-configured host mappings and bounded operating context."""

import re
from pathlib import PurePosixPath

from ahc.domain.sessions import ContractError


def parse_host_context(raw):
    result = {}
    if "host_paths" in raw:
        rows = raw["host_paths"]
        if type(rows) is not list or len(rows) > 4:
            raise ContractError("host path mappings invalid")
        seen = set()
        backend_seen = set()
        mappings = []
        for row in rows:
            if (type(row) is not dict or set(row) != {"host_root", "backend_root"}
                    or type(row["host_root"]) is not str
                    or re.fullmatch(r"[A-Z]:/", row["host_root"]) is None
                    or type(row["backend_root"]) is not str):
                raise ContractError("host path mapping fields invalid")
            path = row["backend_root"]
            if (not path.startswith("/") or path == "/" or len(path) > 256
                    or str(PurePosixPath(path)) != path or ".." in PurePosixPath(path).parts
                    or "\\" in path
                    or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in path)
                    or row["host_root"] in seen or path in backend_seen):
                raise ContractError("host path mapping location invalid")
            seen.add(row["host_root"])
            backend_seen.add(path)
            mappings.append(dict(row))
        result["host_paths"] = mappings
    if "operating_context" in raw:
        value = raw["operating_context"]
        keys = {"source_roots", "operator_docs", "memory_coverage", "skills_coverage"}
        if type(value) is not dict or set(value) - keys:
            raise ContractError("operating context fields invalid")
        for key, item in value.items():
            items = item if key in {"source_roots", "operator_docs"} else [item]
            if (type(items) is not list or len(items) > 4
                    or any(type(x) is not str or not x.strip() or len(x) > 1024
                           or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in x)
                           for x in items)):
                raise ContractError("operating context value invalid")
        result["operating_context"] = dict(value)
    return result
