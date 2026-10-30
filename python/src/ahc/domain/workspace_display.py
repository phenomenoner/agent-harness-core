"""Bounded display data; consumers must not execute HTML as trusted UI."""

import json

from .sessions import ContractError


def display_fields(value):
    if "displays" not in value and "display_truncated" not in value:
        return {}
    rows = value.get("displays")
    if type(rows) is not list or len(rows) > 16 or type(value.get("display_truncated")) is not bool:
        raise ContractError("workspace display invalid")
    for row in rows:
        if (type(row) is not dict or not row
                or set(row) - {"text/plain", "text/html", "image/png", "image/jpeg"}
                or any(type(v) is not str for v in row.values())):
            raise ContractError("workspace display invalid")
    try:
        encoded = json.dumps(rows, ensure_ascii=True, allow_nan=False)
        if len(encoded) > 16384 or any(
            0xD800 <= ord(c) <= 0xDFFF for row in rows for v in row.values() for c in v
        ):
            raise ValueError()
    except (ValueError, UnicodeError):
        raise ContractError("workspace display exceeds bound") from None
    return {"displays": rows, "display_truncated": value["display_truncated"]}
