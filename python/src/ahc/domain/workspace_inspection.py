"""Bounded metadata-only inspection; no arbitrary value representation."""

from .sessions import ContractError

KINDS = ("missing", "null", "boolean", "integer", "float", "string", "list", "dict", "opaque")


def inspection_names(names):
    if (
        type(names) is not tuple
        or len(names) > 32
        or any(
            type(n) is not str
            or not n.isascii()
            or not n.isidentifier()
            or n.startswith("_")
            or len(n) > 128
            for n in names
        )
        or len(set(names)) != len(names)
    ):
        raise ContractError("inspection names invalid")
    return names


def inspection_result(identity, names, rows):
    inspection_names(names)
    if type(rows) is not list or len(rows) != len(names):
        raise ContractError("inspection result invalid")
    for name, row in zip(names, rows, strict=True):
        if (
            type(row) is not dict
            or set(row) != {"name", "kind", "size"}
            or row["name"] != name
            or row["kind"] not in KINDS
            or (row["size"] is not None and (type(row["size"]) is not int or row["size"] < 0))
            or (row["kind"] not in ("string", "list", "dict") and row["size"] is not None)
        ):
            raise ContractError("inspection result invalid")
    return {"identity": identity.to_dict(), "variables": rows}
