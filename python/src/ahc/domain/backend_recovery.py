"""Bounded, informational continuation evidence; never permission to replay effects."""

from .sessions import ContractError


def recovery_policy(value):
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {"delays_seconds"}
            or type(value["delays_seconds"]) is not list
            or not 1 <= len(value["delays_seconds"]) <= 2
            or any(type(v) is not int or not 1 <= v <= 600 for v in value["delays_seconds"])):
        raise ContractError("backend recovery policy invalid")
    return {"delays_seconds": list(value["delays_seconds"])}


def recovery_projection(value):
    if value is None:
        return {}
    if (type(value) is not dict or set(value) != {"attempts", "state", "last_failure"}
            or type(value["attempts"]) is not int or not 0 <= value["attempts"] <= 2
            or value["state"] not in {"not-needed", "cooldown", "continuing", "recovered",
                                      "exhausted", "blocked", "cancelled", "deadline"}
            or value["last_failure"] not in {None, "overloaded", "capacity"}):
        raise ContractError("backend recovery observation invalid")
    return {"backend_recovery": dict(value)}
