"""Explicit channel control catalog; omission preserves existing configuration bytes."""

import re

from ahc.domain.sessions import ContractError


def parse_channel_control(raw, session, backend):
    if "channel_control" not in raw:
        return {}
    value = raw["channel_control"]
    if (type(value) is not dict or set(value) - {"native_ui"} != {"enabled", "models", "aliases"}
            or type(value["enabled"]) is not bool
            or type(value.get("native_ui", False)) is not bool):
        raise ContractError("channel control fields invalid")
    models, aliases = value["models"], value["aliases"]
    if type(models) is not dict or not 1 <= len(models) <= 32 or type(aliases) is not dict:
        raise ContractError("channel model catalog invalid")
    for name, options in models.items():
        if (type(name) is not str or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name)
                or type(options) is not dict or set(options) != {"efforts", "priority"}
                or type(options["priority"]) is not bool
                or type(options["efforts"]) is not list or not options["efforts"]
                or any(type(e) is not str or e not in (
                                 "none", "minimal", "low", "medium", "high", "xhigh", "max",
                                 "ultra") for e in options["efforts"])
                or len(set(options["efforts"])) != len(options["efforts"])):
            raise ContractError("channel model capability invalid")
    if backend["model"] not in models or len(aliases) > 32:
        raise ContractError("configured model missing from channel catalog")
    for alias, model in aliases.items():
        if (type(alias) is not str or not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", alias)
                or type(model) is not str or model not in models or alias in models):
            raise ContractError("channel model alias invalid")
    if backend.get("reasoning_effort") and backend["reasoning_effort"] not in models[
            backend["model"]]["efforts"]:
        raise ContractError("configured effort missing from channel catalog")
    if backend.get("service_tier") == "priority" and not models[backend["model"]]["priority"]:
        raise ContractError("configured model does not support priority")
    return {"channel_control": value}
