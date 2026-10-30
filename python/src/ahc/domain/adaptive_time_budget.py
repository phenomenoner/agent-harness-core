"""Explicit owner policy for expiry-triggered task renewal."""
import math
from ahc.domain.sessions import ContractError

DEFAULTS = {"enabled": False, "model": "gpt-6-luna", "reasoning_effort": "low",
            "extension_seconds": 1800, "judge_timeout_seconds": 120,
            "wrap_up_seconds": 180, "on_judge_error": "wrap-up"}

def time_budget_policy(value):
    if type(value) is not dict or not set(value) <= set(DEFAULTS):
        raise ContractError("time_budget policy fields invalid")
    result = {**DEFAULTS, **value}
    if type(result["enabled"]) is not bool:
        raise ContractError("time_budget.enabled must be boolean")
    if (type(result["model"]) is not str or not result["model"].strip()
            or len(result["model"]) > 256):
        raise ContractError("time_budget.model invalid")
    effort = result["reasoning_effort"]
    if (type(effort) is not str or not effort or len(effort) > 64
            or not effort.isascii() or not effort.replace("-", "").isalnum()):
        raise ContractError("time_budget.reasoning_effort invalid")
    for key in ("extension_seconds", "judge_timeout_seconds", "wrap_up_seconds"):
        number = result[key]
        if type(number) not in (int, float) or not math.isfinite(number) or number <= 0:
            raise ContractError("time_budget duration must be finite and positive")
    if result["extension_seconds"] < 60:
        raise ContractError("time_budget extension must be at least 60 seconds")
    if result["on_judge_error"] not in ("wrap-up", "extend"):
        raise ContractError("time_budget error policy invalid")
    return result

def deadline_value(value):
    return value() if callable(value) else value
