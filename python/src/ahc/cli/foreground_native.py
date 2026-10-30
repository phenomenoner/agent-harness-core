"""Explicit native minion configuration; parsing never starts a server."""

import json
import re

from ahc.adapters.aar_native_binding import NATIVE_TOOLS, NativeAarPin
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.native_budget import native_budget
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json


def parse_native(raw, session):
    if "native_minion" not in raw:
        return {}
    value = raw["native_minion"]
    fields = {"profile", "pin", "route", "principal_id", "session_id", "grants", "budget"}
    if type(value) is not dict or not fields <= set(value) <= fields | {
            "parent_budget", "retain_artifacts"}:
        raise ContractError("native minion configuration fields invalid")
    profile = McpStdioProfile.from_dict(value["profile"])
    if (profile.agent_id != session.agent_id or profile.capability_digest is None
            or not NATIVE_TOOLS <= set(profile.allowed_tools)):
        raise ContractError("native minion requires matching agent and pinned lifecycle profile")
    pin = value["pin"]
    if type(pin) is not dict or set(pin) != set(NativeAarPin.__dataclass_fields__):
        raise ContractError("native minion pin fields invalid")
    NativeAarPin(**pin)
    route = value["route"]
    if type(route) is not dict or set(route) != {"profile_id", "catalog_digest", "profile_digest"}:
        raise ContractError("native minion route fields invalid")
    _identifier(route["profile_id"], "native route")
    for key in ("catalog_digest", "profile_digest"):
        if type(route[key]) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", route[key]):
            raise ContractError("native minion route digest invalid")
    for key in ("principal_id", "session_id"):
        _identifier(value[key], "native host identity")
    grants = value["grants"]
    if type(grants) is not dict or set(grants) != {"execute", "cancel", "reconcile"}:
        raise ContractError("native minion grant fields invalid")
    for items in grants.values():
        if type(items) is not list or len(items) > 16:
            raise ContractError("native minion grants invalid")
        for item in items:
            _identifier(item, "native grant")
        if items != sorted(set(items)):
            raise ContractError("native minion grants must be sorted and unique")
    if not grants["execute"]:
        raise ContractError("native minion execution grants missing")
    budget = value["budget"]
    limits = {"model_requests": 16, "input_tokens": 9223372036854775807,
              "output_tokens": 9223372036854775807, "child_operations": 0,
              "artifact_bytes": 16384, "wall_time_ms": 60000}
    if type(budget) is not dict or set(budget) != set(limits):
        raise ContractError("native minion budget fields invalid")
    if any(type(budget[key]) is not int or not 0 <= budget[key] <= limit
           for key, limit in limits.items()) or not budget["wall_time_ms"]:
        raise ContractError("native minion budget invalid")
    allocation = {key: amount for key, amount in budget.items() if key != "wall_time_ms"}
    parent_budget = native_budget(value.get("parent_budget", allocation))
    if any(allocation[key] > parent_budget[key] for key in allocation):
        raise ContractError("native child allocation exceeds parent budget")
    retain = value.get("retain_artifacts", False)
    if type(retain) is not bool or (retain and (
            not grants["reconcile"] or not budget["artifact_bytes"])):
        raise ContractError("native artifact retention requires explicit grant and budget")
    return {"native_minion": json.loads(canonical_json({**value, "parent_budget": parent_budget,
                                                       "retain_artifacts": retain}))}


def preview_native(config):
    value = config.to_dict().get("native_minion")
    if value is None:
        return {}
    return {"native_minion": {
        "server_id": value["profile"]["server_id"],
        "enabled": value["profile"]["enabled"],
        "status": "configured" if value["profile"]["enabled"] else "disabled",
        "execution_verified": False,
    }}
