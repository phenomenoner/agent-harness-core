"""Explicit, bounded persona content; preview never returns private instructions."""

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import digest_json


def parse_agent_context(raw):
    if "agent_context" not in raw:
        return {}
    value = raw["agent_context"]
    required = {"resource_id", "instructions"}
    defaults = {"recall": False, "history_turns": 4, "memory_tokens": 1200, "memory_items": 4}
    if (type(value) is not dict or not required <= set(value) <= required | defaults.keys()):
        raise ContractError("agent context fields invalid")
    result = {**defaults, **value}
    _identifier(result["resource_id"], "persona resource")
    text = result["instructions"]
    if (type(text) is not str or not text.strip() or len(text) > 12000
            or any(0xD800 <= ord(c) <= 0xDFFF or (ord(c) < 32 and c not in "\n\t")
                   for c in text) or len(text.encode("utf-8")) > 24000):
        raise ContractError("agent instructions invalid or oversized")
    if type(result["recall"]) is not bool:
        raise ContractError("agent context recall invalid")
    for key, lower, upper in (("history_turns", 0, 8), ("memory_tokens", 1, 4096),
                               ("memory_items", 1, 8)):
        if type(result[key]) is not int or not lower <= result[key] <= upper:
            raise ContractError("agent context bounds invalid")
    return {"agent_context": result}


def bind_agent_context(config, registry):
    value = config.to_dict().get("agent_context")
    if value is None:
        return
    if (registry is None
            or registry.profile(config.session.agent_id).persona_ref != value["resource_id"]):
        raise ContractError("agent context requires matching profile persona")
    if value["recall"]:
        memory = config.to_dict().get("memory", {})
        if not memory.get("enabled") or not memory.get("granted"):
            raise ContractError("automatic recall requires enabled granted memory")


def preview_agent_context(config):
    value = config.to_dict().get("agent_context")
    if value is None:
        return {}
    return {"agent_context": {key: val for key, val in value.items() if key != "instructions"}
            | {"instructions_digest": digest_json(value["instructions"])}}
