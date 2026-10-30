"""Reconcile the pinned pure Pack envelope; receipts are provider assertions."""

from hashlib import sha256

from .memory_pack import MemoryPack, _fields, _text
from .sessions import ContractError
from .skill_values import canonical_json


def validate_pack_response(value, *, query, scope, budget_tokens, max_items):
    if isinstance(value, dict) and value.get("schema") == "ahc.memory-pack.v2":
        _fields(value, "schema context_pack estimated_tokens")
        pack = MemoryPack.parse(value["context_pack"], query=query, scope=scope,
                                budget_tokens=budget_tokens, max_items=max_items)
        if (value["context_pack"]["schema"] != "ahc.context-pack.v2"
                or type(value["estimated_tokens"]) is not int
                or not 0 <= value["estimated_tokens"] <= budget_tokens
                or value["estimated_tokens"] != len(pack.to_dict()["bundle_text"].encode())):
            raise ContractError("memory pack conservative budget mismatch")
        return pack
    _fields(value, "bundle_text items citations context_pack budget vector_backend lifecycle_write")
    pack = MemoryPack.parse(value["context_pack"], query=query, scope=scope,
                            budget_tokens=budget_tokens, max_items=max_items)
    context = pack.to_dict()
    items = context["items"]
    if value["bundle_text"] != context["bundle_text"] or value["vector_backend"] != "python":
        raise ContractError("memory pack envelope mismatch")
    if type(value["items"]) is not list or len(value["items"]) != len(items):
        raise ContractError("memory pack outer items invalid")
    for outer, item in zip(value["items"], items, strict=True):
        _fields(outer, "recordRef layer id summary kind lang")
        if (outer["recordRef"] != item["recordRef"] or outer["layer"] != item["layer"]
                or outer["summary"] != item["text"] or type(outer["id"]) is not int
                or outer["id"] < 1):
            raise ContractError("memory pack outer item conflict")
        for field in ("kind", "lang"):
            if outer[field] is not None:
                _text(outer[field], 256)
    if value["citations"] != [item["citations"] for item in items]:
        raise ContractError("memory pack outer citations conflict")
    budget = value["budget"]
    _fields(budget, "budgetTokens estimatedTokens maxItems includedItems")
    if (any(type(v) is not int for v in budget.values())
            or budget["budgetTokens"] != budget_tokens or budget["maxItems"] != max_items
            or budget["includedItems"] != len(items)
            or not 0 <= budget["estimatedTokens"] <= budget_tokens):
        raise ContractError("memory pack budget conflict")
    receipt = value["lifecycle_write"]
    _fields(receipt, "kind mode status ts selection mutation lifecycle storage")
    if (receipt["kind"] != "openclaw-mem.pack.lifecycle-write.v1"
            or receipt["mode"] != "selected_pack_records_only" or receipt["status"] != "disabled"):
        raise ContractError("memory pack tracking not disabled")
    _text(receipt["ts"], 128)
    refs = [item["recordRef"] for item in items]
    expected_selection = {
        "pack_selected_refs": refs, "refreshed_record_refs": [], "skipped_record_refs": [],
        "missing_record_refs": [], "selection_signature": sha256(canonical_json(refs)).hexdigest(),
    }
    if receipt["selection"] != expected_selection:
        raise ContractError("memory pack lifecycle selection conflict")
    mutation = receipt["mutation"]
    _fields(mutation, "memory_mutation writes_observations writes_embeddings "
            "auto_archive_applied auto_mutation_applied hard_delete_applied")
    if mutation["memory_mutation"] != "none" or any(
        type(v) is not int or v != 0 for k, v in mutation.items() if k != "memory_mutation"
    ):
        raise ContractError("memory pack reports mutation")
    if receipt["lifecycle"] != {"last_used_at": receipt["ts"], "used_count_before": {},
                                "used_count_after": {}, "archived_at_preserved": True}:
        raise ContractError("memory pack lifecycle conflict")
    if receipt["storage"] != {"field": "observations.detail_json.lifecycle", "error_code": None}:
        raise ContractError("memory pack lifecycle storage error")
    if len(canonical_json(value)) > 262144:
        raise ContractError("memory pack envelope exceeds bound")
    return pack
