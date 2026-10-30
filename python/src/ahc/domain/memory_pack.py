"""Validated provider context data, never instruction or authorization authority."""

import json
from dataclasses import dataclass, field
from hashlib import sha256

from .sessions import ContractError
from .skill_values import canonical_json


def _fields(value, names):
    if type(value) is not dict or set(value) != set(names.split()):
        raise ContractError("memory pack fields invalid")


def _text(value, limit, *, empty=False):
    if type(value) is not str or (not value and not empty) or len(value) > limit:
        raise ContractError("memory pack text invalid")
    if any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ContractError("memory pack text invalid")


@dataclass(frozen=True, slots=True)
class MemoryPack:
    """Immutable validated JSON with metadata-only diagnostic projection."""

    _encoded: bytes = field(repr=False)

    @classmethod
    def parse(cls, value, *, query, scope, budget_tokens, max_items):
        _text(query, 4096)
        _text(scope, 256)
        if (type(budget_tokens) is not int or not 1 <= budget_tokens <= 16384
                or type(max_items) is not int or not 1 <= max_items <= 64):
            raise ContractError("memory pack request bounds invalid")
        _fields(value, "schema meta bundle_text items notes")
        if value["schema"] not in ("openclaw-mem.context-pack.v1", "ahc.context-pack.v2"):
            raise ContractError("memory pack version unsupported")
        meta = value["meta"]
        _fields(meta, "ts query scope budgetTokens maxItems")
        _text(meta["ts"], 128)
        if (meta["query"] != query or meta["scope"] != scope
                or type(meta["budgetTokens"]) is not int
                or type(meta["maxItems"]) is not int
                or meta["budgetTokens"] != budget_tokens or meta["maxItems"] != max_items):
            raise ContractError("memory pack request identity mismatch")
        _text(value["bundle_text"], 65536, empty=True)
        items = value["items"]
        if type(items) is not list or len(items) > max_items:
            raise ContractError("memory pack items invalid")
        refs = set()
        for item in items:
            _fields(item, "recordRef layer type importance trust text citations")
            for key in ("recordRef", "layer", "type", "importance", "trust"):
                _text(item[key], 256)
            _text(item["text"], 16384)
            if value["schema"] == "ahc.context-pack.v2" and item["trust"] != "untrusted":
                raise ContractError("memory data cannot claim instruction authority")
            ref = item["recordRef"]
            if ref in refs:
                raise ContractError("memory pack duplicate reference")
            refs.add(ref)
            _fields(item["citations"], "url recordRef")
            if item["citations"]["recordRef"] != ref:
                raise ContractError("memory pack citation mismatch")
            if item["citations"]["url"] is not None:
                _text(item["citations"]["url"], 2048)
        expected = "\n".join(f"- [{i['recordRef']}] {i['text']}" for i in items)
        if value["bundle_text"] != expected:
            raise ContractError("memory pack bundle mismatch")
        _fields(value["notes"], "how_to_use")
        notes = value["notes"]["how_to_use"]
        if type(notes) is not list or len(notes) > 16:
            raise ContractError("memory pack notes invalid")
        for note in notes:
            _text(note, 2048)
        encoded = canonical_json(value)
        if len(encoded) > 131072:
            raise ContractError("memory pack exceeds total bound")
        return cls(encoded)

    def to_dict(self):
        return json.loads(self._encoded)

    def summary(self):
        return {"result_digest": sha256(self._encoded).hexdigest(),
                "item_count": len(self.to_dict()["items"]),
                "trust_source": "provider-asserted", "trace_coverage": "not-established"}
