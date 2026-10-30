import builtins

import pytest

from ahc.domain.sessions import ContractError
from ahc.infrastructure.mcp_schema import McpSchemaValidator


def test_absent_dependency_is_explicit(monkeypatch):
    original = builtins.__import__

    def absent(name, *args, **kwargs):
        if name == "jsonschema":
            raise ImportError()
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", absent)
    with pytest.raises(ContractError, match="dependency unavailable"):
        McpSchemaValidator()


def test_real_validator_nested_refs_and_no_external_fetch():
    pytest.importorskip("jsonschema")
    validate = McpSchemaValidator()
    schema = {"type": "object", "$defs": {"count": {"type": "integer", "minimum": 1}},
              "properties": {"count": {"$ref": "#/$defs/count"}},
              "required": ["count"], "additionalProperties": False}
    assert validate(schema, {"count": 2})
    assert not validate(schema, {"count": True})
    assert not validate(schema, {"count": 0})
    assert not validate(schema, {"count": 2, "extra": 1})
    assert not validate({"$ref": "https://invalid.example/schema"}, {})
    assert not validate({"$schema": "unsupported", "type": "object"}, {})
