"""Explicit Draft 2020-12 validation with no remote-reference retrieval."""

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

DIALECT = "https://json-schema.org/draft/2020-12/schema"


class McpSchemaValidator:
    def __init__(self):
        try:
            from jsonschema import Draft202012Validator
            from referencing import Registry
        except ImportError:
            raise ContractError("MCP JSON Schema dependency unavailable") from None
        self.validator_class = Draft202012Validator
        # Empty registry rejects unresolved external references without fetching URLs.
        self.registry = Registry()

    def __call__(self, schema, arguments):
        try:
            if (type(schema) is not dict or type(arguments) is not dict
                    or len(canonical_json(schema)) > 65536
                    or len(canonical_json(arguments)) > 65536):
                return False
            pending, visited = [(schema, 0)], 0
            while pending:
                value, depth = pending.pop()
                visited += 1
                if visited > 4096 or depth > 64:
                    return False
                if type(value) is dict:
                    if "$schema" in value and value["$schema"] != DIALECT:
                        return False
                    pending.extend((v, depth + 1) for v in value.values())
                elif type(value) is list:
                    pending.extend((v, depth + 1) for v in value)
            self.validator_class.check_schema(schema)
            return self.validator_class(schema, registry=self.registry).is_valid(arguments)
        except Exception:
            # Validation failures never include schema/argument contents in diagnostics.
            return False
