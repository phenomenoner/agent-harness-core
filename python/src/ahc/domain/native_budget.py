"""Native work counters; wall deadlines remain separate from aggregate spend."""

from .sessions import ContractError

COUNTERS = frozenset({"model_requests", "input_tokens", "output_tokens",
                      "child_operations", "artifact_bytes"})


def native_budget(value):
    if (type(value) is not dict or set(value) != COUNTERS
            or any(type(amount) is not int or not 0 <= amount <= 9223372036854775807
                   for amount in value.values())):
        raise ContractError("native aggregate budget invalid")
    return dict(value)
