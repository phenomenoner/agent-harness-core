"""Atomic parent-wide native allocations; uncertainty never refunds a reservation."""

import json

from ahc.domain.native_budget import native_budget
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json

from .native_child_repository import NativeChildRepository


class NativeBudgetRepository:
    def __init__(self, children):
        self.native, self.store = NativeChildRepository(children), children.store

    def reserve(self, parent, child_id, token, limits, allocation):
        _identifier(token, "native budget claim")
        limits, allocation = native_budget(limits), native_budget(allocation)
        encoded = canonical_json(limits).decode()

        def write(connection):
            self.native._child(connection, parent, child_id, token)
            if connection.execute("SELECT 1 FROM runtime_native_budgets WHERE child_id=?",
                                  (child_id,)).fetchone() is not None:
                raise ContractError("native budget already reserved; do not replay")
            used = dict.fromkeys(limits, 0)
            rows = connection.execute(
                "SELECT limits_json,allocation_json FROM runtime_native_budgets "
                "WHERE parent_operation_id=?", (parent.operation_id,)).fetchall()
            for row in rows:
                if row["limits_json"] != encoded:
                    raise ContractError("native parent budget policy changed")
                previous = native_budget(json.loads(row["allocation_json"]))
                for key in used:
                    used[key] += previous[key]
            if any(used[key] + allocation[key] > limits[key] for key in used):
                raise ContractError("native parent budget exhausted")
            connection.execute("INSERT INTO runtime_native_budgets VALUES(?,?,?,?)",
                (child_id, parent.operation_id, encoded, canonical_json(allocation).decode()))
        self.store._mutate(write)
