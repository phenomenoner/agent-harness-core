"""Explicit readback of retained native operations; never redispatches a child."""

import json

from ahc.domain.sessions import ContractError, _identifier

from .aar_native_operations import NativeAarOperations


class NativeAarChildRecovery:
    def __init__(self, native, parent, child_id, client, binding, *,
                 cancel_grants=(), reconcile_grants=()):
        for grants in (cancel_grants, reconcile_grants):
            if type(grants) is not tuple:
                raise ContractError("native recovery grants must be explicit immutable tuples")
            for grant in grants:
                _identifier(grant, "native recovery grant")
        retained = native.read(parent, child_id)
        if retained is None or retained["operation_id"] is None:
            raise ContractError("native recovery operation unknown; do not replay")
        expected = retained["binding"]
        actual = {
            "server_id": client.server_id,
            "runtime_generation": binding.runtime_generation,
            "capability_digest": binding.pin.capability_digest,
            "tool_surface_digest": binding.pin.tool_surface_digest,
            "schema_bundle_digest": binding.pin.schema_bundle_digest,
            "principal_id": binding.principal_id,
            "session_id": binding.session_id,
        }
        if client.session != parent.session or any(
            type(expected[key]) is not type(value) or expected[key] != value
            for key, value in actual.items()
        ):
            raise ContractError("native recovery runtime binding mismatch")
        self.native, self.parent, self.child_id = native, parent, child_id
        self.expected = expected
        self.operation = NativeAarOperations(client, binding, retained["operation_id"])
        self.operation.last_receipt = retained["receipt"]
        self.grants = {"cancel": frozenset(cancel_grants),
                       "reconcile": frozenset(reconcile_grants)}

    def status(self, call_id, *, deadline_unix_ms, deadline, cancelled=lambda: False):
        """One caller-admitted status read; caller owns client cleanup and fresh scope."""
        result = self.operation.status(
            call_id, deadline_unix_ms=deadline_unix_ms, deadline=deadline,
            cancelled=cancelled,
        )
        return self._retain(result)

    def cancel(self, call_id, context, *, deadline, cancelled=lambda: False):
        return self._mutation("cancel", call_id, context, deadline, cancelled)

    def reconcile(self, call_id, context, *, deadline, cancelled=lambda: False):
        return self._mutation("reconcile", call_id, context, deadline, cancelled)

    def _mutation(self, method, call_id, context, deadline, cancelled):
        if (type(context) is not dict or type(context.get("grant_id")) is not str
                or context["grant_id"] not in self.grants[method]):
            raise ContractError("native recovery mutation grant not admitted")
        retained = self.native.read(self.parent, self.child_id)
        if (retained is None or retained["binding"] != self.expected
                or retained["operation_id"] != self.operation.operation_id):
            raise ContractError("native recovery retained identity changed")
        self.operation.last_receipt = retained["receipt"]
        result = getattr(self.operation, method)(
            call_id, context, deadline=deadline, cancelled=cancelled)
        return self._retain(result)

    def _retain(self, result):
        if result["status"] == "observed" and result["receipt"] is not None:
            self.native.observe_recovery(
                self.parent, self.child_id, self.expected,
                json.loads(result["receipt"].payload_json),
            )
        return result
