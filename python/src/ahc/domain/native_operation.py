"""Native operation receipt values shared by adapters and durable custody."""

import json
import re
from dataclasses import dataclass

from .sessions import ContractError
from .skill_values import canonical_json

_STATES = {"accepted", "running", "succeeded", "failed", "cancelled",
           "timed_out", "indeterminate"}
_TERMINAL = {"succeeded", "failed", "cancelled", "timed_out"}


@dataclass(frozen=True, slots=True)
class NativeOperationReceipt:
    operation_id: str
    state: str
    certainty: str
    revision: int
    reconciliation_required: bool
    payload_json: str

    @classmethod
    def parse(cls, payload, operation_id):
        if (type(operation_id) is not str
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,127}", operation_id)):
            raise ContractError("native operation identifier invalid")
        if type(payload) is not dict:
            raise ContractError("native operation receipt missing")
        if payload.get("operation") != {"type": "operation", "value": operation_id}:
            raise ContractError("native operation identity mismatch")
        state, certainty = payload.get("state"), payload.get("certainty")
        revision = payload.get("record_revision")
        reconcile = payload.get("reconciliation_required")
        if (type(state) is not str or state not in _STATES
                or certainty not in ("certain", "indeterminate")
                or type(revision) is not int or revision < 0
                or type(reconcile) is not bool):
            raise ContractError("native operation receipt invalid")
        if ((state == "indeterminate") != (certainty == "indeterminate")
                or (state == "indeterminate" and not reconcile)):
            raise ContractError("native operation uncertainty inconsistent")
        encoded = canonical_json(payload)
        if len(encoded) > 65536:
            raise ContractError("native operation receipt exceeds bound")
        return cls(operation_id, state, certainty, revision, reconcile, encoded.decode())

    @property
    def terminal(self):
        return self.state in _TERMINAL and not self.reconciliation_required

    def record_payload(self):
        # Upstream status omits receipt artifacts; reconcile may attach them
        # without changing the underlying OperationRecord revision.
        value = json.loads(self.payload_json)
        return {key: value.get(key) for key in (
            "operation", "state", "certainty", "record_revision",
            "reconciliation_required", "result", "failure")}
