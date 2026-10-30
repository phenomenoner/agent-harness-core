"""Native identity/receipt custody attached to the existing exact child claim."""

import json

from ahc.domain.native_operation import NativeOperationReceipt
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json


class NativeChildRepository:
    def __init__(self, children):
        self.children, self.store = children, children.store

    def _child(self, connection, parent, child_id, token=None):
        row, envelope, _ = self.children._row(connection, parent, child_id)
        if envelope.execution_kind != "minion":
            raise ContractError("native custody requires minion child")
        if token is not None:
            self.children._require_parent(connection, parent)
            if (row["state"] != "claimed" or row["claim_token"] != token
                    or self.children._now() >= envelope.deadline_unix_ms):
                raise ContractError("native child claim is not current")
        return envelope

    @staticmethod
    def _binding(value):
        required = {"server_id", "runtime_generation", "capability_digest",
                    "tool_surface_digest", "schema_bundle_digest", "catalog_digest",
                    "profile_digest", "submission_digest", "principal_id", "session_id"}
        if type(value) is not dict or set(value) != required:
            raise ContractError("native child binding fields invalid")
        if type(value["runtime_generation"]) is not int or value["runtime_generation"] < 1:
            raise ContractError("native child runtime generation invalid")
        for key, item in value.items():
            if key == "runtime_generation":
                continue
            if type(item) is not str or not 1 <= len(item) <= 128:
                raise ContractError("native child binding value invalid")
            if key.endswith("digest"):
                if (len(item) != 71 or not item.startswith("sha256:")
                        or any(char not in "0123456789abcdef" for char in item[7:])):
                    raise ContractError("native child digest invalid")
        return canonical_json(value).decode()

    def _decode(self, row, envelope):
        if row is None:
            return None
        binding = json.loads(row["binding_json"])
        if (row["envelope_digest"] != envelope.request_digest
                or self._binding(binding) != row["binding_json"]
                or digest_json(binding) != row["binding_digest"]):
            raise ContractError("native child retained binding corrupted")
        receipt = None
        if row["receipt_json"] is not None:
            payload = json.loads(row["receipt_json"])
            receipt = NativeOperationReceipt.parse(payload, row["operation_id"])
            if (receipt.payload_json != row["receipt_json"]
                    or digest_json(payload) != row["receipt_digest"]):
                raise ContractError("native child retained receipt corrupted")
        elif row["operation_id"] is not None or row["receipt_digest"] is not None:
            raise ContractError("native child receipt identity incomplete")
        return {"binding": binding, "operation_id": row["operation_id"],
                "receipt": receipt, "submission_reserved": True}

    def read(self, parent, child_id):
        with self.store._read() as connection:
            envelope = self._child(connection, parent, child_id)
            row = connection.execute("SELECT * FROM runtime_native_children WHERE child_id=?",
                                     (child_id,)).fetchone()
            return self._decode(row, envelope)

    def reserve(self, parent, child_id, token, binding):
        _identifier(token, "native child claim token")
        encoded = self._binding(binding)
        binding = json.loads(encoded)

        def write(connection):
            envelope = self._child(connection, parent, child_id, token)
            if connection.execute("SELECT 1 FROM runtime_native_children WHERE child_id=?",
                                  (child_id,)).fetchone() is not None:
                raise ContractError("native child submission already reserved; do not replay")
            connection.execute("INSERT INTO runtime_native_children VALUES(?,?,?,?,NULL,NULL,NULL)",
                (child_id, envelope.request_digest, encoded, digest_json(binding)))
        self.store._mutate(write)

    def observe(self, parent, child_id, token, payload):
        _identifier(token, "native child claim token")
        self._observe(parent, child_id, payload, token=token)

    def observe_recovery(self, parent, child_id, binding, payload):
        """Retain a known operation observation without reviving child authority."""
        encoded = self._binding(binding)
        self._observe(parent, child_id, payload, expected_binding=encoded)

    def _observe(self, parent, child_id, payload, *, token=None, expected_binding=None):
        if type(payload) is not dict or type(payload.get("operation")) is not dict:
            raise ContractError("native operation receipt missing")
        receipt = NativeOperationReceipt.parse(payload, payload["operation"].get("value"))
        if type(receipt.operation_id) is not str or not receipt.operation_id:
            raise ContractError("native operation identifier missing")

        def write(connection):
            envelope = self._child(connection, parent, child_id, token)
            row = connection.execute("SELECT * FROM runtime_native_children WHERE child_id=?",
                                     (child_id,)).fetchone()
            retained = self._decode(row, envelope)
            if retained is None:
                raise ContractError("native submission was not reserved")
            if expected_binding is not None and (
                row["binding_json"] != expected_binding or retained["operation_id"] is None
            ):
                raise ContractError("native recovery requires exact binding and known operation")
            previous = retained["receipt"]
            if previous is not None:
                if (receipt.operation_id != previous.operation_id
                        or receipt.revision < previous.revision
                        or (receipt.revision == previous.revision
                            and receipt.record_payload() != previous.record_payload())
                        or (previous.terminal and (not receipt.terminal
                                                  or receipt.state != previous.state))):
                    raise ContractError("native retained receipt conflicts or regresses")
            connection.execute("UPDATE runtime_native_children SET operation_id=?,receipt_json=?,"
                "receipt_digest=? WHERE child_id=?", (receipt.operation_id, receipt.payload_json,
                    digest_json(json.loads(receipt.payload_json)), child_id))
        self.store._mutate(write)
