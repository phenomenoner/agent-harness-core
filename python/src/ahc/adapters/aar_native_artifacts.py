"""Bounded native artifact disclosure with exact reference and byte verification."""

import base64
import json
import re
from dataclasses import dataclass
from hashlib import sha256

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .aar_native_binding import _DIGEST, _IDENTITY

_MEDIA = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*\Z")
# Existing MCP JSON response bound is 64 KiB including content and metadata.
# This is a supported per-artifact disclosure limit, not the AAR global limit.
MAX_NATIVE_ARTIFACT_BYTES = 16384


@dataclass(frozen=True, slots=True)
class VerifiedNativeArtifact:
    reference_json: str
    content: bytes


def resolve_native_artifact(operations, call_id, reference, *, max_bytes,
                            deadline_unix_ms, deadline, allow_redacted=False,
                            cancelled=lambda: False):
    """Resolve only a reference actually retained by this operation adapter."""
    if (type(max_bytes) is not int or not 0 <= max_bytes <= MAX_NATIVE_ARTIFACT_BYTES
            or type(allow_redacted) is not bool):
        raise ContractError("native artifact disclosure bound invalid")
    encoded = canonical_json(reference)
    reference = json.loads(encoded)
    _validate_reference(reference, operations.operation_id, max_bytes, allow_redacted)
    receipt = operations.last_receipt
    if receipt is None or reference not in json.loads(receipt.payload_json).get("artifacts", []):
        raise ContractError("native artifact is not retained by the operation")
    arguments = {
        "context": operations.binding.read_context(deadline_unix_ms=deadline_unix_ms),
        "artifact_id": reference["artifact"]["value"],
        "digest": reference["digest"], "media_type": reference["media_type"],
        "size_bytes": reference["size_bytes"],
        "created_by_operation_id": operations.operation_id,
        "redacted": reference["redacted"], "max_bytes": max_bytes,
        "allow_redacted": allow_redacted,
    }
    try:
        observed = operations.client.invoke(operations.client.session, call_id,
            "aar_artifact_resolve", arguments, deadline=deadline, cancelled=cancelled)
        if observed.get("status") == "rejected":
            return {"status": "rejected", "artifact": None}
        if observed.get("status") != "observed":
            raise ContractError("native artifact response unverified")
        result = observed["result"]["structuredContent"]
        if type(result) is not dict or result.get("failure") is not None:
            raise ContractError("native artifact resolution failed")
        for field in ("artifact_id", "digest", "media_type", "size_bytes", "redacted"):
            if (type(result.get(field)) is not type(arguments[field])
                    or result[field] != arguments[field]):
                raise ContractError("native artifact response identity mismatch")
        body = result.get("content_base64")
        size = reference["size_bytes"]
        if type(body) is not str or len(body) != 4 * ((size + 2) // 3):
            raise ContractError("native artifact encoded length mismatch")
        content = base64.b64decode(body, validate=True)
        if (len(content) != size or base64.b64encode(content).decode() != body
                or "sha256:" + sha256(content).hexdigest() != reference["digest"]):
            raise ContractError("native artifact content mismatch")
        return {"status": "observed", "artifact": VerifiedNativeArtifact(encoded.decode(), content)}
    except Exception:
        # Never expose unverified bytes or transport diagnostics as child output.
        return {"status": "unknown", "artifact": None}


def _validate_reference(value, operation_id, max_bytes, allow_redacted):
    if type(value) is not dict or set(value) != {
            "schema_version", "artifact", "digest", "media_type",
            "size_bytes", "created_by", "redacted"}:
        raise ContractError("native artifact reference fields invalid")
    artifact = value["artifact"]
    if (value["schema_version"] != "aar.artifact.v1"
            or type(artifact) is not dict or set(artifact) != {"type", "value"}
            or artifact["type"] != "artifact" or type(artifact["value"]) is not str
            or not _IDENTITY.fullmatch(artifact["value"])
            or value["created_by"] != {"type": "operation", "value": operation_id}):
        raise ContractError("native artifact reference identity invalid")
    if (type(value["digest"]) is not str or not _DIGEST.fullmatch(value["digest"])
            or type(value["media_type"]) is not str or len(value["media_type"]) > 128
            or not _MEDIA.fullmatch(value["media_type"])
            or type(value["size_bytes"]) is not int or not 0 <= value["size_bytes"] <= max_bytes
            or type(value["redacted"]) is not bool
            or (value["redacted"] and not allow_redacted)):
        raise ContractError("native artifact reference disclosure invalid")
