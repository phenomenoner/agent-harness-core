import base64
from copy import deepcopy
from hashlib import sha256

import pytest
from test_aar_native_operations import setup

from ahc.adapters.aar_native_artifacts import resolve_native_artifact
from ahc.domain.sessions import ContractError


def artifact_setup(content=b"native result", redacted=False):
    operations, transport = setup()
    reference = {"schema_version": "aar.artifact.v1",
                 "artifact": {"type": "artifact", "value": "native-artifact"},
                 "digest": "sha256:" + sha256(content).hexdigest(),
                 "media_type": "application/octet-stream", "size_bytes": len(content),
                 "created_by": {"type": "operation", "value": "native-op"},
                 "redacted": redacted}
    transport.payload["artifacts"] = [reference]
    operations.status("read", deadline_unix_ms=2000, deadline=10)
    transport.payload = {"artifact_id": "native-artifact", "digest": reference["digest"],
                         "media_type": reference["media_type"], "size_bytes": len(content),
                         "redacted": redacted, "content_base64": base64.b64encode(content).decode(),
                         "failure": None}
    return operations, transport, reference


def resolve(operations, reference, **kwargs):
    return resolve_native_artifact(operations, "resolve", reference, max_bytes=16384,
                                    deadline_unix_ms=2000, deadline=10, **kwargs)


@pytest.mark.parametrize("content", [b"", b"native result", bytes(range(256)) * 64],
                         ids=["empty", "small", "maximum"])
def test_exact_reference_and_bytes(content):
    operations, transport, reference = artifact_setup(content)
    result = resolve(operations, reference)
    assert result["status"] == "observed"
    assert result["artifact"].content == content
    arguments = transport.calls[-1]["arguments"]
    assert arguments["created_by_operation_id"] == "native-op"
    assert arguments["context"]["session_id"] == "native-session"
    assert not arguments["allow_redacted"]


@pytest.mark.parametrize("field,value", [
    ("artifact_id", "other"), ("digest", "sha256:" + "f" * 64),
    ("media_type", "text/plain"), ("size_bytes", True), ("redacted", True),
    ("content_base64", "!" * 20),
    ("content_base64", base64.b64encode(b"wrong-content").decode()),
    ("failure", {"code": "FAILURE"}),
])
def test_bad_response_never_returns_bytes(field, value):
    operations, transport, reference = artifact_setup()
    transport.payload[field] = value
    assert resolve(operations, reference) == {"status": "unknown", "artifact": None}
    assert len(transport.calls) == 2


def test_redacted_requires_explicit_disclosure_and_foreign_reference_never_dispatches():
    operations, transport, reference = artifact_setup(redacted=True)
    with pytest.raises(ContractError):
        resolve(operations, reference)
    assert len(transport.calls) == 1
    assert resolve(operations, reference, allow_redacted=True)["status"] == "observed"
    foreign = deepcopy(reference)
    foreign["created_by"]["value"] = "other"
    with pytest.raises(ContractError):
        resolve(operations, foreign, allow_redacted=True)
    assert len(transport.calls) == 2


def test_unretained_and_oversized_reference_rejected_before_dispatch():
    operations, transport, reference = artifact_setup()
    reference["artifact"]["value"] = "not-retained"
    with pytest.raises(ContractError, match="not retained"):
        resolve(operations, reference)
    reference["size_bytes"] = 16385
    with pytest.raises(ContractError, match="disclosure"):
        resolve(operations, reference)
    assert len(transport.calls) == 1
