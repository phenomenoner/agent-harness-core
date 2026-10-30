"""Read bounded artifact chunks through the current workspace identity."""

import base64

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def read_chunk(workspace, artifacts, arguments):
    try:
        if type(arguments) is not dict or set(arguments) != {
                "expected", "artifact_id", "offset", "length"}:
            raise ContractError("artifact read fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        if expected != workspace.identity or artifacts.store is not workspace.store:
            raise ContractError("artifact read workspace mismatch")
        offset, length = arguments["offset"], arguments["length"]
        if (type(offset) is not int or offset < 0
                or type(length) is not int or not 1 <= length <= 4096):
            raise ContractError("artifact read range invalid")
        workspace.store.require_workspace(expected)
        artifact = artifacts.read(expected.session, arguments["artifact_id"])
        if artifact is None:
            raise ContractError("artifact unavailable")
        manifest, content = artifact
        if offset > len(content):
            raise ContractError("artifact offset exceeds size")
        chunk = content[offset:offset + length]
        return {"disposition": "observed", "unresolved_effects": False, "result": {
            "artifact_id": manifest.artifact_id, "media_type": manifest.media_type,
            "byte_count": manifest.byte_count, "offset": offset,
            "next_offset": offset + len(chunk), "eof": offset + len(chunk) == len(content),
            "encoding": "base64", "data": base64.b64encode(chunk).decode("ascii")}}
    except Exception:
        return {"disposition": "rejected", "unresolved_effects": False,
                "reason": "artifact-read-unavailable"}
