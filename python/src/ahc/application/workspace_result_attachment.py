"""Validate an explicit result selection without sending or publishing anything."""

from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def select_attachment(workspace, artifacts, arguments):
    try:
        if (type(arguments) is not dict
                or not {"expected", "artifact_id", "filename"} <= set(arguments)
                or set(arguments) - {"expected", "artifact_id", "filename",
                                     "delivery_mode", "caption"}):
            raise ContractError("attachment selection fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        if expected != workspace.identity or artifacts.store is not workspace.store:
            raise ContractError("attachment selection workspace mismatch")
        selected = ResultArtifact(arguments["artifact_id"], arguments["filename"],
                                  arguments.get("delivery_mode", "document"),
                                  arguments.get("caption", ""))
        workspace.store.require_workspace(expected)
        artifact = artifacts.read(expected.session, selected.artifact_id)
        if artifact is None:
            raise ContractError("attachment selection unavailable")
        if selected.delivery_mode == "photo":
            from ahc.domain.channel_photo import validate_photo

            validate_photo(artifact[1], artifact[0].media_type)
        return {"disposition": "observed", "unresolved_effects": False,
                "result": selected.to_dict()}
    except Exception:
        return {"disposition": "rejected", "reason": "attachment-selection-unavailable",
                "unresolved_effects": False}
