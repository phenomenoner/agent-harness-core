"""Manifest-only capture with effect-local publication failure semantics."""

from ahc.domain.artifacts import ArtifactManifest, ArtifactNotPublished
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity
from ahc.domain.workspace_inspection import inspection_names


def capture(workspace, publisher, arguments):
    try:
        if type(arguments) is not dict or set(arguments) != {
            "expected",
            "name",
            "media_type",
            "lineage",
        }:
            raise ContractError("artifact tool fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        name = inspection_names((arguments["name"],))[0]
        if expected != workspace.identity or type(arguments["lineage"]) is not list:
            raise ContractError("artifact tool identity or lineage invalid")
        lineage = tuple(arguments["lineage"])
        ArtifactManifest(expected, "0" * 64, arguments["media_type"], 0, lineage)
    except ContractError:
        return {"disposition": "rejected", "reason": "invalid-artifact-request",
                "unresolved_effects": False,
                "recovery": "name is a Python variable, not a pathname; lineage contains "
                            "artifact IDs, not message IDs. Correct the request or deliver text."}
    try:
        manifest = workspace.capture_artifact(
            expected, name, publisher, arguments["media_type"], lineage
        )
        return {
            "disposition": "observed",
            "unresolved_effects": False,
            "result": {"artifact_id": manifest.artifact_id, "manifest": manifest.to_dict()},
        }
    except ArtifactNotPublished as error:
        available = not workspace.closed and not workspace.unavailable
        return {"disposition": "rejected", "reason": error.reason,
                "unresolved_effects": False, "workspace_available": available,
                "recovery": ("Nothing was published. Select an exact bytes or str variable, "
                             "use the current identity, or finish with the confirmed text."
                             if available else
                             "Nothing was published. This workspace is unavailable; finish "
                             "with already confirmed facts. Its owner will handle cleanup.")}
    except Exception:
        return {
            "disposition": "unknown",
            "unresolved_effects": True,
            "reason": "artifact-publication-unverified",
        }
