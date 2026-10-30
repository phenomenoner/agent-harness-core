"""Optional task-recovery facts, bound to the original session and workspace."""

from .artifacts import ArtifactManifest
from .sessions import ContractError
from .turns import _sha256_hex
from .workspace import WorkspaceIdentity


def confirmed_work_projection(value, request):
    if value is None:
        return {}
    try:
        if type(value) is not dict or set(value) != {"workspace", "checkpoint", "artifacts"}:
            raise ContractError("confirmed work fields invalid")

        def require_owner(raw):
            owner = WorkspaceIdentity.from_dict(raw)
            if (owner.session != request.session or owner.workspace_id != request.workspace_scope):
                raise ContractError("confirmed work owner mismatch")
            return owner

        workspace = None if value["workspace"] is None else require_owner(value["workspace"]).to_dict()
        checkpoint = value["checkpoint"]
        if checkpoint is not None:
            if type(checkpoint) is not dict or set(checkpoint) != {"origin", "digest"}:
                raise ContractError("confirmed checkpoint fields invalid")
            _sha256_hex(checkpoint["digest"])
            checkpoint = {"origin": require_owner(checkpoint["origin"]).to_dict(),
                          "digest": checkpoint["digest"]}
        if type(value["artifacts"]) is not list:
            raise ContractError("confirmed artifacts invalid")
        artifacts = [ArtifactManifest.from_dict(raw) for raw in value["artifacts"]]
        for manifest in artifacts:
            require_owner(manifest.producer.to_dict())
        if len({manifest.artifact_id for manifest in artifacts}) != len(artifacts):
            raise ContractError("duplicate confirmed artifact")
        if workspace is None and checkpoint is None and not artifacts:
            return {}
        return {"confirmed_work": {"workspace": workspace, "checkpoint": checkpoint,
                                   "artifacts": [manifest.to_dict() for manifest in artifacts]}}
    except (ContractError, TypeError, KeyError, ValueError):
        return {}  # Optional recovery metadata cannot invalidate the actual result.
