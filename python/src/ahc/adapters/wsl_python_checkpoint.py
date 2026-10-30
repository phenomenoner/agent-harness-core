"""Portable checkpoint owner and generation validation for the WSL interpreter."""

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceCheckpoint


def validate_checkpoint(identity, checkpoint, *, initial=False):
    if checkpoint is None:
        return
    if type(checkpoint) is not WorkspaceCheckpoint:
        raise ContractError("workspace restore requires a portable checkpoint")
    origin = checkpoint.origin
    if origin.session != identity.session or origin.workspace_id != identity.workspace_id:
        raise ContractError("checkpoint owner mismatch")
    if (initial and origin.generation >= identity.generation) or (
        not initial
        and (
            origin.generation > identity.generation
            or (origin.generation == identity.generation and origin.revision > identity.revision)
        )
    ):
        raise ContractError("checkpoint must belong to an older workspace generation")
