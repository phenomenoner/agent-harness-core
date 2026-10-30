"""Pure restore transformation; creating this value never activates a runtime."""

from dataclasses import dataclass

from .sessions import ContractError
from .skill_values import digest_json
from .turns import _sha256_hex
from .working_sets import WorkingSetSnapshot


@dataclass(frozen=True, slots=True)
class RecoveryTransition:
    saved: WorkingSetSnapshot
    source: WorkingSetSnapshot
    backup_digest: str
    source_database_digest: str

    def __post_init__(self):
        if (type(self.saved) is not WorkingSetSnapshot
                or type(self.source) is not WorkingSetSnapshot):
            raise ContractError("recovery requires saved and current source snapshots")
        _sha256_hex(self.backup_digest)
        _sha256_hex(self.source_database_digest)
        before, current = self.saved.session, self.source.session
        if (before.ref.virtual_session_id != current.ref.virtual_session_id
                or before.agent_id != current.agent_id or before.route_id != current.route_id):
            raise ContractError("recovery session owner mismatch")
        if before.ref.generation == current.ref.generation and before != current:
            raise ContractError("recovery generation identity conflict")
        if self.saved.status not in {"active", "interrupted"}:
            raise ContractError("terminal saved session remains historical")

    @property
    def recovered(self):
        anchor = max((self.saved.session, self.source.session), key=lambda s: s.ref.generation)
        # Session lineage follows the highest actual identity. Working content lineage
        # follows the saved snapshot; provenance records both rather than inventing history.
        return self.saved.continue_as(anchor.successor(), "restore")

    def to_dict(self):
        return {"schema": "ahc.runtime-recovery-transition.v1",
                "backup_digest": self.backup_digest,
                "source_database_digest": self.source_database_digest,
                "saved_snapshot_digest": self.saved.snapshot_digest,
                "source_snapshot_digest": self.source.snapshot_digest,
                "source_snapshot": self.source.to_dict(),
                "saved_session": self.saved.session.to_dict(),
                "source_session": self.source.session.to_dict(),
                "recovered_session": self.recovered.session.to_dict(),
                "recovered_snapshot_digest": self.recovered.snapshot_digest,
                "automatic_replay": False}

    @property
    def digest(self):
        return digest_json(self.to_dict())
