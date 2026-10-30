"""Explicit pre-create refusal; never inferred from transport loss or unit state."""

import json

from ahc.domain.backend_creation import BackendCreationRejected
from ahc.domain.sessions import ContractError


def create_or_resolve_rejection(custody, lease, workspace, runner, command, lifetime, *, attempt=None):
    try:
        if attempt is not None:
            from ahc.domain.native_process_identity import matches_attempt
            unit = runner.create_attempt(attempt, command, lifetime_seconds=lifetime)
            if not matches_attempt(unit, attempt):
                raise ContractError("created backend differs from durable attempt")
            return unit
        return runner.create(command, lifetime_seconds=lifetime)
    except BackendCreationRejected:
        resolve_rejected_intent(custody, lease, workspace)
        raise


def resolve_rejected_intent(custody, lease, workspace):
    # Caller holds the lease guard and execution lock across create and resolution.
    custody._require(lease)
    expected = json.dumps(workspace.to_dict(), sort_keys=True)

    def resolve(connection):
        row = connection.execute(
            "DELETE FROM backend_creation_intents "
            "WHERE owner_key=? AND workspace_json=? AND state='pending'",
            (lease.key, expected),
        )
        if row.rowcount != 1:
            raise ContractError("rejected creation intent identity changed")

    custody.repository._mutate(resolve)
