"""Reconcile only the exact current session's already terminated native resources."""

from ahc.adapters.native_retirement_observer import NativeRetirementObserver
from ahc.domain.sessions import ContractError

from .backend_custody import BackendCustody
from .child_backend_recovery import recover_child_backends


def retire_native_custody(store, expected, workspace_id):
    """No service stop, workload launch, queue rewrite, or foreign-owner cleanup."""
    if store.read_current(expected.session.ref.virtual_session_id) != expected:
        raise ContractError("native retirement expected snapshot is stale")
    observer = NativeRetirementObserver()
    recovered = list(recover_child_backends(store, expected.session, workspace_id, observer))
    for role in ("backend", "python-workspace"):
        custody = BackendCustody(store, resource_role=role)
        lease = custody.acquire(expected.session, workspace_id)
        try:
            if store.read_current(expected.session.ref.virtual_session_id) != expected:
                raise ContractError("native retirement expected snapshot changed")
            recovered.extend(custody.recover(lease, observer))
        finally:
            lease.close()
    return tuple(recovered)
