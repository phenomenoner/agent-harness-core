"""Process-control infrastructure used by the foreground lifecycle."""

from .identity import (
    ProcessIdentityError,
    StaleProcessIdentity,
    current_identity,
    identity_for_pid,
    identity_for_process,
    identity_matches,
    require_identity,
)
from .lease import InstanceLease, LeaseUnavailable

__all__ = [
    "InstanceLease",
    "LeaseUnavailable",
    "ProcessIdentityError",
    "StaleProcessIdentity",
    "current_identity",
    "identity_for_pid",
    "identity_for_process",
    "identity_matches",
    "require_identity",
]
