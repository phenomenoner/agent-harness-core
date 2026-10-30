"""Stable M0 lifecycle application surface."""

from .operator import (
    INVALID_STATE,
    LEASE_BUSY,
    NOT_RUNNING,
    OK,
    STOP_SUPERSEDED,
    STOP_TIMEOUT,
    SUPERVISOR_FAILURE,
    inspect_status,
    request_stop,
)
from .supervisor import Supervisor

__all__ = [
    "INVALID_STATE",
    "LEASE_BUSY",
    "NOT_RUNNING",
    "OK",
    "STOP_SUPERSEDED",
    "STOP_TIMEOUT",
    "SUPERVISOR_FAILURE",
    "Supervisor",
    "inspect_status",
    "request_stop",
]
