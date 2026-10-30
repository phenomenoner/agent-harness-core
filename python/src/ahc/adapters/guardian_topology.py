"""Admission check for a guardian that must outlive its controller process.

Job breakaway is not enabled or inferred. A Job-owned controller needs a separately
qualified guardian launch arrangement before this adapter can create a container.
"""

import ctypes
import os
from ctypes import wintypes

from .docker_workspace import WorkspaceUnavailable


def require_independent_guardian_host():
    if os.name != "nt":
        raise WorkspaceUnavailable("guardian host requires qualified Windows topology")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.argtypes = []
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.IsProcessInJob.argtypes = [
        wintypes.HANDLE,
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.BOOL),
    ]
    kernel.IsProcessInJob.restype = wintypes.BOOL
    member = wintypes.BOOL()
    if not kernel.IsProcessInJob(kernel.GetCurrentProcess(), None, ctypes.byref(member)):
        raise WorkspaceUnavailable("guardian host Job membership is unverified")
    if member.value:
        raise WorkspaceUnavailable("guardian launch inside a controller Job is not qualified")
