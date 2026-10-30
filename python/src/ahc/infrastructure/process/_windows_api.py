"""Low-level Windows kernel bindings for process ownership primitives."""

from __future__ import annotations

import ctypes
import math
from ctypes import wintypes

import psutil

from ahc.domain import ProcessIdentity

PROCESS_TERMINATE = 0x0001
PROCESS_SET_QUOTA = 0x0100
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
SYNCHRONIZE = 0x00100000
CREATE_SUSPENDED = 0x00000004

_THREAD_SUSPEND_RESUME = 0x0002
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
_INVALID_RESUME_COUNT = 0xFFFFFFFF
_FILETIME_UNIX_EPOCH = 116_444_736_000_000_000
_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
_GET_LAST_ERROR = ctypes.get_last_error  # type: ignore[attr-defined]


class NativeProcessError(RuntimeError):
    """A native process or Job Object operation failed closed."""


class NativeIdentityMismatch(NativeProcessError):
    """A stable process handle does not identify the expected generation."""


class _IOCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IOCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _BasicAccountingInformation(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", ctypes.c_longlong),
        ("TotalKernelTime", ctypes.c_longlong),
        ("ThisPeriodTotalUserTime", ctypes.c_longlong),
        ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
        ("TotalPageFaultCount", wintypes.DWORD),
        ("TotalProcesses", wintypes.DWORD),
        ("ActiveProcesses", wintypes.DWORD),
        ("TotalTerminatedProcesses", wintypes.DWORD),
    ]


def _bind() -> None:
    _KERNEL32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _KERNEL32.OpenProcess.restype = wintypes.HANDLE
    _KERNEL32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    _KERNEL32.GetProcessTimes.restype = wintypes.BOOL
    _KERNEL32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _KERNEL32.TerminateProcess.restype = wintypes.BOOL
    _KERNEL32.CloseHandle.argtypes = [wintypes.HANDLE]
    _KERNEL32.CloseHandle.restype = wintypes.BOOL
    _KERNEL32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _KERNEL32.CreateJobObjectW.restype = wintypes.HANDLE
    _KERNEL32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    _KERNEL32.SetInformationJobObject.restype = wintypes.BOOL
    _KERNEL32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _KERNEL32.AssignProcessToJobObject.restype = wintypes.BOOL
    _KERNEL32.IsProcessInJob.argtypes = [
        wintypes.HANDLE,
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.BOOL),
    ]
    _KERNEL32.IsProcessInJob.restype = wintypes.BOOL
    _KERNEL32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _KERNEL32.TerminateJobObject.restype = wintypes.BOOL
    _KERNEL32.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    _KERNEL32.QueryInformationJobObject.restype = wintypes.BOOL
    _KERNEL32.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _KERNEL32.OpenThread.restype = wintypes.HANDLE
    _KERNEL32.ResumeThread.argtypes = [wintypes.HANDLE]
    _KERNEL32.ResumeThread.restype = wintypes.DWORD


_bind()


def _last_error(operation: str) -> NativeProcessError:
    return NativeProcessError(f"{operation} failed with Windows error {_GET_LAST_ERROR()}")


def same_identity(left: ProcessIdentity, right: ProcessIdentity) -> bool:
    return left.pid == right.pid and math.isclose(
        left.start_time,
        right.start_time,
        rel_tol=0.0,
        abs_tol=1e-6,
    )


class NativeProcessHandle:
    """A stable Windows process object handle, not a later PID lookup."""

    def __init__(self, handle: wintypes.HANDLE, identity: ProcessIdentity) -> None:
        self._handle = handle
        self._identity = identity

    @classmethod
    def open_pid(cls, pid: int, *, access: int) -> NativeProcessHandle:
        handle = _KERNEL32.OpenProcess(access, False, pid)
        if not handle:
            raise _last_error(f"OpenProcess({pid})")
        try:
            return cls(handle, ProcessIdentity(pid=pid, start_time=_creation_time(handle)))
        except Exception:
            _KERNEL32.CloseHandle(handle)
            raise

    @classmethod
    def open_exact(cls, identity: ProcessIdentity, *, access: int) -> NativeProcessHandle:
        handle = cls.open_pid(identity.pid, access=access)
        if not same_identity(handle.identity, identity):
            handle.close()
            raise NativeIdentityMismatch(f"PID {identity.pid} is a different process generation")
        return handle

    @property
    def raw(self) -> wintypes.HANDLE:
        return self._handle

    @property
    def identity(self) -> ProcessIdentity:
        return self._identity

    def terminate(self, exit_code: int = 1) -> None:
        if not _KERNEL32.TerminateProcess(self._handle, exit_code):
            raise _last_error(f"TerminateProcess({self.identity.pid})")

    def close(self) -> None:
        if self._handle:
            _KERNEL32.CloseHandle(self._handle)
            self._handle = wintypes.HANDLE()


def _creation_time(handle: wintypes.HANDLE) -> float:
    values = [wintypes.FILETIME() for _ in range(4)]
    if not _KERNEL32.GetProcessTimes(handle, *(ctypes.byref(value) for value in values)):
        raise _last_error("GetProcessTimes")
    creation = values[0]
    ticks = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
    return (ticks - _FILETIME_UNIX_EPOCH) / 10_000_000


def create_kill_on_close_job() -> wintypes.HANDLE:
    handle = _KERNEL32.CreateJobObjectW(None, None)
    if not handle:
        raise _last_error("CreateJobObjectW")
    information = _ExtendedLimitInformation()
    information.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not _KERNEL32.SetInformationJobObject(
        handle,
        _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        _KERNEL32.CloseHandle(handle)
        raise _last_error("SetInformationJobObject")
    return handle


def assign_to_job(job: wintypes.HANDLE, process: NativeProcessHandle) -> None:
    if not _KERNEL32.AssignProcessToJobObject(job, process.raw):
        raise _last_error("AssignProcessToJobObject")
    if not process_is_in_job(job, process):
        raise NativeProcessError("process was not assigned to the expected Job Object")


def process_is_in_job(job: wintypes.HANDLE, process: NativeProcessHandle) -> bool:
    assigned = wintypes.BOOL()
    if not _KERNEL32.IsProcessInJob(process.raw, job, ctypes.byref(assigned)):
        raise _last_error("IsProcessInJob")
    return bool(assigned.value)


def terminate_job(job: wintypes.HANDLE) -> None:
    if not _KERNEL32.TerminateJobObject(job, 1):
        raise _last_error("TerminateJobObject")


def active_job_processes(job: wintypes.HANDLE) -> int:
    information = _BasicAccountingInformation()
    returned = wintypes.DWORD()
    if not _KERNEL32.QueryInformationJobObject(
        job,
        _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
        ctypes.byref(information),
        ctypes.sizeof(information),
        ctypes.byref(returned),
    ):
        raise _last_error("QueryInformationJobObject")
    return int(information.ActiveProcesses)


def close_handle(handle: wintypes.HANDLE) -> None:
    if handle:
        _KERNEL32.CloseHandle(handle)


def resume_initial_threads(pid: int) -> None:
    thread_ids = [thread.id for thread in psutil.Process(pid).threads()]
    if not thread_ids:
        raise NativeProcessError(f"suspended PID {pid} has no observable thread")
    for thread_id in thread_ids:
        handle = _KERNEL32.OpenThread(_THREAD_SUSPEND_RESUME, False, thread_id)
        if not handle:
            raise _last_error(f"OpenThread({thread_id})")
        try:
            if _KERNEL32.ResumeThread(handle) == _INVALID_RESUME_COUNT:
                raise _last_error(f"ResumeThread({thread_id})")
        finally:
            _KERNEL32.CloseHandle(handle)
