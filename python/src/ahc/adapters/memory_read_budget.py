"""Conversation-local read budget; no provider, workspace or write authority."""

import subprocess
import time
from ahc.domain.adaptive_time_budget import deadline_value
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class _Budget:
    deadline: float
    clock: object
    cancelled: object


_BUDGET = ContextVar("ahc_memory_read_budget", default=None)


@contextmanager
def memory_read_budget(deadline, *, clock=time.monotonic, cancelled=lambda: False):
    token = _BUDGET.set(_Budget(deadline, clock, cancelled))
    try:
        yield
    finally:
        _BUDGET.reset(token)


def run_read_worker(command, *, timeout, **options):
    budget = _BUDGET.get()
    if budget is None:
        return subprocess.run(command, timeout=timeout, **options)

    def remaining():
        value = deadline_value(budget.deadline) - budget.clock()
        if value <= 0 or budget.cancelled():
            raise subprocess.TimeoutExpired(command, max(0, value))
        return value

    remaining()  # Expiry or cancellation cannot start a new worker.
    data = options.pop("input")
    if options.pop("capture_output", False) is not True:
        raise ValueError("memory read requires captured worker output")
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, **options)
    drained = False
    try:
        while True:
            try:
                output, error = process.communicate(input=data, timeout=min(.25, remaining()))
            except subprocess.TimeoutExpired:
                data = None  # communicate retains input/output across a bounded wait.
                remaining()
                continue
            drained = True
            remaining()  # An observed cancellation cannot be retried into success.
            return subprocess.CompletedProcess(command, process.returncode, output, error)
    except BaseException:
        if not drained:
            try:
                process.kill()
            finally:
                try:
                    process.communicate(timeout=5)
                    drained = True
                except (OSError, subprocess.TimeoutExpired):
                    pass  # Do not turn uncertain cleanup into a successful resource read.
        raise
    finally:
        if drained:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
