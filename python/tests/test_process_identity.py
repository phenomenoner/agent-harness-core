from __future__ import annotations

import subprocess
import sys

import pytest

from ahc import ProcessIdentity
from ahc.infrastructure.process import (
    identity_for_pid,
    identity_matches,
)


def test_identity_requires_matching_start_time() -> None:
    process = subprocess.Popen((sys.executable, "-c", "import time; time.sleep(30)"))
    try:
        identity = identity_for_pid(process.pid)
        assert identity_matches(identity)
        stale = ProcessIdentity(identity.pid, identity.start_time + 1.0)
        assert not identity_matches(stale)
        assert process.poll() is None
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=2)


@pytest.mark.parametrize(
    "value",
    [
        {"pid": True, "start_time": 1.0},
        {"pid": 1.5, "start_time": 1.0},
        {"pid": 1, "start_time": True},
        {"pid": 1, "start_time": float("nan")},
    ],
)
def test_process_identity_rejects_coercive_or_non_finite_values(value) -> None:
    with pytest.raises(ValueError, match="process identity|start_time"):
        ProcessIdentity.from_dict(value)
