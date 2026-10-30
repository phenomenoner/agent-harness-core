"""Root lifetime ownership spans startup/cleanup and excludes offline restoration."""

import pytest
from test_runtime_core import setup

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_restore_guard import offline_restore_source
from ahc.infrastructure.working_db import WORKING_DB_FILENAME
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_shared_foregrounds_block_restore_until_both_close(tmp_path):
    repo, saved, _ = setup(tmp_path)
    other = WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("other", "other", "other"), saved.goal)
    repo.create(other)
    runtimes = [ForegroundRuntime(repo, item.session, lambda *_: None, None)
                for item in (saved, other)]
    digest = _digest(repo.root / WORKING_DB_FILENAME)
    try:
        for runtime in runtimes:
            runtime.start()
        for runtime in runtimes:
            with pytest.raises(WorkingWriterBusy):
                with offline_restore_source(repo, tmp_path / "unrelated-supervisor.lock", digest):
                    pytest.fail("foreground bypassed through a different supervisor path")
            runtime.close()
        with offline_restore_source(repo, tmp_path / "unrelated-supervisor.lock", digest):
            blocked = ForegroundRuntime(repo, saved.session, lambda *_: None, None)
            with pytest.raises(WorkingWriterBusy):
                blocked.start()
            blocked.close()
    finally:
        for runtime in runtimes:
            runtime.close()


def test_failed_cleanup_keeps_root_lease_until_verified_close(tmp_path):
    repo, saved, _ = setup(tmp_path)

    class Receiver:
        fail = True

        def start(self):
            pass

        def close(self):
            if self.fail:
                raise RuntimeError("cleanup incomplete")

    receiver = Receiver()
    runtime = ForegroundRuntime(repo, saved.session, lambda *_: None, None,
                                channel_receiver=receiver)
    runtime.start()
    try:
        with pytest.raises(RuntimeError):
            runtime.close()
        with pytest.raises(WorkingWriterBusy):
            with offline_restore_source(repo, tmp_path / "supervisor.lock",
                    _digest(repo.root / WORKING_DB_FILENAME)):
                pytest.fail("cleanup failure released root ownership")
    finally:
        receiver.fail = False
        runtime.close()
