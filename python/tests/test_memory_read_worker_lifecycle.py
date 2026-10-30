"""Small real worker probes: old cutoff exceeded and cancellation reaps its process."""

import subprocess
import sys
import threading
import time

import pytest

from ahc.adapters.memory_read_budget import memory_read_budget, run_read_worker


def options(tmp_path):
    return dict(input=b'',cwd=tmp_path,env={},capture_output=True,timeout=.01)


def test_cold_read_exceeds_old_per_call_cutoff_within_parent_budget(tmp_path):
    with memory_read_budget(time.monotonic()+5):
        result=run_read_worker([sys.executable,'-I','-c',
            "import time;time.sleep(.15);print('ready')"],**options(tmp_path))
    assert result.returncode == 0 and result.stdout.strip() == b'ready'


def test_cancelled_read_reaps_worker_without_waiting_for_parent_expiry(tmp_path, monkeypatch):
    original=subprocess.Popen
    workers=[]
    cancelled=threading.Event()
    def create(*args,**kwargs):
        worker=original(*args,**kwargs);workers.append(worker)
        cancelled.set()  # Deterministically cancel after dispatch, before collection.
        return worker
    monkeypatch.setattr(subprocess,'Popen',create)
    started=time.monotonic()
    with memory_read_budget(started+60,cancelled=cancelled.is_set), pytest.raises(subprocess.TimeoutExpired):
        run_read_worker([sys.executable,'-I','-c',"import time;time.sleep(40)"],**options(tmp_path))
    assert time.monotonic()-started < 5
    assert len(workers) == 1 and workers[0].poll() is not None
