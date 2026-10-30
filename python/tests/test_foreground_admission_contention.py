"""Only read-only selection may defer; effects remain under their original custody."""

import threading
from contextlib import contextmanager

import pytest
import test_foreground_runtime as fixture

from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt, WorkingWriterBusy


@contextmanager
def competing_writer(store):
    acquired, release = threading.Event(), threading.Event()

    def hold():
        with store.writer_lock():
            acquired.set()
            assert release.wait(5)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert acquired.wait(2)
        yield
    finally:
        release.set()
        thread.join(2)
        assert not thread.is_alive()


@pytest.mark.parametrize("phase", ["snapshot", "submission"])
def test_real_writer_contention_preserves_pending_and_recovers_once(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(fixture, "RuntimeRepository", ThreadedRuntimeRepository)
    runtime, _, invoked, request = fixture.setup(tmp_path)
    runtime.store._thread_wait_seconds = 0.02
    inbox = RuntimeInbox(runtime.store)
    runtime.ingress = LocalRuntimeIngress(
        inbox, artifact_digest="a" * 64, configuration_digest=lambda _: "b" * 64,
        workspace_scope="work",
    )
    runtime.start()
    try:
        inbox.submit(request.session, "pending", request.goal_digest, "input")
        current = runtime.store.read_current("session")
        with competing_writer(runtime.store):
            if phase == "snapshot":
                assert runtime.step() is None
            else:
                assert runtime.ingress(current, runtime.tools) is None
            assert invoked == [] and runtime.state == "running"
            assert runtime.step(cancelled=lambda: True) is None
        assert inbox.query(request.session, "pending")["status"] == "pending"
        assert runtime.step().result["accepted"]
        assert runtime.step() is None
        assert invoked == ["pending"]
    finally:
        runtime.close()


@pytest.mark.parametrize("phase", ["delivery", "ingress", "files"])
def test_busy_after_effect_is_not_swallowed_or_retried(tmp_path, phase):
    runtime, _, _, _ = fixture.setup(tmp_path)
    calls = []

    def effect(*args, **kwargs):
        calls.append(phase)
        raise WorkingWriterBusy("post-effect commit busy")

    setattr(runtime, {"delivery": "channel_delivery", "ingress": "channel_ingress",
                      "files": "channel_files"}[phase], effect)
    runtime.start()
    try:
        with pytest.raises(WorkingWriterBusy):
            runtime.step()
        assert calls == [phase]
    finally:
        runtime.close()


def test_corrupt_snapshot_remains_fatal(tmp_path, monkeypatch):
    runtime, _, _, _ = fixture.setup(tmp_path)
    runtime.start()

    def corrupt(*_):
        raise WorkingSetCorrupt("corrupt")

    try:
        monkeypatch.setattr(runtime.store, "read_current", corrupt)
        with pytest.raises(WorkingSetCorrupt):
            runtime.step()
    finally:
        monkeypatch.undo()
        runtime.close()


@pytest.mark.parametrize("phase", ["submission", "preferences"])
def test_trusted_selection_defers_only_reads_and_reselects(tmp_path, monkeypatch, phase):
    from test_foreground_trusted import config_file

    from ahc.cli import foreground_trusted as composition
    from ahc.infrastructure.channel_control_store import ChannelControlStore

    original, request, value, _ = config_file(tmp_path, enabled=True)
    value["channel_control"] = {"enabled": True, "models": {
        "gpt-test": {"efforts": ["medium"], "priority": False}}, "aliases": {}}
    bindings = []

    class Factory:
        def __init__(self, *args, **kwargs):
            pass

        def binding(self, tools):
            bindings.append(True)
            return "a" * 64, "b" * 64

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    runtime = composition.compose_trusted_foreground(value)
    inbox = RuntimeInbox(runtime.store)
    inbox.submit(request.session, "pending", request.goal_digest, "input")
    current = original.store.read_current("session")
    # Exercise composed ingress without starting a workspace, receiver or provider.
    original.start()
    try:
        target, name = ((RuntimeInbox, "next_pending") if phase == "submission"
                        else (ChannelControlStore, "preferences_for"))
        original_read = getattr(target, name)

        def busy(*args):
            raise WorkingWriterBusy("selection busy")

        monkeypatch.setattr(target, name, busy)
        assert runtime.ingress(current, original.tools) is None
        assert not bindings
        monkeypatch.setattr(target, name, original_read)
        # A tool binding with no workspace avoids coupling two repository instances.
        original.tools.workspace = None
        selected = runtime.ingress(current, original.tools)
        assert selected.operation_id == "pending" and len(bindings) == 1
        assert inbox.query(request.session, "pending")["status"] == "pending"
    finally:
        original.close()
