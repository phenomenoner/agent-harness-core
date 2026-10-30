"""Focused contention and failure boundaries for automatic channel delivery."""

import sqlite3
from types import SimpleNamespace

import pytest
from test_channel_attachment_outbox import pending as pending_attachment
from test_channel_outbox import accepted, state

from ahc.cli import foreground_channels
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.working_repository import WorkingSetCorrupt


def _config(route):
    return SimpleNamespace(to_dict=lambda: {"channels": [{
        "route": route.to_dict(),
        "token_env": "AHC_CONTENTION_FIXTURE",
        "enabled": True,
    }]})


def _install_transports(monkeypatch, calls):
    class Text:
        def __init__(self, *args):
            pass

        def send(self, *args):
            calls.append("text")
            return "text-receipt"

    class Attachment:
        def __init__(self, *args):
            pass

        def send(self, upload):
            calls.append("attachment")
            return {"message_id": "attachment-receipt"}

    monkeypatch.setattr(foreground_channels, "BotTextTransport", Text)
    monkeypatch.setattr(foreground_channels, "BotAttachmentTransport", Attachment)


def _compose(monkeypatch, route, store, calls):
    _install_transports(monkeypatch, calls)
    monkeypatch.setenv("AHC_CONTENTION_FIXTURE", "fixture-token")
    return foreground_channels.compose_channels(_config(route), store)


def _hold_exclusive(store):
    connection = sqlite3.connect(store.db_path, isolation_level=None, timeout=0.0)
    connection.execute("BEGIN EXCLUSIVE")
    return connection


def _release(connection):
    connection.rollback()
    connection.close()


def _assert_real_busy_read(store):
    probe = sqlite3.connect(store.db_path, isolation_level=None, timeout=0.0)
    try:
        with pytest.raises(sqlite3.OperationalError) as raised:
            probe.execute("SELECT * FROM runtime_channel_outbox").fetchall()
        code = getattr(raised.value, "sqlite_errorcode", 0)
        assert code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
    finally:
        probe.close()


def test_busy_startup_reconciliation_defers_and_retries(tmp_path, monkeypatch):
    repo, _, _, route = state(tmp_path)
    calls, reconcile_calls = [], []
    real_reconcile = foreground_channels.ChannelOutbox.reconcile

    def counted_reconcile(outbox, current_route):
        reconcile_calls.append(current_route)
        return real_reconcile(outbox, current_route)

    monkeypatch.setattr(foreground_channels.ChannelOutbox, "reconcile", counted_reconcile)
    callback = _compose(monkeypatch, route, repo, calls)

    lock = _hold_exclusive(repo)
    try:
        assert callback(cancelled=lambda: False) is None
    finally:
        _release(lock)
    assert calls == []
    assert len(reconcile_calls) == 1

    assert callback(cancelled=lambda: False) is None
    assert calls == []
    assert len(reconcile_calls) == 2


def test_busy_text_read_defers_without_transport_and_next_cycle_proceeds(
    tmp_path, monkeypatch
):
    repo, _, _, route = state(tmp_path)
    calls, reconcile_calls = [], []
    real_reconcile = foreground_channels.ChannelOutbox.reconcile

    def counted_reconcile(outbox, current_route):
        reconcile_calls.append(current_route)
        return real_reconcile(outbox, current_route)

    monkeypatch.setattr(foreground_channels.ChannelOutbox, "reconcile", counted_reconcile)
    callback = _compose(monkeypatch, route, repo, calls)

    assert callback(cancelled=lambda: False) is None
    lock = _hold_exclusive(repo)
    try:
        _assert_real_busy_read(repo)
        assert callback(cancelled=lambda: False) is None
    finally:
        _release(lock)
    assert calls == []
    assert len(reconcile_calls) == 1

    assert callback(cancelled=lambda: False) is None
    assert calls == []
    assert len(reconcile_calls) == 1


def test_busy_text_record_read_defers_pending_delivery_and_retries(tmp_path, monkeypatch):
    repo, snapshot, request, route = state(tmp_path)
    calls = []
    callback = _compose(monkeypatch, route, repo, calls)

    assert callback(cancelled=lambda: False) is None
    accepted(repo, snapshot, request)

    real_record = foreground_channels.ChannelOutbox._record

    def busy_record(outbox, connection, row, current_route):
        lock = _hold_exclusive(repo)
        try:
            return real_record(outbox, connection, row, current_route)
        finally:
            _release(lock)

    monkeypatch.setattr(foreground_channels.ChannelOutbox, "_record", busy_record)
    try:
        assert callback(cancelled=lambda: False) is None
    finally:
        monkeypatch.setattr(foreground_channels.ChannelOutbox, "_record", real_record)
    assert calls == []
    assert ChannelOutbox(repo).read(route)[0]["state"] == "pending"

    assert callback(cancelled=lambda: False) is None
    assert calls == ["text"]
    assert ChannelOutbox(repo).read(route)[0]["state"] == "sent"


def test_empty_result_stays_idle_and_unchanged(tmp_path, monkeypatch):
    repo, _, _, route = state(tmp_path)
    calls = []
    callback = _compose(monkeypatch, route, repo, calls)

    assert callback(cancelled=lambda: False) is None
    assert callback(cancelled=lambda: False) is None
    assert ChannelOutbox(repo).read(route) == ()
    assert ChannelAttachmentOutbox(repo).read(route) == ()
    assert calls == []


def test_corrupt_pre_send_record_propagates_without_transport(tmp_path, monkeypatch):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)
    repo._mutate(lambda connection: connection.execute(
        "UPDATE runtime_channel_outbox SET payload_json='{}' WHERE operation_id=?",
        (request.operation_id,),
    ))
    calls = []
    callback = _compose(monkeypatch, route, repo, calls)

    with pytest.raises(WorkingSetCorrupt, match="channel outbox payload corrupted"):
        callback(cancelled=lambda: False)
    assert calls == []


def test_non_busy_sql_error_from_pre_send_read_propagates(tmp_path, monkeypatch):
    repo, _, _, route = state(tmp_path)
    error = sqlite3.OperationalError("near malformed: syntax error")

    def failed_read(outbox, current_route):
        raise error

    monkeypatch.setattr(foreground_channels.ChannelOutbox, "read", failed_read)
    calls = []
    callback = _compose(monkeypatch, route, repo, calls)

    with pytest.raises(sqlite3.OperationalError) as raised:
        callback(cancelled=lambda: False)
    assert raised.value is error
    assert calls == []


def test_busy_coded_post_send_error_is_not_swallowed_or_retried(tmp_path, monkeypatch):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)
    calls = []
    busy = sqlite3.OperationalError("database is locked after send")
    busy.sqlite_errorcode = sqlite3.SQLITE_BUSY

    class PostSendFailure:
        def __init__(self, *args):
            pass

        def deliver(self, operation_id, token):
            calls.append((operation_id, token))
            raise busy

    monkeypatch.setattr(foreground_channels, "ChannelDelivery", PostSendFailure)
    callback = _compose(monkeypatch, route, repo, [])

    with pytest.raises(sqlite3.OperationalError) as raised:
        callback(cancelled=lambda: False)
    assert raised.value is busy
    assert len(calls) == 1
    assert calls[0][0] == request.operation_id


def test_busy_attachment_read_defers_and_later_delivers(tmp_path, monkeypatch):
    repo, request, route, workspace = pending_attachment(tmp_path)
    calls = []
    try:
        callback = _compose(monkeypatch, route, repo, calls)
        assert callback(cancelled=lambda: False) is None
        assert calls == ["text"]

        real_read = foreground_channels.ChannelAttachmentOutbox.read
        locks = []

        def busy_read(outbox, current_route):
            locks.append(_hold_exclusive(repo))
            return real_read(outbox, current_route)

        monkeypatch.setattr(foreground_channels.ChannelAttachmentOutbox, "read", busy_read)
        try:
            assert callback(cancelled=lambda: False) is None
        finally:
            for lock in locks:
                _release(lock)
            monkeypatch.setattr(
                foreground_channels.ChannelAttachmentOutbox, "read", real_read
            )
        assert calls == ["text"]

        assert callback(cancelled=lambda: False) is None
        assert calls == ["text", "attachment"]
        assert ChannelAttachmentOutbox(repo).read(route)[0]["state"] == "sent"
    finally:
        workspace.close()
