"""Progress is synchronous, bounded, and cannot outlive its admitted turn."""

import pytest
from test_channel_outbox import state

from ahc.application.channel_progress import ChannelProgress
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("failure", [False, True])
def test_progress_requires_live_scope_and_never_retries_call(tmp_path, failure):
    repo, snapshot, request, route = state(tmp_path)
    now, calls = [0], []

    class Transport:
        def send(self, bound, payload, delivery_id):
            assert bound == route
            calls.append(payload)
            if failure:
                raise TimeoutError("private-fixture")
            return "progress-receipt"

    progress = ChannelProgress(repo, request, route, Transport(), clock=lambda: now[0],
                               wall_clock=lambda: now[0])
    with pytest.raises(ContractError):
        progress.send("outside", "working")
    with repo.execution_lock():
        repo.begin(snapshot, request)
        with repo.workspace_turn_scope(request):
            assert progress.send("one", "working")["status"] == ("unknown" if failure else "sent")
            with pytest.raises(ContractError):
                progress.send("one", "again")
            assert progress.send("too-fast", "working")["status"] == "limited"
            for number in range(2, 5):
                now[0] += 5
                progress.send("call-" + str(number), "working")
            now[0] += 5
            assert progress.send("exhausted", "working")["status"] == "limited"
            rebuilt = ChannelProgress(repo, request, route, Transport(), clock=lambda: now[0],
                                      wall_clock=lambda: now[0])
            with pytest.raises(ContractError, match="already attempted"):
                rebuilt.send("one", "working")
            assert rebuilt.send("new-call", "working")["status"] == "limited"
        with pytest.raises(ContractError):
            progress.send("late", "working")
    assert len(calls) == 4
    with repo._read() as connection:
        rows = connection.execute("SELECT * FROM runtime_channel_progress").fetchall()
        assert len(rows) == 4
        assert {row["state"] for row in rows} == {"unknown" if failure else "sent"}


def test_uncommitted_receipt_does_not_allow_repeat(tmp_path):
    from types import SimpleNamespace

    repo, snapshot, request, route = state(tmp_path)
    calls = []
    transport = SimpleNamespace(send=lambda *args: calls.append(args) or "receipt")
    with repo.execution_lock():
        repo.begin(snapshot, request)
        with repo.workspace_turn_scope(request):
            progress = ChannelProgress(repo, request, route, transport, clock=lambda: 0,
                                       wall_clock=lambda: 100)

            def unavailable(*args):
                raise OSError("fixture receipt write failure")

            progress.ledger.finish = unavailable
            with pytest.raises(OSError):
                progress.send("lost-receipt", "working")
            rebuilt = ChannelProgress(repo, request, route, transport, clock=lambda: 0,
                                      wall_clock=lambda: 200)
            with pytest.raises(ContractError, match="already attempted"):
                rebuilt.send("lost-receipt", "working")
    assert len(calls) == 1
    with repo._read() as connection:
        row = connection.execute("SELECT state FROM runtime_channel_progress").fetchone()
        assert row[0] == "claimed"
