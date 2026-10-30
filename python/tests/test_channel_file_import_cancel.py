"""Cancellation between downloads or before commit leaves staging recoverable."""

from types import SimpleNamespace

import pytest
from test_artifacts import state

from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.channel_file_import import ChannelFileImport, FileImportCancelled
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox


@pytest.mark.parametrize("phase", ["before-fetch", "after-first-fetch", "during-commit"])
def test_cancelled_import_rolls_back_and_can_resume(tmp_path, phase):
    repo, snapshot, producer, _ = state(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "event-1", "read", tuple(
        ChannelFile("telegram", "456", str(i), f"file-{i}.txt", 4, "text/plain")
        for i in (2, 3)))
    ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
    cancel = [phase == "before-fetch"]
    calls = []

    def fetch(reference):
        calls.append(reference.file_id)
        if phase == "after-first-fetch":
            cancel[0] = True
        return b"data"

    mutate = repo._mutate
    if phase == "during-commit":
        def wrapped(operation, **kwargs):
            def transaction(connection):
                connection.set_trace_callback(lambda sql: cancel.__setitem__(0, True)
                    if sql.startswith("INSERT INTO runtime_channel_file_imports") else None)
                return operation(connection)
            return mutate(transaction, **kwargs)
        repo._mutate = wrapped
    with pytest.raises(FileImportCancelled):
        ChannelFileImport(repo).run(event, snapshot.goal.digest, producer,
                                   SimpleNamespace(fetch=fetch), cancelled=lambda: cancel[0])
    assert len(calls) == {"before-fetch": 0, "after-first-fetch": 1, "during-commit": 2}[phase]
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 0
    assert ChannelFileInbox(repo).pending(route) == [(event, snapshot.goal.digest)]
    repo._mutate = mutate
    assert len(ChannelFileImport(repo).run(event, snapshot.goal.digest, producer,
                                          SimpleNamespace(fetch=lambda ref: b"data"))) == 2
