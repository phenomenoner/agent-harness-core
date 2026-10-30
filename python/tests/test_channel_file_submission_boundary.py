"""File staging and executable inbox cannot retrospectively replace each other."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("order", ["text-first", "file-first"])
@pytest.mark.parametrize("channel_projection", [False, True])
def test_no_retroactive_files_or_unimported_submission(tmp_path, order, channel_projection):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "event-1", "read", (
        ChannelFile("telegram", "456", "2", "report.txt", 4, "text/plain"),))
    inbox, files = RuntimeInbox(repo), ChannelFileInbox(repo)

    def submit():
        return inbox.submit(snapshot.session, event.operation_id, snapshot.goal.digest,
                            event.input_text, channel_event=(replace(event, files=())
                                                           if channel_projection else None))

    if order == "text-first":
        submit()
        with pytest.raises(ContractError):
            files.stage(event, snapshot.goal.digest)
        assert files.pending(route) == []
        assert inbox.next_pending(route.session)["operation_id"] == event.operation_id
    else:
        files.stage(event, snapshot.goal.digest)
        with pytest.raises(ContractError):
            submit()
        assert inbox.next_pending(route.session) is None
        assert files.pending(route) == [(event, snapshot.goal.digest)]
