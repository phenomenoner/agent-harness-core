"""Progress readback and local reconciliation never permit a repeat effect."""

import json

import pytest
from test_channel_outbox import state

from ahc.cli import main
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_progress_store import ChannelProgressStore


def test_progress_readback_and_reconcile_are_owner_scoped(tmp_path, capsys):
    repo, snapshot, request, route = state(tmp_path)
    ledger = ChannelProgressStore(repo)
    common = ["--runtime-root", str(repo.root), "--route-json", json.dumps(route.to_dict())]
    with repo.execution_lock():
        repo.begin(snapshot, request)
        assert ledger.reserve(request, route, "progress-one", "private-progress-text",
                              "private-claim-token", 10000)
        assert main(["runtime-channel-progress", *common]) == 0
        raw = capsys.readouterr().out
        assert "private-progress-text" not in raw and "private-claim-token" not in raw
        assert json.loads(raw)["progress"][0]["state"] == "claimed"
        assert main(["runtime-channel-progress-reconcile", *common]) == 1
        capsys.readouterr()
        assert ledger.read(route)[0]["state"] == "claimed"
    assert main(["runtime-channel-progress-reconcile", *common]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["progress"][0]["state"] == "unknown"
    assert report["automatic_retry"] is False and report["platform_readback"] == "not-performed"
    with pytest.raises(ContractError):
        ledger.finish(request, route, "progress-one", "private-claim-token", "late")
    with pytest.raises(ContractError, match="already attempted"):
        ledger.reserve(request, route, "progress-one", "retry", "new-token", 20000)
    assert main(["runtime-channel-progress", *common]) == 0
    assert json.loads(capsys.readouterr().out)["progress"] == report["progress"]
