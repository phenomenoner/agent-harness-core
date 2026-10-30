"""Relocation preserves all historical bytes and refuses uncertain live effects."""

import pytest
from test_runtime_core import setup
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_backup import _digest
from ahc.infrastructure.runtime_relocation import relocate_offline_runtime


def test_offline_copy_preserves_identity_and_unknown_history(tmp_path):
    store, saved, _ = setup(tmp_path)
    store._mutate(lambda c: c.execute(
        "INSERT INTO runtime_turns VALUES(?,?,?,?,?,?)",
        ("historical", "digest", "{}", "snapshot", "terminal", '{"status":"unknown"}')))
    before = store.db_path.read_bytes()
    destination = tmp_path / "relocated"
    receipt = relocate_offline_runtime(store, destination,
        supervisor_lock=tmp_path / "owner.lock", source_digest=_digest(store.db_path))
    assert (destination / "working.db").read_bytes() == before == store.db_path.read_bytes()
    assert receipt["identity_preserved"] and not receipt["activation_authorized"]
    assert sorted(p.name for p in destination.iterdir()) == ["relocation.json", "working.db"]
    with pytest.raises(FileExistsError):
        relocate_offline_runtime(store, destination,
            supervisor_lock=tmp_path / "owner.lock", source_digest=_digest(store.db_path))


@pytest.mark.parametrize("state", ["submitted", "held", "changed", "queued"])
def test_relocation_refuses_unresolved_or_changed_source(tmp_path, state):
    store, _, _ = setup(tmp_path)
    if state == "queued":
        from ahc.infrastructure.runtime_inbox import RuntimeInbox
        current = store.read_current("session")
        RuntimeInbox(store).submit(current.session, "queued", current.goal.digest, "not replayed")
    if state == "submitted":
        store._mutate(lambda c: c.execute("INSERT INTO runtime_turns VALUES(?,?,?,?,?,?)",
            ("unfinished", "digest", "{}", "snapshot", "submitted", None)))
    if state == "held":
        from ahc.infrastructure.backend_custody import BackendCustody
        BackendCustody(store)
        store._mutate(lambda c: c.execute("INSERT INTO backend_custody VALUES(?,?,?,?,?)",
                                         ("held", "owner", "{}", "digest", "held")))
    destination = tmp_path / "refused"
    with pytest.raises(ContractError):
        relocate_offline_runtime(store, destination, supervisor_lock=tmp_path / "owner.lock",
                                source_digest="a" * 64 if state == "changed" else _digest(store.db_path))
    assert not destination.exists()
