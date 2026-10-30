"""Recovery generations supersede saved/current identities without ordinary CAS bypass."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.runtime_recovery import RecoveryTransition
from ahc.domain.sessions import ContractError
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition


@pytest.mark.parametrize(("saved_generation", "source_generation"), [(1, 4), (4, 1), (3, 3)])
def test_recovery_high_water_content_and_provenance(tmp_path, saved_generation, source_generation):
    _, initial, _ = setup(tmp_path)

    def generation(number):
        snapshot = initial
        for _ in range(number - 1):
            snapshot = snapshot.continue_as(snapshot.session.successor(), "compaction")
        return snapshot

    saved = generation(saved_generation).revise(progress=("saved work",), active_turn_id="old-turn")
    source = generation(source_generation).revise(progress=("newer work",))
    transition = RecoveryTransition(saved, source, "a" * 64, "b" * 64)
    recovered = transition.recovered
    assert recovered.session.ref.generation == max(saved_generation, source_generation) + 1
    assert recovered.session.predecessor.generation == max(saved_generation, source_generation)
    assert recovered.progress == ("saved work",) and recovered.goal == saved.goal
    assert recovered.active_turn_id is None and recovered.status == "active"
    assert recovered.revision == 1 and recovered.continuation_reason == "restore"
    assert recovered.predecessor_digest == saved.snapshot_digest
    assert WorkingSetSnapshot.from_dict(recovered.to_dict()) == recovered
    assert transition.to_dict()["source_snapshot_digest"] == source.snapshot_digest
    assert transition.to_dict()["automatic_replay"] is False
    assert len(transition.digest) == 64
    with pytest.raises(ContractError, match="explicit recovery"):
        validate_working_set_transition(saved, recovered)


@pytest.mark.parametrize("fault", ["agent", "route", "session", "terminal", "digest"])
def test_invalid_recovery_contract_rejects(tmp_path, fault):
    _, saved, _ = setup(tmp_path)
    source, backup_digest = saved, "a" * 64
    if fault in {"agent", "route", "session"}:
        identity = saved.session
        if fault == "agent":
            identity = replace(identity, agent_id="other")
        elif fault == "route":
            identity = replace(identity, route_id="other")
        else:
            identity = replace(identity, ref=replace(identity.ref, virtual_session_id="other"))
        source = WorkingSetSnapshot.initial(identity, saved.goal)
    elif fault == "terminal":
        saved = saved.revise(status="completed")
    else:
        backup_digest = "invalid"
    with pytest.raises(ContractError):
        RecoveryTransition(saved, source, backup_digest, "b" * 64)
