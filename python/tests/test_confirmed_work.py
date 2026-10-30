"""Recovery facts cannot cross the original task's owner or become acceptance."""

from copy import deepcopy
from dataclasses import replace

from test_artifacts import state
from test_runtime_core import setup

from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.confirmed_work import confirmed_work_projection
from ahc.domain.runtime_turn import accept_observation


def test_current_owner_facts_survive_unknown_without_selecting_delivery(tmp_path):
    repo, _, producer, artifacts = state(tmp_path)
    _, _, template = setup(tmp_path / "request")
    request = replace(template, session=producer.session, workspace_scope=producer.workspace_id)
    manifest = artifacts.publish(producer, b"confirmed", "text/plain")
    facts = {"workspace": producer.to_dict(), "checkpoint": {"origin": producer.to_dict(),
             "digest": "a"*64}, "artifacts": [manifest.to_dict()]}
    observed = AgentTurnObservation(request, "unknown", "lost", 1, 1,
                                    unresolved_effects=True, confirmed_work=facts)
    result = accept_observation(request, observed)
    assert result["confirmed_work"] == facts and not result["accepted"]
    assert result["output_text"] == "" and "artifacts" not in result
    assert result["unresolved_effects"]


def test_foreign_or_malformed_recovery_metadata_is_discarded_without_erasing_answer(tmp_path):
    _, _, producer, artifacts = state(tmp_path)
    _, _, template = setup(tmp_path / "request")
    request = replace(template, session=producer.session, workspace_scope=producer.workspace_id)
    manifest = artifacts.publish(producer, b"confirmed", "text/plain")
    facts = {"workspace": producer.to_dict(), "checkpoint": None, "artifacts": [manifest.to_dict()]}
    for variant in ("workspace", "artifact", "shape"):
        invalid = deepcopy(facts)
        if variant == "workspace":
            invalid["workspace"]["session"]["agent_id"] = "foreign-private-agent"
        elif variant == "artifact":
            invalid["artifacts"][0]["producer"]["session"]["route_id"] = "foreign-route"
        else:
            invalid["extra"] = "untrusted"
        assert confirmed_work_projection(invalid, request) == {}
        observation = AgentTurnObservation(request, "completed", "verified", 1, 1,
            output_text="useful answer", confirmed_work=invalid)
        result = accept_observation(request, observation)
        assert result["accepted"] and result["output_text"] == "useful answer"
        assert "confirmed_work" not in result
