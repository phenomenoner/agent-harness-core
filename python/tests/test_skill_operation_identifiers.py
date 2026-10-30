"""Channel operation identity survives selection, persistence and attribution."""
from dataclasses import replace

import pytest
from skill_test_support import goal, request
from test_judgment_spine import _intent

from ahc.application.skill_service import SkillTurnService
from ahc.domain.judgment_attribution import ATTRIBUTION_SCHEMA, OutcomeAttribution
from ahc.domain.sessions import ContractError
from ahc.domain.skill_records import SkillEpisode, SkillManifest
from ahc.domain.skill_values import identifier


@pytest.mark.parametrize("operation", ["short", "channel-" + "a" * 64, "x" * 128])
def test_operation_roundtrip_across_skill_records(repository, operation):
    selected = replace(request(1, goal().digest), turn_id=operation)
    result = SkillTurnService(repository).select_and_pin(
        selected, episode_id=operation, goal=goal())
    manifest = result["manifest"]
    episode = repository.get_episode(operation)
    assert SkillManifest.from_dict(manifest.to_dict()) == manifest
    assert SkillEpisode.from_dict(episode.to_dict()) == episode
    assert manifest.turn_id == episode.turn_id == episode.episode_id == operation
    frame = replace(_intent(), turn_id=operation, digest=None)
    assert type(frame).from_dict(frame.to_dict()) == frame
    attribution = OutcomeAttribution(ATTRIBUTION_SCHEMA, operation, "unknown", (), (), "verifier")
    assert OutcomeAttribution.from_dict(attribution.to_dict()) == attribution


@pytest.mark.parametrize("invalid", ["x" * 129, "bad/id", "bad\n", "非ASCII", ""])
def test_invalid_operation_still_rejected(invalid):
    with pytest.raises(ContractError):
        replace(request(1, goal().digest), turn_id=invalid)


def test_skill_identifiers_keep_their_original_limit():
    with pytest.raises(ContractError):
        identifier("x" * 65, "skill-local identifier")
