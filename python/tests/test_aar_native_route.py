from copy import deepcopy

import pytest
from test_aar_native_binding import candidate

from ahc.adapters.aar_native_route import verify_native_model_route
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


def route_candidate():
    pin, report = candidate()
    binding = pin.bind(report, principal_id="host", session_id="session")
    profile = {"schema_version": "aar.model-route.v1", "profile_id": "host-route",
               "provider_driver": "fixture", "provider": "fixture", "model": "fixture-model",
               "reasoning_effort": "high", "max_output_tokens": 256,
               "fallback_policy": "none", "cache_policy": "disabled"}
    material = {"schema_version": "aar.model-route.v1", "profiles": [profile]}
    digest = "sha256:" + digest_json(material)
    report["model_routes"] = {**material, "catalog_digest": digest}
    report["model_broker"] = {"configured": True, "default_profile_id": "host-route",
                              "catalog_digest": digest, "route_profile_count": 1}
    expected = {"profile_id": "host-route", "catalog_digest": digest,
                "profile_digest": "sha256:" + digest_json(profile)}
    return binding, report, expected


def test_exact_native_default_profile_and_detached_readback():
    binding, report, expected = route_candidate()
    route = verify_native_model_route(binding, report, **expected)
    assert route == report["model_routes"]["profiles"][0]
    route["model"] = "changed"
    assert report["model_routes"]["profiles"][0]["model"] == "fixture-model"


@pytest.mark.parametrize("field,value", [
    ("configured", False), ("configured", 1), ("default_profile_id", "other"),
    ("catalog_digest", "sha256:" + "f" * 64), ("route_profile_count", True),
])
def test_broker_mismatch_rejects(field, value):
    binding, report, expected = route_candidate()
    report["model_broker"][field] = value
    with pytest.raises(ContractError):
        verify_native_model_route(binding, report, **expected)


def test_profile_drift_and_runtime_restart_reject():
    binding, report, expected = route_candidate()
    changed = deepcopy(report)
    changed["model_routes"]["profiles"][0]["model"] = "other"
    with pytest.raises(ContractError, match="catalog bytes"):
        verify_native_model_route(binding, changed, **expected)
    report["ready"]["runtime_generation"] += 1
    with pytest.raises(ContractError, match="generation changed"):
        verify_native_model_route(binding, report, **expected)
