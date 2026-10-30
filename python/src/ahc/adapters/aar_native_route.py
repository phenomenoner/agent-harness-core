"""Readback of an explicitly selected native broker route, never route discovery."""

import json

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .aar_native_binding import _DIGEST, _IDENTITY


def verify_native_model_route(binding, report, *, profile_id, catalog_digest, profile_digest):
    """Return a detached exact route only when broker/catalog/default all agree.

    Native aar_rlm_execute has no per-request model selector. The configured
    default route must therefore match the host's expected profile before use.
    This readback is not a model invocation or a proof of provider capability.
    """
    binding.require_same_runtime(report)
    if type(profile_id) is not str or not _IDENTITY.fullmatch(profile_id):
        raise ContractError("native route profile identity invalid")
    for digest in (catalog_digest, profile_digest):
        if type(digest) is not str or not _DIGEST.fullmatch(digest):
            raise ContractError("native route expected digest invalid")
    broker, catalog = report.get("model_broker"), report.get("model_routes")
    if (type(broker) is not dict or broker.get("configured") is not True
            or broker.get("default_profile_id") != profile_id
            or broker.get("catalog_digest") != catalog_digest
            or type(catalog) is not dict or set(catalog) != {
                "schema_version", "profiles", "catalog_digest"}
            or catalog["schema_version"] != "aar.model-route.v1"
            or catalog["catalog_digest"] != catalog_digest):
        raise ContractError("native model route unavailable or mismatched")
    profiles = catalog["profiles"]
    if (type(profiles) is not list or not 1 <= len(profiles) <= 64
            or any(type(profile) is not dict for profile in profiles)
            or type(broker.get("route_profile_count")) is not int
            or broker["route_profile_count"] != len(profiles)):
        raise ContractError("native model catalog shape invalid")
    ids = [profile.get("profile_id") for profile in profiles]
    if (any(type(value) is not str for value in ids)
            or ids != sorted(set(ids))):
        raise ContractError("native model catalog order invalid")
    material = {key: catalog[key] for key in ("schema_version", "profiles")}
    if "sha256:" + digest_json(material) != catalog_digest:
        raise ContractError("native model catalog bytes mismatch")
    selected = next((profile for profile in profiles if profile["profile_id"] == profile_id), None)
    if selected is None or "sha256:" + digest_json(selected) != profile_digest:
        raise ContractError("native model profile bytes mismatch")
    return json.loads(canonical_json(selected))
