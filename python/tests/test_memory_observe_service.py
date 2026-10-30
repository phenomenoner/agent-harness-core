import copy

import pytest
from profile_test_support import profile_registry_payload

from ahc.application.memory_observe_service import MemoryObserveService, MemoryObserveUnavailable
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def service(get, status=lambda: None, granted=True, resource="hexi.durable-memory", resolve=None):
    return MemoryObserveService(
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()),
        agent_id="hexi",
        resource_id=resource,
        scope="hexi",
        status=status,
        get=get,
        resolve=resolve,
        grants=tuple(
            CapabilityGrant("hexi", "hexi.durable-memory", "memory.observe." + name)
            for name in ("status", "get")
        )
        if granted
        else (),
    )


def session(agent="hexi"):
    return VirtualSessionIdentity.initial("session", agent, "route")


def record():
    return {
        "status": "succeeded",
        "record_ref": "opaque",
        "request_digest": "a" * 64,
        "record": {
            "kind": "note",
            "summary": "orchid",
            "detail": {"scope": "hexi"},
            "created_at": "now",
        },
        "trust": "untrusted",
        "provenance": {"origin": "unknown"},
        "trace": {"operation_id": "one", "request_digest": "a" * 64},
    }


@pytest.mark.parametrize("agent,granted", [("lizi", True), ("hexi", False)])
def test_admission_denies_before_both_ports(agent, granted):
    def forbidden(*args, **kwargs):
        pytest.fail("unadmitted call")

    bound = service(forbidden, forbidden, granted)
    with pytest.raises(ContractError):
        bound.get(session(agent), "opaque")
    with pytest.raises(ContractError):
        bound.status(session(agent))


def test_wrong_resource_rejected_at_binding():
    with pytest.raises(ContractError):
        service(lambda: None, resource="lizi.durable-memory")


def test_lexical_readiness_is_operational_and_does_not_claim_integrity():
    value={'schema_version':'ahc.memory.native.status.v3','status':'ready',
           'code':'native-keyword-ready','observation_scope':'operational','integrity':'not-checked',
           'capabilities':{name:True for name in ('observe_status','observe_get','store_committed','reconcile_store')}}
    assert service(lambda **_:None,status=lambda:value).status(session())==value
    value['schema_version']='ahc.memory.native.status.v2'
    del value['integrity'];del value['observation_scope']
    with pytest.raises(MemoryObserveUnavailable):
        service(lambda **_:None,status=lambda:value).status(session())


def test_get_binds_scope_and_detaches_untrusted_result():
    value = record()
    calls = []

    def get(**kwargs):
        calls.append(kwargs)
        return value

    result = service(get).get(session(), "opaque")
    assert calls == [{"scope": "hexi", "record_ref": "opaque"}]
    assert result["content_authority"] == "untrusted-data"
    value["record"]["summary"] = "changed"
    assert result["record"]["summary"] == "orchid"


@pytest.mark.parametrize("mutation", ["scope", "reference", "trace", "trust", "extra"])
def test_invalid_identity_and_schema_not_exposed(mutation):
    value = copy.deepcopy(record())
    if mutation == "scope":
        value["record"]["detail"]["scope"] = "foreign"
    if mutation == "reference":
        value["record_ref"] = "other"
    if mutation == "trace":
        value["trace"]["request_digest"] = "b" * 64
    if mutation == "trust":
        value["trust"] = "instructions"
    if mutation == "extra":
        value["private"] = "secret"
    with pytest.raises(MemoryObserveUnavailable, match="^memory record unavailable$"):
        service(lambda **kw: value).get(session(), "opaque")


def test_status_strict_ready_and_not_found():
    value = {
        "schema_version": "openclaw-mem.native.status.v1",
        "status": "ready",
        "code": "native-schema-supported",
        "capabilities": dict.fromkeys(
            ("observe_status", "observe_get", "store_committed", "reconcile_store"), True
        ),
    }
    bound = service(lambda **kw: {"status": "not-found"}, lambda: value)
    assert bound.status(session()) == value
    assert bound.get(session(), "absent") == {"status": "not-found"}
    value["capabilities"]["store_committed"] = 1
    with pytest.raises(MemoryObserveUnavailable):
        bound.status(session())


def test_citation_resolution_is_scoped_and_uses_read_grant():
    calls = []
    def resolve(**kw):
        calls.append(kw)
        return {"status": "succeeded", "record_ref": "opaque-citation"}
    bound = service(lambda: None, resolve=resolve)
    assert bound.resolve(session(), 7)["record_ref"] == "opaque-citation"
    assert calls == [{"scope": "hexi", "row_id": 7}]
    with pytest.raises(ContractError):
        service(lambda: None, granted=False, resolve=resolve).resolve(session(), 7)
    with pytest.raises(ContractError):
        bound.resolve(session(), True)
    assert len(calls) == 1


@pytest.mark.parametrize("result", [{"status": "succeeded"},
    {"status": "succeeded", "record_ref": "x", "extra": "untrusted"},
    {"status": "succeeded", "record_ref": ""}])
def test_citation_malformed_result_is_unavailable(result):
    with pytest.raises(MemoryObserveUnavailable):
        service(lambda: None, resolve=lambda **kw: result).resolve(session(), 1)
