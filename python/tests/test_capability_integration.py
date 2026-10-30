"""Controlled T2 effect-seam acceptance for M2.3-Lite."""

from __future__ import annotations

from dataclasses import dataclass

from ahc.application.capability_composition import compose_capability_views
from ahc.application.capability_view import AgentCapabilityView
from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    CapabilityGrant,
    DecisionKind,
    EffectClass,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind

_SCALAR_FIELDS = {
    "persona_ref": ProfileResourceKind.PERSONA,
    "workspace_ref": ProfileResourceKind.WORKSPACE,
    "backend_profile_ref": ProfileResourceKind.BACKEND_PROFILE,
    "credential_ref": ProfileResourceKind.CREDENTIAL,
    "working_memory_ref": ProfileResourceKind.WORKING_MEMORY,
    "durable_memory_ref": ProfileResourceKind.DURABLE_MEMORY,
    "subagent_policy_ref": ProfileResourceKind.SUBAGENT_POLICY,
    "cron_policy_ref": ProfileResourceKind.CRON_POLICY,
}
_LIST_FIELDS = {
    "plugin_refs": ProfileResourceKind.PLUGIN,
    "channel_refs": ProfileResourceKind.CHANNEL,
}


@dataclass(frozen=True, slots=True)
class _DomainOutcome:
    result_id: str
    receipt_id: str


@dataclass
class _FakeResourceSink:
    resource_id: str
    effect_count: int = 0
    result_count: int = 0
    receipt_count: int = 0

    def perform_effect(self) -> _DomainOutcome:
        self.effect_count += 1
        self.result_count += 1
        self.receipt_count += 1
        return _DomainOutcome("domain-result", "domain-receipt")


@dataclass
class _FakeDomain:
    view: AgentCapabilityView
    sink: _FakeResourceSink
    authorization_pass_count: int = 0

    def perform(
        self, capability_id: str, effect: EffectClass
    ) -> tuple[DecisionKind, _DomainOutcome | None]:
        decision = self.view.decide(capability_id, self.sink.resource_id, effect)
        if decision.kind is not DecisionKind.ALLOW:
            return decision.kind, None
        self.authorization_pass_count += 1
        return decision.kind, self.sink.perform_effect()


def _profile_registry_payload() -> dict[str, object]:
    profiles: list[dict[str, object]] = []
    resources: list[dict[str, str]] = []
    for agent_id in ("hexi", "lizi"):
        profile: dict[str, object] = {
            "schema_version": "ahc.agent-profile.v1",
            "agent_id": agent_id,
        }
        for field_name, kind in _SCALAR_FIELDS.items():
            resource_id = f"{agent_id}.{kind.value}"
            profile[field_name] = resource_id
            resources.append(
                {
                    "schema_version": "ahc.profile-resource.v1",
                    "resource_id": resource_id,
                    "kind": kind.value,
                    "owner_agent_id": agent_id,
                }
            )
        for field_name, kind in _LIST_FIELDS.items():
            resource_id = f"{agent_id}.{kind.value}"
            profile[field_name] = [resource_id]
            resources.append(
                {
                    "schema_version": "ahc.profile-resource.v1",
                    "resource_id": resource_id,
                    "kind": kind.value,
                    "owner_agent_id": agent_id,
                }
            )
        profiles.append(profile)
    return {
        "schema_version": "ahc.agent-profile-registry.v1",
        "default_agent_id": "hexi",
        "profiles": profiles,
        "resources": resources,
    }


def _composition():
    registry = AgentProfileRegistry.from_dict(_profile_registry_payload())
    definitions = [
        CapabilityDefinition(
            "workspace.read",
            ProfileResourceKind.WORKSPACE,
            EffectClass.READ_ONLY,
            ApprovalMode.NONE,
        ),
        CapabilityDefinition(
            "channel.send",
            ProfileResourceKind.CHANNEL,
            EffectClass.EXTERNAL_OR_IRREVERSIBLE,
            ApprovalMode.REQUIRED,
        ),
    ]
    grants = [
        CapabilityGrant("hexi", "hexi.workspace", "workspace.read"),
        CapabilityGrant("lizi", "lizi.workspace", "workspace.read"),
        CapabilityGrant("lizi", "lizi.channel", "channel.send"),
    ]
    return compose_capability_views(
        profile_registry=registry, definitions=definitions, grants=grants
    )


def test_deny_and_unresolved_approval_reach_zero_effects() -> None:
    # L-T2-01, L-T2-02
    composition = _composition()
    hexi_sink = _FakeResourceSink("hexi.channel")
    lizi_sink = _FakeResourceSink("lizi.channel")
    hexi = _FakeDomain(composition.view("hexi"), hexi_sink)
    lizi = _FakeDomain(composition.view("lizi"), lizi_sink)
    assert hexi.perform("channel.send", EffectClass.EXTERNAL_OR_IRREVERSIBLE) == (
        DecisionKind.DENY,
        None,
    )
    assert lizi.perform("channel.send", EffectClass.EXTERNAL_OR_IRREVERSIBLE) == (
        DecisionKind.REQUIRES_APPROVAL,
        None,
    )
    assert hexi.authorization_pass_count == lizi.authorization_pass_count == 0
    assert hexi_sink.effect_count == lizi_sink.effect_count == 0


def test_allow_reaches_exactly_one_domain_owned_effect_result_and_receipt() -> None:
    # L-T2-03, L-T2-05
    sink = _FakeResourceSink("hexi.workspace")
    domain = _FakeDomain(_composition().view("hexi"), sink)
    kind, outcome = domain.perform("workspace.read", EffectClass.READ_ONLY)
    assert kind is DecisionKind.ALLOW
    assert outcome == _DomainOutcome("domain-result", "domain-receipt")
    assert domain.authorization_pass_count == 1
    assert (sink.effect_count, sink.result_count, sink.receipt_count) == (1, 1, 1)
    decision = domain.view.decide("workspace.read", "hexi.workspace", EffectClass.READ_ONLY)
    assert not hasattr(decision, "result")
    assert not hasattr(decision, "receipt")


def test_same_capability_agent_a_view_cannot_authorize_agent_b_resource_path() -> None:
    # L-T2-04
    composition = _composition()
    hexi_sink = _FakeResourceSink("hexi.workspace")
    lizi_target_sink = _FakeResourceSink("lizi.workspace")
    wrong_view_on_lizi_path = _FakeDomain(composition.view("hexi"), lizi_target_sink)
    assert wrong_view_on_lizi_path.perform("workspace.read", EffectClass.READ_ONLY) == (
        DecisionKind.DENY,
        None,
    )
    assert wrong_view_on_lizi_path.authorization_pass_count == 0
    assert hexi_sink.effect_count == 0
    assert lizi_target_sink.effect_count == 0
