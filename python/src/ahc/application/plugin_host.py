"""Ephemeral, first-party native plugin lifecycle and invocation owner."""

from __future__ import annotations

from typing import Any

from ahc.domain import (
    NATIVE_PLUGIN_API_VERSION,
    NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
    AgentProfileRegistry,
    ContractError,
    NativePluginManifest,
    PluginBinding,
    PluginInvocationReceipt,
    PluginInvocationRequest,
    PluginReadiness,
    ProfileResourceKind,
)
from ahc.domain.plugin_values import bounded_json_object, digest_json
from ahc.domain.skill_values import identifier

from .plugin_catalog import _load_package_catalog
from .plugin_port import (
    AdapterCallError,
    AdapterOpenResult,
    NativePluginFactory,
)
from .plugin_receipt import _plugin_receipt
from .plugin_state import _BindingState


class NativePluginHost:
    """The single in-process owner of native plugin admission and effects."""

    def __init__(self, profile_registry: AgentProfileRegistry) -> None:
        self._initialize(profile_registry, "empty")

    @classmethod
    def _from_package_catalog(
        cls, profile_registry: AgentProfileRegistry, catalog_id: str
    ) -> NativePluginHost:
        host = cls.__new__(cls)
        host._initialize(profile_registry, catalog_id)
        return host

    def _initialize(self, profile_registry: AgentProfileRegistry, catalog_id: str) -> None:
        if type(profile_registry) is not AgentProfileRegistry:
            raise ContractError("profile_registry must be an AgentProfileRegistry")
        manifests, factories = _load_package_catalog(catalog_id)
        manifest_map = {item.plugin_id: item for item in manifests}
        if len(manifest_map) != len(manifests):
            raise RuntimeError("package catalog contains duplicate plugin_id values")
        if set(factories) - set(manifest_map):
            raise RuntimeError("package catalog factory identifies an unknown manifest")
        self._profiles = profile_registry
        self._manifests = manifest_map
        self._factories = dict(factories)
        self._states: dict[str, _BindingState] = {}
        self._invocation_ids: set[str] = set()

    def manifest(self, plugin_id: str) -> NativePluginManifest:
        identifier(plugin_id, "plugin_id")
        try:
            return self._manifests[plugin_id]
        except KeyError as exc:
            raise ContractError("unknown package plugin_id") from exc

    def _package_factory(self, plugin_id: str) -> NativePluginFactory | None:
        return self._factories.get(plugin_id)

    def admit(self, binding: PluginBinding) -> PluginReadiness:
        if type(binding) is not PluginBinding:
            raise ContractError("binding must be a PluginBinding")
        if binding.binding_id in self._states:
            raise ContractError("duplicate binding_id")
        manifest = self._manifests.get(binding.plugin_id)
        if manifest is None:
            raise ContractError("binding identifies an unknown manifest")
        if binding.manifest_digest != manifest.manifest_digest:
            raise ContractError("binding manifest_digest does not match admitted manifest")
        if binding.scope not in manifest.binding_scopes:
            raise ContractError("binding scope is not admitted by manifest")
        capability_ids = {item.capability_id for item in manifest.capabilities}
        if not set(binding.grants).issubset(capability_ids):
            raise ContractError("binding grant is not admitted by manifest")
        if binding.scope == "agent":
            self._validate_agent_binding(binding)
        readiness = self._initial_readiness(binding, manifest)
        self._states[binding.binding_id] = _BindingState(binding, manifest, readiness)
        return readiness

    def readiness(self, binding_id: str) -> PluginReadiness:
        identifier(binding_id, "binding_id")
        state = self._states.get(binding_id)
        if state is None:
            return PluginReadiness(
                NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
                binding_id,
                None,
                None,
                "absent",
                "binding-absent",
            )
        return state.readiness

    def readinesses(self) -> tuple[PluginReadiness, ...]:
        return tuple(self._states[key].readiness for key in sorted(self._states))

    def open(self, binding_id: str) -> PluginReadiness:
        state = self._state(binding_id)
        if not state.binding.enabled or state.manifest.api_version != NATIVE_PLUGIN_API_VERSION:
            return state.readiness
        factory = self._factories.get(state.manifest.plugin_id)
        if factory is None:
            return state.readiness
        if state.adapter is not None:
            return state.readiness
        try:
            adapter = factory(state.manifest, state.binding)
        except Exception:
            state.readiness = self._known_readiness(state, "unavailable", "factory-failed")
            return state.readiness
        state.adapter = adapter
        try:
            opened = adapter.open()
            if type(opened) is not AdapterOpenResult:
                raise ContractError("adapter open must return AdapterOpenResult")
            state.readiness = self._known_readiness(state, opened.status, opened.reason_code)
        except Exception:
            state.readiness = self._known_readiness(state, "unavailable", "adapter-open-failed")
        return state.readiness

    def invoke(
        self,
        request: PluginInvocationRequest,
        payload: dict[str, Any],
        *,
        approved: bool = False,
    ) -> tuple[PluginInvocationReceipt, dict[str, Any] | None]:
        if type(request) is not PluginInvocationRequest:
            raise ContractError("request must be a PluginInvocationRequest")
        if type(approved) is not bool:
            raise ContractError("approved must be a strict boolean")
        state = self._state(request.binding_id)
        self._profiles.profile(request.agent_id)
        request_digest = digest_json(request.to_dict())
        if state.binding.scope == "agent" and request.agent_id != state.binding.agent_id:
            raise ContractError("caller does not own agent binding")
        if state.readiness.status not in {"ready", "degraded"} or state.adapter is None:
            raise ContractError("plugin is not ready")
        if request.invocation_id in self._invocation_ids:
            raise ContractError("duplicate invocation_id")
        try:
            capability = state.manifest.capability(request.capability_id)
        except ContractError as exc:
            raise ContractError("unknown capability_id") from exc
        if request.capability_id not in state.binding.grants:
            self._invocation_ids.add(request.invocation_id)
            return (
                _plugin_receipt(
                    state,
                    request,
                    capability,
                    request_digest,
                    "rejected",
                    "capability-not-granted",
                ),
                None,
            )
        actual_payload = bounded_json_object(payload, "plugin payload")
        if request.payload_digest != digest_json(actual_payload):
            raise ContractError("payload_digest does not match canonical payload")
        self._invocation_ids.add(request.invocation_id)
        if capability.approval_required and not approved:
            return (
                _plugin_receipt(
                    state, request, capability, request_digest, "rejected", "approval-required"
                ),
                None,
            )
        try:
            result = bounded_json_object(
                state.adapter.invoke(request.capability_id, actual_payload), "plugin result"
            )
        except AdapterCallError as exc:
            outcome = "unknown" if exc.effect_may_have_occurred else "failed"
            return (
                _plugin_receipt(
                    state, request, capability, request_digest, outcome, exc.reason_code
                ),
                None,
            )
        except Exception:
            return _plugin_receipt(
                state, request, capability, request_digest, "unknown", "adapter-exception"
            ), None
        receipt = _plugin_receipt(
            state,
            request,
            capability,
            request_digest,
            "succeeded",
            "adapter-succeeded",
            result_digest=digest_json(result),
        )
        return receipt, result

    def close(self, binding_id: str) -> PluginReadiness:
        state = self._state(binding_id)
        if state.adapter is None:
            return state.readiness
        adapter, state.adapter = state.adapter, None
        reason = "adapter-closed"
        try:
            adapter.close()
        except Exception:
            reason = "adapter-close-failed"
        state.readiness = self._known_readiness(state, "unavailable", reason)
        return state.readiness

    def _validate_agent_binding(self, binding: PluginBinding) -> None:
        assert binding.agent_id is not None and binding.plugin_ref is not None
        profile = self._profiles.profile(binding.agent_id)
        if binding.plugin_ref not in profile.plugin_refs:
            raise ContractError("binding is not a profile plugin reference")
        resource = next(
            (item for item in self._profiles.resources if item.resource_id == binding.plugin_ref),
            None,
        )
        if resource is None or resource.kind is not ProfileResourceKind.PLUGIN:
            raise ContractError("profile plugin reference requires kind plugin")
        if resource.owner_agent_id != binding.agent_id:
            raise ContractError("profile plugin resource crosses owner boundary")

    def _initial_readiness(
        self, binding: PluginBinding, manifest: NativePluginManifest
    ) -> PluginReadiness:
        if not binding.enabled:
            return self._readiness(binding, manifest, "disabled", "binding-disabled")
        if manifest.api_version != NATIVE_PLUGIN_API_VERSION:
            return self._readiness(binding, manifest, "incompatible", "api-incompatible")
        reason = (
            "factory-absent"
            if manifest.plugin_id not in self._factories
            else "adapter-not-open"
        )
        return self._readiness(binding, manifest, "unavailable", reason)

    def _state(self, binding_id: str) -> _BindingState:
        identifier(binding_id, "binding_id")
        try:
            return self._states[binding_id]
        except KeyError as exc:
            raise ContractError("binding is not admitted") from exc

    @staticmethod
    def _readiness(
        binding: PluginBinding, manifest: NativePluginManifest, status: str, reason: str
    ) -> PluginReadiness:
        return PluginReadiness(
            NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
            binding.binding_id,
            manifest.plugin_id,
            manifest.manifest_digest,
            status,
            reason,
        )

    @staticmethod
    def _known_readiness(state: _BindingState, status: str, reason: str) -> PluginReadiness:
        return NativePluginHost._readiness(state.binding, state.manifest, status, reason)

__all__ = ["NativePluginHost"]
