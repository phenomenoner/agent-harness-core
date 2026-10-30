"""Pure typed receipt construction for the native plugin host."""

from __future__ import annotations

from ahc.domain import (
    NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
    PluginCapability,
    PluginInvocationReceipt,
    PluginInvocationRequest,
)

from .plugin_state import _BindingState


def _plugin_receipt(
    state: _BindingState,
    request: PluginInvocationRequest,
    capability: PluginCapability,
    request_digest: str,
    outcome: str,
    reason: str,
    result_digest: str | None = None,
) -> PluginInvocationReceipt:
    assert state.manifest.manifest_digest is not None
    return PluginInvocationReceipt(
        NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
        request.invocation_id,
        request.agent_id,
        state.binding.binding_id,
        state.manifest.plugin_id,
        state.manifest.manifest_digest,
        capability.capability_id,
        capability.effect,
        request_digest,
        result_digest,
        outcome,
        reason,
    )


__all__: list[str] = []
