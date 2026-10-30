"""Composition-owned issuance for one-domain M2.5 credential-use façades."""

from __future__ import annotations

from collections.abc import Callable

from ahc.domain.credential_values import (
    CredentialAdmissionDecision,
    CredentialPurpose,
    CredentialUseRequest,
)
from ahc.domain.profile_registry import AgentProfileRegistry

from .credential_facade import CredentialUseFacade
from .credential_port import _CredentialConsumer, _CredentialProvider


def _compose_credential_facade[DomainResult](
    *,
    profile_registry: AgentProfileRegistry,
    purpose: CredentialPurpose,
    provider: _CredentialProvider,
    admission: Callable[[CredentialUseRequest], CredentialAdmissionDecision],
    consumer: _CredentialConsumer[DomainResult],
) -> CredentialUseFacade[DomainResult]:
    """Issue one closed façade from trusted composition-owned collaborators."""

    if type(profile_registry) is not AgentProfileRegistry:
        raise TypeError("profile_registry must be an AgentProfileRegistry")
    if type(purpose) is not CredentialPurpose:
        raise TypeError("purpose must be a CredentialPurpose")
    if not callable(getattr(provider, "resolve", None)):
        raise TypeError("provider must implement resolve")
    if not callable(admission):
        raise TypeError("admission must be callable")
    if not callable(consumer):
        raise TypeError("consumer must be callable")

    facade: CredentialUseFacade[DomainResult] = object.__new__(CredentialUseFacade)
    object.__setattr__(facade, "_CredentialUseFacade__profiles", profile_registry)
    object.__setattr__(facade, "_CredentialUseFacade__purpose", purpose)
    object.__setattr__(facade, "_CredentialUseFacade__provider", provider)
    object.__setattr__(facade, "_CredentialUseFacade__admission", admission)
    object.__setattr__(facade, "_CredentialUseFacade__consumer", consumer)
    return facade


__all__: list[str] = []
