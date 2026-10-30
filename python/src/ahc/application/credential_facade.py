"""Composition-issued, one-domain M2.5 credential-use façade."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass

from ahc.domain.credential_values import (
    CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION,
    CredentialAdmissionDecision,
    CredentialPurpose,
    CredentialUseOutcome,
    CredentialUseRequest,
    CredentialUseStatus,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError

from . import credential_facade_support as _support
from .credential_port import (
    _CancellationProbe,
    _CredentialConsumer,
    _CredentialMaterialView,
    _CredentialProvider,
)

_PURPOSE_VALUES = frozenset(item.value for item in CredentialPurpose)
_PROCESS_INFLIGHT: set[tuple[str, str]] = set()
_PROCESS_INFLIGHT_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True, repr=False)
class CredentialUseResult[DomainResult]:
    outcome: CredentialUseOutcome
    domain_result: DomainResult | None

    def __repr__(self) -> str:
        return (
            "<credential-use-result "
            f"status={self.outcome.status.value} reason={self.outcome.reason_code} "
            "domain_result=redacted>"
        )

    __str__ = __repr__


class CredentialUseFacade[DomainResult]:
    """Exact-purpose façade issued by trusted composition with no caller callback."""

    __slots__ = (
        "__profiles",
        "__purpose",
        "__provider",
        "__admission",
        "__consumer",
    )

    __profiles: AgentProfileRegistry
    __purpose: CredentialPurpose
    __provider: _CredentialProvider
    __admission: Callable[[CredentialUseRequest], CredentialAdmissionDecision]
    __consumer: _CredentialConsumer[DomainResult]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("CredentialUseFacade is issued only by credential composition")

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("CredentialUseFacade bindings are immutable")

    @property
    def purpose(self) -> CredentialPurpose:
        return self.__purpose

    def use(
        self,
        request: CredentialUseRequest,
        cancellation_probe: _CancellationProbe,
    ) -> CredentialUseResult[DomainResult]:
        if type(request) is not CredentialUseRequest:
            raise ContractError("request must be a CredentialUseRequest")
        if not callable(getattr(cancellation_probe, "is_cancelled", None)):
            raise ContractError("cancellation_probe must implement is_cancelled")
        if not self.__binding_is_exact(request):
            return self.__result(request, CredentialUseStatus.DENIED, "profile-binding-denied")
        if request.purpose not in _PURPOSE_VALUES or request.purpose != self.__purpose.value:
            return self.__result(request, CredentialUseStatus.DENIED, "purpose-denied")

        attempt = (request.operation_id, request.attempt_id)
        with _PROCESS_INFLIGHT_LOCK:
            if attempt in _PROCESS_INFLIGHT:
                return self.__result(request, CredentialUseStatus.DENIED, "attempt-in-flight")
            _PROCESS_INFLIGHT.add(attempt)
        try:
            decision, admission_failed, process_control = _support.invoke_admission(
                self.__admission, request
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if admission_failed:
                return self.__result(request, CredentialUseStatus.DENIED, "domain-admission-failed")
            if decision is not CredentialAdmissionDecision.ALLOW:
                return self.__result(request, CredentialUseStatus.DENIED, "domain-denied")

            cancellation, probe_failed, process_control = _support.read_cancellation(
                cancellation_probe
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if probe_failed:
                return self.__result(request, CredentialUseStatus.ERROR, "cancellation-probe-error")
            if cancellation:
                return self.__result(
                    request, CredentialUseStatus.CANCELLED, "cancelled-before-consumer"
                )

            material_bytes, provider_failed, process_control = _support.resolve_provider(
                self.__provider, request
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if provider_failed:
                return self.__result(request, CredentialUseStatus.ERROR, "provider-error")
            if material_bytes is None:
                return self.__result(
                    request, CredentialUseStatus.UNAVAILABLE, "provider-unavailable"
                )
            if type(material_bytes) is not bytes or not material_bytes:
                return self.__result(request, CredentialUseStatus.ERROR, "provider-contract-error")

            cancellation, probe_failed, process_control = _support.read_cancellation(
                cancellation_probe
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if probe_failed:
                return self.__result(request, CredentialUseStatus.ERROR, "cancellation-probe-error")
            if cancellation:
                return self.__result(
                    request, CredentialUseStatus.CANCELLED, "cancelled-before-consumer"
                )

            material = _CredentialMaterialView(material_bytes)
            domain_result, consumer_failed, process_control = _support.invoke_consumer(
                self.__consumer, material, cancellation_probe
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if consumer_failed:
                cancellation, probe_failed, process_control = _support.read_cancellation(
                    cancellation_probe
                )
                if process_control is not None:
                    _support.raise_process_control(process_control)
                if probe_failed:
                    return self.__result(
                        request, CredentialUseStatus.ERROR, "cancellation-probe-error"
                    )
                if cancellation:
                    return self.__result(
                        request, CredentialUseStatus.CANCELLED, "consumer-cancelled"
                    )
                return self.__result(request, CredentialUseStatus.ERROR, "consumer-error")

            cancellation, probe_failed, process_control = _support.read_cancellation(
                cancellation_probe
            )
            if process_control is not None:
                _support.raise_process_control(process_control)
            if probe_failed:
                return self.__result(request, CredentialUseStatus.ERROR, "cancellation-probe-error")
            if cancellation:
                return self.__result(request, CredentialUseStatus.CANCELLED, "consumer-cancelled")
            if _support.contains_material(domain_result, material_bytes, _CredentialMaterialView):
                return self.__result(request, CredentialUseStatus.ERROR, "consumer-result-unsafe")
            return self.__result(
                request,
                CredentialUseStatus.USED,
                "consumer-returned",
                domain_result=domain_result,
            )
        finally:
            with _PROCESS_INFLIGHT_LOCK:
                _PROCESS_INFLIGHT.discard(attempt)

    def __binding_is_exact(self, request: CredentialUseRequest) -> bool:
        try:
            profile = self.__profiles.profile(request.agent_id)
        except ContractError:
            return False
        if profile.backend_profile_ref != request.backend_profile_ref:
            return False
        if profile.credential_ref != request.credential_ref:
            return False
        resources = {item.resource_id: item for item in self.__profiles.resources}
        backend = resources.get(request.backend_profile_ref)
        credential = resources.get(request.credential_ref)
        return bool(
            backend is not None
            and backend.kind is ProfileResourceKind.BACKEND_PROFILE
            and backend.owner_agent_id == request.agent_id
            and credential is not None
            and credential.kind is ProfileResourceKind.CREDENTIAL
            and credential.owner_agent_id == request.agent_id
        )

    @staticmethod
    def __result(
        request: CredentialUseRequest,
        status: CredentialUseStatus,
        reason_code: str,
        *,
        domain_result: DomainResult | None = None,
    ) -> CredentialUseResult[DomainResult]:
        outcome = CredentialUseOutcome(
            CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION,
            status,
            reason_code,
            request.agent_id,
            request.purpose,
            request.operation_id,
            request.attempt_id,
            request.binding_digest,
        )
        return CredentialUseResult(outcome, domain_result)


__all__ = ["CredentialUseFacade", "CredentialUseResult"]
