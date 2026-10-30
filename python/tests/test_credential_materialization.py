"""M2.5 credential reference and callback-scoped materialization acceptance."""

from __future__ import annotations

import ast
import importlib
import inspect
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from profile_test_support import profile_registry_payload, standalone_config_text

import ahc
import ahc.application as ahc_application
import ahc.domain as ahc_domain
from ahc.application import credential_port
from ahc.application.credential_composition import _compose_credential_facade
from ahc.application.credential_doctor import inspect_credential_facade
from ahc.application.credential_facade import CredentialUseFacade
from ahc.application.credential_test_doubles import (
    AbsentCredentialProvider,
    FakeCredentialProvider,
    MutableCancellationProbe,
)
from ahc.domain.credential_values import (
    CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION,
    CREDENTIAL_USE_REQUEST_SCHEMA_VERSION,
    CredentialAdmissionDecision,
    CredentialPurpose,
    CredentialUseOutcome,
    CredentialUseRequest,
    CredentialUseStatus,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError
from ahc.infrastructure.config import load_config_document

_SENTINEL_HEXI = b"SENTINEL_HEXI_M25_NOT_A_REAL_CREDENTIAL"
_SENTINEL_LIZI = b"SENTINEL_LIZI_M25_NOT_A_REAL_CREDENTIAL"
_SRC = Path(__file__).parents[1] / "src" / "ahc"


def _hidden_lookup_findings(source: str) -> set[str]:
    tree = ast.parse(source)
    findings: set[str] = set()
    module_aliases: dict[str, str] = {}
    imported_names: dict[str, str] = {}
    forbidden_import_roots = {"keyring", "subprocess"}
    forbidden_calls = {
        "__import__",
        "check_call",
        "check_output",
        "getenv",
        "import_module",
        "open",
        "popen",
        "read_bytes",
        "read_text",
        "run",
        "system",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots = {alias.name.split(".")[0] for alias in node.names}
            if roots & forbidden_import_roots:
                findings.add("forbidden-import")
            for alias in node.names:
                module_aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        if isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            names = {alias.name for alias in node.names}
            if root in forbidden_import_roots or (root == "os" and "environ" in names):
                findings.add("forbidden-import")
            for alias in node.names:
                imported_names[alias.asname or alias.name] = f"{node.module}.{alias.name}"
            if node.module.endswith("credential_facade") and names.intersection(
                {"_ISSUER", "_compose_credential_facade"}
            ):
                findings.add("private-issuer-import")
            if node.module.endswith("credential_composition"):
                findings.add("private-composition-import")
            if node.module.endswith("credential_port"):
                findings.add("private-port-import")
            if node.module == "ahc.application":
                for alias in node.names:
                    if alias.name in {
                        "credential_facade",
                        "credential_composition",
                        "credential_port",
                    }:
                        module_aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        if isinstance(node, ast.Subscript):
            value = node.value
            if isinstance(value, ast.Name) and value.id == "environ":
                findings.add("environment-subscript")
            if (
                isinstance(value, ast.Attribute)
                and isinstance(value.value, ast.Name)
                and value.value.id == "os"
                and value.attr == "environ"
            ):
                findings.add("environment-subscript")
        if isinstance(node, ast.Call):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name in forbidden_calls:
                findings.add("forbidden-call")
            if isinstance(node.func, ast.Name) and imported_names.get(node.func.id, "").endswith(
                ".CredentialUseFacade"
            ):
                findings.add("raw-facade-construction")
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and module_aliases.get(node.func.value.id, "").endswith("credential_facade")
                and node.func.attr == "CredentialUseFacade"
            ):
                findings.add("raw-facade-construction")
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and module_aliases.get(node.value.id, "").endswith("credential_facade")
            and node.attr in {"_ISSUER", "_compose_credential_facade"}
        ):
            findings.add("private-issuer-access")
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and module_aliases.get(node.value.id, "").endswith("credential_composition")
            and node.attr == "_compose_credential_facade"
        ):
            findings.add("private-composition-access")
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and module_aliases.get(node.value.id, "").endswith("credential_port")
        ):
            findings.add("private-port-access")
    return findings


@dataclass(frozen=True, slots=True)
class _DomainResult:
    status: str


class _Admission:
    def __init__(self, *, allowed: bool = True, events: list[str] | None = None) -> None:
        self.allowed = allowed
        self.calls: list[CredentialUseRequest] = []
        self.events = events

    def __call__(self, request: CredentialUseRequest) -> CredentialAdmissionDecision:
        self.calls.append(request)
        if self.events is not None:
            self.events.append("admit")
        return (
            CredentialAdmissionDecision.ALLOW if self.allowed else CredentialAdmissionDecision.DENY
        )


def _registry() -> AgentProfileRegistry:
    return AgentProfileRegistry.from_dict(profile_registry_payload())


def _request(
    agent_id: str = "hexi",
    *,
    backend_profile_ref: str | None = None,
    credential_ref: str | None = None,
    purpose: str = CredentialPurpose.MODEL_INVOKE.value,
    operation_id: str = "operation-1",
    attempt_id: str = "attempt-1",
) -> CredentialUseRequest:
    return CredentialUseRequest(
        CREDENTIAL_USE_REQUEST_SCHEMA_VERSION,
        agent_id,
        backend_profile_ref or f"{agent_id}.backend-profile",
        credential_ref or f"{agent_id}.credential",
        purpose,
        operation_id,
        attempt_id,
    )


def _provider(
    *,
    events: list[str] | None = None,
    error: Exception | None = None,
) -> FakeCredentialProvider:
    return FakeCredentialProvider(
        {
            "hexi.credential": _SENTINEL_HEXI,
            "lizi.credential": _SENTINEL_LIZI,
        },
        events=events,
        error=error,
    )


def _facade(
    *,
    purpose: CredentialPurpose = CredentialPurpose.MODEL_INVOKE,
    provider: object | None = None,
    admission: _Admission | None = None,
    consumer: Any = None,
) -> tuple[CredentialUseFacade[_DomainResult], object, _Admission]:
    actual_provider = _provider() if provider is None else provider
    actual_admission = _Admission() if admission is None else admission
    actual_consumer = (
        (lambda _material, _cancel: _DomainResult("domain-ok")) if consumer is None else consumer
    )
    return (
        _compose_credential_facade(
            profile_registry=_registry(),
            purpose=purpose,
            provider=actual_provider,
            admission=actual_admission,
            consumer=actual_consumer,
        ),
        actual_provider,
        actual_admission,
    )


def test_m25_t0_01_profile_schema_remains_reference_only() -> None:
    profile = profile_registry_payload()["profiles"][0]
    resource = profile_registry_payload()["resources"][0]
    assert set(profile) == {
        "schema_version",
        "agent_id",
        "persona_ref",
        "workspace_ref",
        "backend_profile_ref",
        "credential_ref",
        "working_memory_ref",
        "durable_memory_ref",
        "plugin_refs",
        "channel_refs",
        "subagent_policy_ref",
        "cron_policy_ref",
    }
    assert set(resource) == {"schema_version", "resource_id", "kind", "owner_agent_id"}
    forbidden = {"secret", "token", "password", "private_key", "connection_string", "locator"}
    assert not forbidden.intersection(profile)
    assert not forbidden.intersection(resource)


def test_m25_t0_02_material_port_is_private_redacted_and_not_serializable() -> None:
    facade, provider, _admission = _facade()
    captured: list[object] = []
    facade = _compose_credential_facade(
        profile_registry=_registry(),
        purpose=CredentialPurpose.MODEL_INVOKE,
        provider=provider,
        admission=_Admission(),
        consumer=lambda material, _cancel: captured.append(material) or _DomainResult("ok"),
    )
    facade.use(_request(), MutableCancellationProbe())
    material = captured[0]
    assert repr(material) == "<credential-material:redacted>"
    assert str(material) == "<credential-material:redacted>"
    assert not hasattr(material, "to_dict")
    assert credential_port.__all__ == []
    for name in ("CredentialMaterialView", "CredentialProvider", "CredentialConsumer"):
        assert name not in ahc.__all__
        assert name not in ahc_application.__all__
        assert not hasattr(ahc, name)
        assert not hasattr(ahc_application, name)


def test_m25_t0_03_new_source_has_no_hidden_lookup_or_command_path() -> None:
    for relative in (
        "application/credential_facade.py",
        "application/credential_doctor.py",
        "application/credential_port.py",
        "application/credential_test_doubles.py",
    ):
        source = (_SRC / relative).read_text(encoding="utf-8")
        findings = _hidden_lookup_findings(source)
        if relative == "application/credential_facade.py":
            findings.discard("private-port-import")
        assert findings == set()

    composition = _SRC / "application/credential_composition.py"
    if composition.exists():
        findings = _hidden_lookup_findings(composition.read_text(encoding="utf-8"))
        findings.discard("private-port-import")
        findings.discard("private-composition-import")
        assert findings == set()

    authorized = {
        "credential_composition.py",
        "credential_doctor.py",
        "credential_facade.py",
        "credential_port.py",
        "credential_test_doubles.py",
    }
    for path in (_SRC / "application").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        if path.name not in authorized and "credential_" in source:
            assert _hidden_lookup_findings(source) == set()


def test_m25_t0_04_public_api_cannot_mint_credential_facades() -> None:
    for name in ("compose_credential_facade", "_compose_credential_facade"):
        assert name not in ahc_application.__all__
        assert not hasattr(ahc_application, name)
    facade_module = importlib.import_module("ahc.application.credential_facade")
    assert "_ISSUER" not in vars(facade_module)
    assert "_compose_credential_facade" not in vars(facade_module)


def test_m25_t0_05_facade_collaborators_are_sealed_after_issuance() -> None:
    facade, provider, _ = _facade(admission=_Admission(allowed=False))
    assert not hasattr(facade, "__dict__")
    for name in (
        "_CredentialUseFacade__profiles",
        "_CredentialUseFacade__purpose",
        "_CredentialUseFacade__provider",
        "_CredentialUseFacade__admission",
        "_CredentialUseFacade__consumer",
    ):
        with pytest.raises(AttributeError):
            setattr(facade, name, object())
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert provider.resolve_count == 0


def test_m25_t0_06_malformed_values_reject_without_echo_or_chain() -> None:
    sentinel = _SENTINEL_HEXI.decode()
    request_payload = _request().to_dict() | {sentinel: "x"}
    with pytest.raises(ContractError) as request_error:
        CredentialUseRequest.from_dict(request_payload)
    assert sentinel not in str(request_error.value)
    assert sentinel not in repr(request_error.value)
    assert request_error.value.__cause__ is None
    assert request_error.value.__context__ is None

    outcome_payload: dict[str, object] = {
        "schema_version": CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION,
        "status": sentinel,
        "reason_code": "consumer-returned",
        "agent_id": "hexi",
        "purpose": "model.invoke",
        "operation_id": "operation-1",
        "attempt_id": "attempt-1",
        "binding_digest": "sha256:" + "0" * 64,
    }
    with pytest.raises(ContractError) as status_error:
        CredentialUseOutcome.from_dict(outcome_payload)
    assert sentinel not in str(status_error.value)
    assert sentinel not in repr(status_error.value)
    assert status_error.value.__cause__ is None
    assert status_error.value.__context__ is None

    outcome_payload["status"] = "used"
    outcome_payload["binding_digest"] = 7
    with pytest.raises(ContractError, match="binding_digest must be a SHA-256 digest"):
        CredentialUseOutcome.from_dict(outcome_payload)


def test_m25_t0_07_hidden_lookup_guard_rejects_representative_mutants() -> None:
    mutants = {
        "from os import environ\nvalue = environ['API_TOKEN']",
        "import os\nvalue = os.environ['API_TOKEN']",
        "from pathlib import Path\nvalue = Path('credential').read_bytes()",
        "import keyring\nvalue = keyring.get_password('service', 'account')",
        "import subprocess\nvalue = subprocess.run(['credential-helper'])",
        "import importlib\nvalue = importlib.import_module('keyring')",
        "from ahc.application.credential_facade import _ISSUER, CredentialUseFacade",
        "import ahc.application.credential_facade as cf\nvalue = cf._ISSUER",
        (
            "from ahc.application import credential_facade as cf\n"
            "value = cf._compose_credential_facade"
        ),
        "from ahc.application.credential_facade import _compose_credential_facade",
        (
            "from ahc.application.credential_facade import CredentialUseFacade\n"
            "value = CredentialUseFacade()"
        ),
        "import ahc.application.credential_facade as cf\nvalue = cf.CredentialUseFacade()",
        "from ahc.application.credential_composition import _compose_credential_facade",
        (
            "import ahc.application.credential_composition as cc\n"
            "value = cc._compose_credential_facade"
        ),
        "from ahc.application.credential_port import _CredentialProvider",
        "import ahc.application.credential_port as cp\nvalue = cp._CredentialProvider",
    }
    assert all(_hidden_lookup_findings(source) for source in mutants)


def test_m25_t1_01_exact_binding_admits_before_one_lookup_and_callback() -> None:
    events: list[str] = []
    provider = _provider(events=events)
    admission = _Admission(events=events)

    def consume(material: Any, _cancel: Any) -> _DomainResult:
        events.append("consume")
        assert material.reveal() == _SENTINEL_HEXI
        return _DomainResult("provider-observed")

    facade, _, _ = _facade(provider=provider, admission=admission, consumer=consume)
    result = facade.use(_request(), MutableCancellationProbe())
    assert events == ["admit", "resolve", "consume"]
    assert result.outcome.status is CredentialUseStatus.USED
    assert result.domain_result == _DomainResult("provider-observed")
    assert provider.resolve_count == 1
    assert _SENTINEL_HEXI.decode() not in json.dumps(result.outcome.to_dict())


def test_m25_t1_02_unknown_agent_denies_before_admission_or_lookup() -> None:
    facade, provider, admission = _facade()
    result = facade.use(_request("missing"), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert result.outcome.reason_code == "profile-binding-denied"
    assert provider.resolve_count == 0
    assert admission.calls == []


def test_m25_t1_03_other_agent_credential_denies_before_lookup() -> None:
    facade, provider, admission = _facade()
    result = facade.use(_request(credential_ref="lizi.credential"), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert provider.resolve_count == 0
    assert admission.calls == []


def test_m25_t1_04_backend_mismatch_denies_before_lookup() -> None:
    facade, provider, admission = _facade()
    result = facade.use(
        _request(backend_profile_ref="lizi.backend-profile"), MutableCancellationProbe()
    )
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert provider.resolve_count == 0
    assert admission.calls == []


def test_m25_t1_05_unknown_or_free_text_purpose_denies_before_lookup() -> None:
    facade, provider, admission = _facade()
    result = facade.use(_request(purpose="model.unknown"), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert result.outcome.reason_code == "purpose-denied"
    assert provider.resolve_count == 0
    assert admission.calls == []


def test_m25_t1_06_absent_materializer_is_unavailable_without_consumer() -> None:
    provider = AbsentCredentialProvider()
    calls = 0

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal calls
        calls += 1
        return _DomainResult("unexpected")

    facade, _, _ = _facade(provider=provider, consumer=consume)
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.UNAVAILABLE
    assert result.outcome.reason_code == "provider-unavailable"
    assert provider.resolve_count == 1
    assert calls == 0


def test_m25_t1_07_provider_error_is_closed_and_sentinel_free() -> None:
    sentinel = _SENTINEL_HEXI.decode()
    facade, provider, _ = _facade(provider=_provider(error=RuntimeError(sentinel)))
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.ERROR
    assert result.outcome.reason_code == "provider-error"
    assert provider.resolve_count == 1
    projection = repr(result) + json.dumps(result.outcome.to_dict())
    assert sentinel not in projection


def test_m25_t1_08_consumer_error_is_not_retried_or_chained() -> None:
    sentinel = _SENTINEL_HEXI.decode()
    calls = 0

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal calls
        calls += 1
        raise RuntimeError(sentinel)

    facade, provider, _ = _facade(consumer=consume)
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.ERROR
    assert result.outcome.reason_code == "consumer-error"
    assert calls == provider.resolve_count == 1
    assert sentinel not in repr(result)
    assert result.outcome.__cause__ is None if hasattr(result.outcome, "__cause__") else True


def test_m25_t1_09_precancelled_attempt_has_zero_lookup_or_callback() -> None:
    calls = 0

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal calls
        calls += 1
        return _DomainResult("unexpected")

    facade, provider, admission = _facade(consumer=consume)
    probe = MutableCancellationProbe(cancelled=True)
    result = facade.use(_request(), probe)
    assert result.outcome.status is CredentialUseStatus.CANCELLED
    assert result.outcome.reason_code == "cancelled-before-consumer"
    assert len(admission.calls) == 1
    assert provider.resolve_count == calls == 0


def test_m25_t1_10_cooperative_cancellation_closes_after_callback_exit() -> None:
    probe = MutableCancellationProbe()

    def consume(material: Any, cancel: MutableCancellationProbe) -> _DomainResult:
        assert material.reveal() == _SENTINEL_HEXI
        cancel.cancel()
        return _DomainResult("discarded")

    facade, provider, _ = _facade(consumer=consume)
    result = facade.use(_request(), probe)
    assert result.outcome.status is CredentialUseStatus.CANCELLED
    assert result.outcome.reason_code == "consumer-cancelled"
    assert result.domain_result is None
    assert provider.resolve_count == 1


def test_m25_t1_11_new_domain_admitted_attempt_reacquires_without_cache() -> None:
    facade, provider, _ = _facade()
    first = facade.use(_request(attempt_id="attempt-1"), MutableCancellationProbe())
    second = facade.use(_request(attempt_id="attempt-2"), MutableCancellationProbe())
    assert first.outcome.status is second.outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == 2
    assert provider.requests == ("hexi.credential", "hexi.credential")


def test_m25_t1_12_raw_constructor_callback_and_cross_domain_bypass_reject() -> None:
    facade_module = importlib.import_module("ahc.application.credential_facade")
    issuer = getattr(facade_module, "_ISSUER", None)
    if issuer is not None:
        forged_provider = _provider()
        forged = CredentialUseFacade(
            issuer,
            _registry(),
            CredentialPurpose.MODEL_INVOKE,
            forged_provider,
            _Admission(),
            lambda _material, _cancel: _DomainResult("forged"),
        )
        forged_result = forged.use(_request(), MutableCancellationProbe())
        assert forged_result.outcome.status is not CredentialUseStatus.USED
        assert forged_provider.resolve_count == 0
    with pytest.raises(TypeError, match="issued only by credential composition"):
        CredentialUseFacade(object(), object(), object(), object(), object())
    facade, provider, _ = _facade(purpose=CredentialPurpose.MODEL_INVOKE)
    assert list(inspect.signature(facade.use).parameters) == ["request", "cancellation_probe"]
    result = facade.use(
        _request(purpose=CredentialPurpose.CHANNEL_DELIVER.value),
        MutableCancellationProbe(),
    )
    assert result.outcome.status is CredentialUseStatus.DENIED
    assert provider.resolve_count == 0


def test_m25_t1_13_same_attempt_concurrency_has_one_inflight_owner() -> None:
    entered = threading.Event()
    release = threading.Event()
    completed: list[Any] = []

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        entered.set()
        assert release.wait(timeout=5)
        return _DomainResult("first")

    facade, provider, _ = _facade(consumer=consume)
    request = _request()
    thread = threading.Thread(
        target=lambda: completed.append(facade.use(request, MutableCancellationProbe()))
    )
    thread.start()
    assert entered.wait(timeout=5)
    duplicate = facade.use(request, MutableCancellationProbe())
    release.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert duplicate.outcome.status is CredentialUseStatus.DENIED
    assert duplicate.outcome.reason_code == "attempt-in-flight"
    assert completed[0].outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == 1


def test_m25_t1_14_trusted_consumer_capture_is_possible_but_not_in_outcome() -> None:
    captured: list[bytes] = []

    def consume(material: Any, _cancel: Any) -> _DomainResult:
        captured.append(material.reveal())
        return _DomainResult("safe")

    facade, _, _ = _facade(consumer=consume)
    result = facade.use(_request(), MutableCancellationProbe())
    assert captured == [_SENTINEL_HEXI]
    assert _SENTINEL_HEXI.decode() not in repr(result)
    assert _SENTINEL_HEXI.decode() not in json.dumps(result.outcome.to_dict())


def test_m25_t1_15_digest_binds_purpose_and_attempt() -> None:
    base = _request()
    other_purpose = _request(purpose=CredentialPurpose.WORKSPACE_INVOKE.value)
    other_attempt = _request(attempt_id="attempt-2")
    digests = {base.binding_digest, other_purpose.binding_digest, other_attempt.binding_digest}
    assert len(digests) == 3
    assert all(
        value.startswith("sha256:") and len(value) == 71
        for value in {
            base.binding_digest,
            other_purpose.binding_digest,
            other_attempt.binding_digest,
        }
    )


def test_m25_t1_16_process_fence_blocks_duplicate_across_facades() -> None:
    entered = threading.Event()
    release = threading.Event()

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        entered.set()
        assert release.wait(timeout=5)
        return _DomainResult("first")

    provider = _provider()
    first, _, _ = _facade(provider=provider, consumer=consume)
    second, _, _ = _facade(provider=provider)
    request = _request()
    completed: list[Any] = []
    thread = threading.Thread(
        target=lambda: completed.append(first.use(request, MutableCancellationProbe()))
    )
    thread.start()
    assert entered.wait(timeout=5)
    duplicate = second.use(request, MutableCancellationProbe())
    release.set()
    thread.join(timeout=5)
    assert duplicate.outcome.status is CredentialUseStatus.DENIED
    assert duplicate.outcome.reason_code == "attempt-in-flight"
    assert completed[0].outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == 1


def test_m25_t1_17_admission_reentry_has_one_attempt_owner() -> None:
    provider = _provider()
    consumer_calls = 0

    class ReentrantAdmission:
        facade: CredentialUseFacade[_DomainResult] | None = None
        inner: Any = None
        entered = False

        def __call__(self, request: CredentialUseRequest) -> CredentialAdmissionDecision:
            if not self.entered:
                self.entered = True
                assert self.facade is not None
                self.inner = self.facade.use(request, MutableCancellationProbe())
            return CredentialAdmissionDecision.ALLOW

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal consumer_calls
        consumer_calls += 1
        return _DomainResult("outer")

    admission = ReentrantAdmission()
    facade = _compose_credential_facade(
        profile_registry=_registry(),
        purpose=CredentialPurpose.MODEL_INVOKE,
        provider=provider,
        admission=admission,
        consumer=consume,
    )
    admission.facade = facade
    outer = facade.use(_request(), MutableCancellationProbe())
    assert admission.inner.outcome.status is CredentialUseStatus.DENIED
    assert admission.inner.outcome.reason_code == "attempt-in-flight"
    assert outer.outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == consumer_calls == 1


def test_m25_t1_18_cancellation_probe_reentry_has_one_attempt_owner() -> None:
    provider = _provider()
    consumer_calls = 0

    class ReentrantProbe:
        facade: CredentialUseFacade[_DomainResult] | None = None
        inner: Any = None
        entered = False

        def is_cancelled(self) -> bool:
            if not self.entered:
                self.entered = True
                assert self.facade is not None
                self.inner = self.facade.use(_request(), MutableCancellationProbe())
            return False

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal consumer_calls
        consumer_calls += 1
        return _DomainResult("outer")

    facade, _, _ = _facade(provider=provider, consumer=consume)
    probe = ReentrantProbe()
    probe.facade = facade
    outer = facade.use(_request(), probe)
    assert probe.inner.outcome.status is CredentialUseStatus.DENIED
    assert probe.inner.outcome.reason_code == "attempt-in-flight"
    assert outer.outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == consumer_calls == 1


def test_m25_t1_19_cancellation_during_lookup_prevents_consumer_entry() -> None:
    probe = MutableCancellationProbe()
    consumer_calls = 0

    class CancellingProvider:
        resolve_count = 0

        def resolve(self, _request: CredentialUseRequest) -> bytes:
            self.resolve_count += 1
            probe.cancel()
            return _SENTINEL_HEXI

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal consumer_calls
        consumer_calls += 1
        return _DomainResult("unexpected")

    provider = CancellingProvider()
    facade, _, _ = _facade(provider=provider, consumer=consume)
    result = facade.use(_request(), probe)
    assert result.outcome.status is CredentialUseStatus.CANCELLED
    assert result.outcome.reason_code == "cancelled-before-consumer"
    assert provider.resolve_count == 1
    assert consumer_calls == 0


def test_m25_t1_20_direct_material_result_is_rejected() -> None:
    facade, provider, _ = _facade(consumer=lambda material, _cancel: material.reveal())
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.ERROR
    assert result.outcome.reason_code == "consumer-result-unsafe"
    assert result.domain_result is None
    assert provider.resolve_count == 1

    decoded, _, _ = _facade(consumer=lambda material, _cancel: material.reveal().decode())
    decoded_result = decoded.use(_request(attempt_id="attempt-decoded"), MutableCancellationProbe())
    assert decoded_result.outcome.status is CredentialUseStatus.ERROR
    assert decoded_result.outcome.reason_code == "consumer-result-unsafe"
    assert decoded_result.domain_result is None


def test_m25_t1_21_process_control_is_sanitized_and_releases_fence() -> None:
    sentinel = _SENTINEL_HEXI.decode()

    class ExitOnceProvider:
        calls = 0

        def resolve(self, _request: CredentialUseRequest) -> bytes:
            self.calls += 1
            if self.calls == 1:
                raise SystemExit(sentinel)
            return _SENTINEL_HEXI

    provider = ExitOnceProvider()
    facade, _, _ = _facade(provider=provider)
    with pytest.raises(SystemExit) as exit_error:
        facade.use(_request(), MutableCancellationProbe())
    assert sentinel not in repr(exit_error.value)
    assert exit_error.value.__cause__ is None
    assert exit_error.value.__context__ is None
    retried = facade.use(_request(), MutableCancellationProbe())
    assert retried.outcome.status is CredentialUseStatus.USED
    assert provider.calls == 2

    class OpaqueFailure(BaseException):
        pass

    opaque, _, _ = _facade(
        provider=_provider(error=None),
        consumer=lambda _material, _cancel: (_ for _ in ()).throw(OpaqueFailure(sentinel)),
    )
    closed = opaque.use(_request(attempt_id="attempt-opaque"), MutableCancellationProbe())
    assert closed.outcome.status is CredentialUseStatus.ERROR
    assert closed.outcome.reason_code == "consumer-error"
    assert sentinel not in repr(closed)

    class ExitOnceAdmission:
        calls = 0

        def __call__(self, _request: CredentialUseRequest) -> CredentialAdmissionDecision:
            self.calls += 1
            if self.calls == 1:
                raise SystemExit(sentinel)
            return CredentialAdmissionDecision.ALLOW

    admission = ExitOnceAdmission()
    admission_facade, _, _ = _facade(admission=admission)  # type: ignore[arg-type]
    admission_request = _request(attempt_id="attempt-admission-exit")
    with pytest.raises(SystemExit) as admission_exit:
        admission_facade.use(admission_request, MutableCancellationProbe())
    assert sentinel not in repr(admission_exit.value)
    assert admission_exit.value.__cause__ is None
    assert admission_exit.value.__context__ is None
    assert (
        admission_facade.use(admission_request, MutableCancellationProbe()).outcome.status
        is CredentialUseStatus.USED
    )

    class OpaqueOnceAdmission:
        calls = 0

        def __call__(self, _request: CredentialUseRequest) -> CredentialAdmissionDecision:
            self.calls += 1
            if self.calls == 1:
                raise OpaqueFailure(sentinel)
            return CredentialAdmissionDecision.ALLOW

    opaque_admission = OpaqueOnceAdmission()
    opaque_admission_facade, _, _ = _facade(admission=opaque_admission)  # type: ignore[arg-type]
    opaque_admission_request = _request(attempt_id="attempt-admission-opaque")
    admission_closed = opaque_admission_facade.use(
        opaque_admission_request, MutableCancellationProbe()
    )
    assert admission_closed.outcome.status is CredentialUseStatus.DENIED
    assert admission_closed.outcome.reason_code == "domain-admission-failed"
    assert sentinel not in repr(admission_closed)
    assert (
        opaque_admission_facade.use(
            opaque_admission_request, MutableCancellationProbe()
        ).outcome.status
        is CredentialUseStatus.USED
    )

    class ExitOnceProbe:
        calls = 0

        def is_cancelled(self) -> bool:
            self.calls += 1
            if self.calls == 1:
                raise SystemExit(sentinel)
            return False

    exit_probe = ExitOnceProbe()
    probe_facade, _, _ = _facade()
    probe_request = _request(attempt_id="attempt-probe-exit")
    with pytest.raises(SystemExit) as probe_exit:
        probe_facade.use(probe_request, exit_probe)
    assert sentinel not in repr(probe_exit.value)
    assert probe_exit.value.__cause__ is None
    assert probe_exit.value.__context__ is None
    assert probe_facade.use(probe_request, exit_probe).outcome.status is CredentialUseStatus.USED

    class OpaqueOnceProbe:
        calls = 0

        def is_cancelled(self) -> bool:
            self.calls += 1
            if self.calls == 1:
                raise OpaqueFailure(sentinel)
            return False

    opaque_probe = OpaqueOnceProbe()
    opaque_probe_facade, _, _ = _facade()
    opaque_probe_request = _request(attempt_id="attempt-probe-opaque")
    probe_closed = opaque_probe_facade.use(opaque_probe_request, opaque_probe)
    assert probe_closed.outcome.status is CredentialUseStatus.ERROR
    assert probe_closed.outcome.reason_code == "cancellation-probe-error"
    assert sentinel not in repr(probe_closed)
    assert (
        opaque_probe_facade.use(opaque_probe_request, opaque_probe).outcome.status
        is CredentialUseStatus.USED
    )


def test_m25_t2_01_two_agent_facades_never_cross_material() -> None:
    provider = _provider()
    seen: dict[str, bytes] = {}

    def facade_for(agent_id: str) -> CredentialUseFacade[_DomainResult]:
        facade = _compose_credential_facade(
            profile_registry=_registry(),
            purpose=CredentialPurpose.MODEL_INVOKE,
            provider=provider,
            admission=_Admission(),
            consumer=lambda material, _cancel: (
                seen.__setitem__(agent_id, material.reveal()) or _DomainResult(agent_id)
            ),
        )
        return facade

    hexi = facade_for("hexi").use(_request("hexi"), MutableCancellationProbe())
    lizi = facade_for("lizi").use(_request("lizi"), MutableCancellationProbe())
    assert hexi.domain_result == _DomainResult("hexi")
    assert lizi.domain_result == _DomainResult("lizi")
    assert seen == {"hexi": _SENTINEL_HEXI, "lizi": _SENTINEL_LIZI}


def test_m25_t2_02_doctor_is_pure_and_never_resolves_material() -> None:
    facade, provider, _ = _facade()
    configured = inspect_credential_facade(facade)
    absent = inspect_credential_facade(None)
    assert configured == {
        "schema_version": "ahc.credential-doctor.v1",
        "status": "configured-unverified",
        "purpose": "model.invoke",
    }
    assert absent == {
        "schema_version": "ahc.credential-doctor.v1",
        "status": "not-configured",
        "purpose": None,
    }
    assert provider.resolve_count == 0


def test_m25_t2_03_enumerated_ahc_channels_remain_sentinel_free(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    sentinel = _SENTINEL_HEXI.decode()
    facade, provider, _ = _facade(provider=_provider(error=RuntimeError(sentinel)))
    result = facade.use(_request(), MutableCancellationProbe())
    print(result.outcome.to_dict())
    captured = capsys.readouterr()
    channels = "\n".join(
        [
            repr(result),
            str(result),
            json.dumps(result.outcome.to_dict()),
            repr(provider),
            repr(inspect_credential_facade(facade)),
            captured.out,
            captured.err,
            caplog.text,
        ]
    )
    assert sentinel not in channels


def test_m25_t2_04_domain_result_is_distinct_from_materialization_outcome() -> None:
    facade, _, _ = _facade(consumer=lambda _m, _c: _DomainResult("provider-unknown"))
    result = facade.use(_request(), MutableCancellationProbe())
    assert result.outcome.status is CredentialUseStatus.USED
    assert result.outcome.schema_version == CREDENTIAL_USE_OUTCOME_SCHEMA_VERSION
    assert result.domain_result == _DomainResult("provider-unknown")
    assert "provider-unknown" not in result.outcome.to_dict().values()


def test_m25_t2_05_legacy_config_and_absent_facade_stay_compatible(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(standalone_config_text(version=1), encoding="utf-8")
    document = load_config_document(config)
    assert document.config_version == "ahc.standalone.config.v1"
    assert inspect_credential_facade(None)["status"] == "not-configured"
    assert "CredentialUseFacade" not in ahc.__all__
    assert "CredentialUseRequest" not in ahc.__all__
    assert ahc_domain.CredentialUseRequest is CredentialUseRequest
    assert ahc_application.CredentialUseFacade is CredentialUseFacade


def test_m25_t2_06_error_retry_waits_for_domain_reconciliation() -> None:
    class ReconciledAdmission:
        reconciled = False

        def __call__(self, request: CredentialUseRequest) -> CredentialAdmissionDecision:
            if request.attempt_id == "attempt-1" or self.reconciled:
                return CredentialAdmissionDecision.ALLOW
            return CredentialAdmissionDecision.DENY

    calls = 0

    def consume(_material: Any, _cancel: Any) -> _DomainResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("provider result unknown")
        return _DomainResult("reconciled-success")

    admission = ReconciledAdmission()
    provider = _provider()
    facade = _compose_credential_facade(
        profile_registry=_registry(),
        purpose=CredentialPurpose.MODEL_INVOKE,
        provider=provider,
        admission=admission,
        consumer=consume,
    )
    first = facade.use(_request(attempt_id="attempt-1"), MutableCancellationProbe())
    blocked = facade.use(_request(attempt_id="attempt-2"), MutableCancellationProbe())
    admission.reconciled = True
    retried = facade.use(_request(attempt_id="attempt-3"), MutableCancellationProbe())
    assert first.outcome.status is CredentialUseStatus.ERROR
    assert blocked.outcome.status is CredentialUseStatus.DENIED
    assert blocked.outcome.reason_code == "domain-denied"
    assert retried.outcome.status is CredentialUseStatus.USED
    assert provider.resolve_count == 2
    assert calls == 2
