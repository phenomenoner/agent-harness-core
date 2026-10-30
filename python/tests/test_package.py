from __future__ import annotations

import tomllib
from inspect import signature
from pathlib import Path

import ahc
import ahc.application as ahc_application
from ahc.application import WorkingContinuityService, WorkingSetPort, plugin_port, plugin_receipt
from ahc.application.profile_doctor import (
    PROFILE_DOCTOR_SCHEMA_VERSION,
    inspect_profile_authority,
)
from ahc.domain import (
    AGENT_PROFILE_REGISTRY_SCHEMA_VERSION,
    AGENT_PROFILE_SCHEMA_VERSION,
    PROFILE_RESOURCE_SCHEMA_VERSION,
    WORKING_CONTINUITY_STATUS_SCHEMA_VERSION,
    WORKING_SET_SCHEMA_VERSION,
    AgentProfile,
    AgentProfileRegistry,
    FakeTurnRequest,
    FakeTurnResult,
    ProfileResource,
    ProfileResourceKind,
    VirtualSessionIdentity,
    VirtualSessionRef,
    WorkingSetSnapshot,
)
from ahc.infrastructure import (
    WORKING_DB_SCHEMA_VERSION,
    WorkingSetConflict,
    WorkingSetRepository,
    WorkingWriterBusy,
    inspect_working_continuity,
)
from ahc.infrastructure.config import (
    CONFIG_DOCTOR_SCHEMA_VERSION,
    CONFIG_VERSION,
    CONFIG_VERSION_V1,
    ProfileRegistryLoadError,
    load_config_document,
    load_profile_registry,
)


def test_runtime_version_matches_package_metadata() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]

    assert ahc.__version__ == project["version"]
    assert project["requires-python"] == ">=3.14"
    assert "Operating System :: Microsoft :: Windows" in project["classifiers"]


def test_m1_domain_and_application_exports_are_public() -> None:
    assert ahc.VirtualSessionRef is VirtualSessionRef
    assert ahc.VirtualSessionIdentity is VirtualSessionIdentity
    assert ahc.FakeTurnRequest is FakeTurnRequest
    assert ahc.FakeTurnResult is FakeTurnResult
    assert callable(ahc.execute_fake_turn)


def test_m21_profile_authority_exports_are_public() -> None:
    assert ahc.AgentProfile is AgentProfile
    assert ahc.AgentProfileRegistry is AgentProfileRegistry
    assert ahc.ProfileResource is ProfileResource
    assert ahc.ProfileResourceKind is ProfileResourceKind
    assert ahc.AGENT_PROFILE_SCHEMA_VERSION == "ahc.agent-profile.v1"
    assert ahc.AGENT_PROFILE_REGISTRY_SCHEMA_VERSION == "ahc.agent-profile-registry.v1"
    assert ahc.PROFILE_RESOURCE_SCHEMA_VERSION == "ahc.profile-resource.v1"
    assert AGENT_PROFILE_SCHEMA_VERSION == ahc.AGENT_PROFILE_SCHEMA_VERSION
    assert AGENT_PROFILE_REGISTRY_SCHEMA_VERSION == ahc.AGENT_PROFILE_REGISTRY_SCHEMA_VERSION
    assert PROFILE_RESOURCE_SCHEMA_VERSION == ahc.PROFILE_RESOURCE_SCHEMA_VERSION
    assert CONFIG_VERSION == "ahc.standalone.config.v2"
    assert CONFIG_VERSION_V1 == "ahc.standalone.config.v1"
    assert CONFIG_DOCTOR_SCHEMA_VERSION == "ahc.config-doctor.v1"
    assert issubclass(ProfileRegistryLoadError, ValueError)
    profile_loader_signature = signature(load_profile_registry)
    config_loader_signature = signature(load_config_document)
    assert list(profile_loader_signature.parameters) == ["path"]
    assert profile_loader_signature.parameters["path"].annotation == "Path"
    assert profile_loader_signature.return_annotation == "AgentProfileRegistry"
    assert list(config_loader_signature.parameters) == ["path"]
    assert config_loader_signature.parameters["path"].annotation == "str | Path"
    assert config_loader_signature.return_annotation == "ConfigDocument"
    assert PROFILE_DOCTOR_SCHEMA_VERSION == "ahc.profile-doctor.v1"
    assert callable(inspect_profile_authority)


def test_m22_working_continuity_exports_and_signatures_are_public() -> None:
    assert ahc.WorkingSetSnapshot is WorkingSetSnapshot
    assert ahc.WorkingContinuityService is WorkingContinuityService
    assert ahc.WorkingSetPort is WorkingSetPort
    assert ahc.WorkingSetRepository is WorkingSetRepository
    assert ahc.WorkingSetConflict is WorkingSetConflict
    assert ahc.WorkingWriterBusy is WorkingWriterBusy
    assert ahc.inspect_working_continuity is inspect_working_continuity
    assert ahc.WORKING_SET_SCHEMA_VERSION == "ahc.working-set.v1"
    assert WORKING_SET_SCHEMA_VERSION == ahc.WORKING_SET_SCHEMA_VERSION
    assert WORKING_CONTINUITY_STATUS_SCHEMA_VERSION == "ahc.working-continuity-status.v1"
    assert WORKING_DB_SCHEMA_VERSION == 1

    initialize = signature(WorkingSetRepository.initialize)
    create = signature(WorkingSetRepository.create)
    append = signature(WorkingSetRepository.append)
    read_current = signature(WorkingSetRepository.read_current)
    status = signature(inspect_working_continuity)
    assert list(initialize.parameters) == ["root"]
    assert initialize.parameters["root"].annotation == "str | Path"
    assert initialize.return_annotation == "WorkingSetRepository"
    assert list(create.parameters) == ["self", "snapshot"]
    assert create.return_annotation == "WorkingSetSnapshot"
    assert list(append.parameters) == ["self", "expected", "successor"]
    assert append.return_annotation == "WorkingSetSnapshot"
    assert list(read_current.parameters) == ["self", "virtual_session_id"]
    assert read_current.return_annotation == "WorkingSetSnapshot"
    assert list(status.parameters) == ["root"]
    assert status.return_annotation == "dict[str, object]"


def test_m23_native_plugin_host_exports_and_signatures_are_public() -> None:
    assert ahc.NATIVE_PLUGIN_API_VERSION == "1.0.0"
    assert ahc.NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION == "ahc.native-plugin-manifest.v1"
    assert ahc.NATIVE_PLUGIN_BINDING_SCHEMA_VERSION == "ahc.native-plugin-binding.v1"
    assert ahc.NATIVE_PLUGIN_READINESS_SCHEMA_VERSION == "ahc.native-plugin-readiness.v1"
    assert ahc.NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION == (
        "ahc.native-plugin-invocation-request.v1"
    )
    assert ahc.NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION == (
        "ahc.native-plugin-invocation-receipt.v1"
    )
    assert ahc.NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION == "ahc.native-plugin-doctor.v1"
    assert ahc.PluginEffect.READ == "read"
    assert callable(ahc.NativePluginHost)
    assert callable(ahc.NativePluginManifest)
    assert callable(ahc.PluginBinding)
    assert callable(ahc.PluginCapability)
    assert callable(ahc.PluginReadiness)
    assert callable(ahc.PluginInvocationRequest)
    assert callable(ahc.PluginInvocationReceipt)
    assert callable(ahc.inspect_native_plugin_host)

    constructor = signature(ahc.NativePluginHost.__init__)
    manifest = signature(ahc.NativePluginHost.manifest)
    invoke = signature(ahc.NativePluginHost.invoke)
    doctor = signature(ahc.inspect_native_plugin_host)
    assert list(constructor.parameters) == ["self", "profile_registry"]
    assert list(manifest.parameters) == ["self", "plugin_id"]
    assert list(invoke.parameters) == ["self", "request", "payload", "approved"]
    assert invoke.parameters["approved"].kind.name == "KEYWORD_ONLY"
    assert list(doctor.parameters) == ["host"]
    for private_port in (
        "AdapterCallError",
        "AdapterOpenResult",
        "NativePluginAdapter",
        "NativePluginFactory",
        "_plugin_receipt",
    ):
        assert private_port not in ahc.__all__
        assert private_port not in ahc_application.__all__
        assert not hasattr(ahc, private_port)
        assert not hasattr(ahc_application, private_port)
    assert plugin_port.__all__ == []
    assert plugin_receipt.__all__ == []
