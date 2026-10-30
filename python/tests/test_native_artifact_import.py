import json
from dataclasses import replace

import pytest
from test_aar_native_artifacts import artifact_setup, resolve
from test_native_child_repository import prepared

from ahc.adapters.aar_native_artifacts import VerifiedNativeArtifact
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.native_artifact_import import NativeArtifactImport
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


def state(tmp_path):
    store, parent, clock, native, binding = prepared(tmp_path)
    producer = store.reserve_workspace_generation(
        store.read_current(parent.session.ref.virtual_session_id), "workspace")
    operations, _, _ = artifact_setup()
    native.reserve(parent, "child", "token", binding)
    native.observe(parent, "child", "token", json.loads(operations.last_receipt.payload_json))
    reference = json.loads(operations.last_receipt.payload_json)["artifacts"][0]
    artifact = resolve(operations, reference)["artifact"]
    return store, parent, clock, native, producer, artifact


def test_native_verified_bytes_mapping_and_reopen(tmp_path):
    store, parent, _, native, producer, artifact = state(tmp_path)
    importer = NativeArtifactImport(native.children)
    first = importer.publish(parent, "child", "token", producer, artifact)
    assert importer.publish(parent, "child", "token", producer, artifact) == first
    reopened = RuntimeRepository(store.root)
    assert ArtifactRepository(reopened).read(parent.session, first.artifact_id) == (
        first, b"native result")
    history = NativeArtifactImport(ChildWorkRepository(reopened)).read(parent, "child")
    assert history == ({"native_reference": json.loads(artifact.reference_json),
                        "artifact": first.to_dict()},)
    assert native.children.read(parent)[0]["state"] == "claimed"


@pytest.mark.parametrize("mode", ["bytes", "reference", "claim", "workspace", "expired", "stale"])
def test_bad_or_stale_import_creates_neither_artifact_nor_mapping(tmp_path, mode):
    store, parent, clock, native, producer, artifact = state(tmp_path)
    token = "token"
    if mode == "bytes":
        artifact = VerifiedNativeArtifact(artifact.reference_json, b"wrong")
    if mode == "reference":
        artifact = VerifiedNativeArtifact(
            artifact.reference_json.replace("native-artifact", "unretained"), artifact.content)
    if mode == "claim":
        token = "foreign"
    if mode == "workspace":
        producer = replace(producer, workspace_id="other")
    if mode == "expired":
        clock[0] = 201
    if mode == "stale":
        store.begin_workspace_step(producer)
    with pytest.raises((ContractError, WorkingSetConflict)):
        NativeArtifactImport(native.children).publish(parent, "child", token, producer, artifact)
    with store._read() as connection:
        for table in ("runtime_artifacts", "runtime_native_artifacts"):
            assert connection.execute("SELECT count(*) FROM " + table).fetchone()[0] == 0


def test_zero_artifact_allocation_blocks_publication(tmp_path):
    from ahc.infrastructure.native_budget_repository import NativeBudgetRepository

    store, parent, _, native, producer, artifact = state(tmp_path)
    zero = {key: 0 for key in ("model_requests", "input_tokens", "output_tokens",
                               "child_operations", "artifact_bytes")}
    NativeBudgetRepository(native.children).reserve(parent, "child", "token", zero, zero)
    importer = NativeArtifactImport(native.children)
    with pytest.raises(ContractError, match="allocation exhausted"):
        importer.publish(parent, "child", "token", producer, artifact)
    assert importer.read(parent, "child") == ()
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 0


def test_corrupted_mapping_is_not_returned_as_provenance(tmp_path):
    store, parent, _, native, producer, artifact = state(tmp_path)
    importer = NativeArtifactImport(native.children)
    importer.publish(parent, "child", "token", producer, artifact)
    store._mutate(lambda connection: connection.execute(
        "UPDATE runtime_native_artifacts SET native_artifact_id='replacement'"))
    with pytest.raises(ContractError, match="mapping corrupted"):
        importer.read(parent, "child")
