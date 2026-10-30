import hashlib
import json

from test_runtime_core import setup

from ahc.application.tool_batch import BatchCall
from ahc.cli import main
from ahc.infrastructure.batch_receipt_repository import BatchReceiptRepository
from ahc.infrastructure.runtime_health import inspect_runtime_health


def test_status_reports_pending_metadata_without_repair_or_liveness_claim(tmp_path, capsys):
    store, snapshot, request = setup(tmp_path)
    store.begin(snapshot, request)
    BatchReceiptRepository(store).begin(request, (
        BatchCall("one", "runtime.result.read", "runtime", '{"operation_id":"private"}'),))
    before = hashlib.sha256(store.db_path.read_bytes()).hexdigest()
    assert main(["runtime-status", "--runtime-root", str(store.root),
                 "--session-json", json.dumps(snapshot.session.to_dict())]) == 0
    result = json.loads(capsys.readouterr().out)
    health = result["durable_health"]
    assert health["observation_scope"] == "persisted-state-counts"
    assert health["domains"]["turns"]["state_counts"] == {"submitted": 1}
    assert health["domains"]["batch_receipts"]["state_counts"] == {"completion-unverified": 1}
    assert result["capabilities"]["live_backend"] == "not-inspected"
    assert "private" not in json.dumps(health)
    assert hashlib.sha256(store.db_path.read_bytes()).hexdigest() == before


def test_missing_optional_schema_is_not_created(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    store._mutate(lambda c: c.execute("DROP TABLE runtime_batch_receipts"))
    result = inspect_runtime_health(store, snapshot.session)
    assert result["domains"]["batch_receipts"] == {"status": "schema-absent"}
    with store._read() as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='runtime_batch_receipts'"
        ).fetchone() is None


def test_unreadable_domain_does_not_hide_other_counts(tmp_path):
    store, snapshot, _ = setup(tmp_path)

    def damage(connection):
        connection.execute("DROP TABLE workspace_heads")
        connection.execute("CREATE TABLE workspace_heads (unexpected TEXT)")

    store._mutate(damage)
    result = inspect_runtime_health(store, snapshot.session)["domains"]
    assert result["workspaces"] == {"status": "unreadable"}
    assert result["turns"] == {"status": "observed", "state_counts": {}}


def test_memory_approval_health_preserves_unknown_and_session_boundary(tmp_path):
    from test_memory_store_approvals import prepared

    repo, snapshot, approvals, digest = prepared(tmp_path)
    approvals.approve(snapshot.session, digest, approval_expires=150)
    approvals.consume(snapshot.session, digest)
    approvals.finish(snapshot.session, digest, "unknown")
    before = repo.db_path.read_bytes()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]["memory_proposals"]
    assert report == {"status": "observed", "state_counts": {"unknown": 1}}
    other = inspect_runtime_health(repo, snapshot.session.successor())["domains"]
    other = other["memory_proposals"]
    assert other == {"status": "observed", "state_counts": {}}
    assert "orchid" not in json.dumps(report)
    assert repo.db_path.read_bytes() == before


def test_missing_memory_approval_schema_is_not_initialized(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    before = repo.db_path.read_bytes()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]["memory_proposals"]
    assert report == {"status": "schema-absent"}
    assert repo.db_path.read_bytes() == before


def test_corrupt_memory_proposal_is_unreadable_without_hiding_other_domains(tmp_path):
    from test_memory_store_approvals import prepared

    repo, snapshot, _, _ = prepared(tmp_path)
    repo._mutate(lambda c: c.execute("UPDATE memory_store_proposals SET payload='invalid-json'"))
    before = repo.db_path.read_bytes()
    domains = inspect_runtime_health(repo, snapshot.session)["domains"]
    assert domains["memory_proposals"] == {"status": "unreadable"}
    assert domains["turns"]["status"] == "observed"
    assert repo.db_path.read_bytes() == before


def test_terminal_unknown_effect_remains_visible_without_repair(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    repo.begin(snapshot, request)
    assert inspect_runtime_health(repo, snapshot.session)["domains"]["turn_effects"] == {
        "status": "observed", "state_counts": {"not-terminal": 1}}
    repo.reconcile(snapshot.session)
    before = repo.db_path.read_bytes()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]
    assert report["turns"]["state_counts"] == {"terminal": 1}
    assert report["turn_effects"] == {
        "status": "observed", "state_counts": {"unresolved": 1}}
    assert inspect_runtime_health(repo, snapshot.session.successor())["domains"][
        "turn_effects"]["state_counts"] == {}
    assert repo.db_path.read_bytes() == before


def test_missing_effect_flag_is_unclassified_and_corrupt_result_is_unreadable(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    repo.begin(snapshot, request)
    repo.finish(request, {"accepted": True, "output_text": "fixture"})
    assert inspect_runtime_health(repo, snapshot.session)["domains"]["turn_effects"] == {
        "status": "observed", "state_counts": {"unclassified": 1}}
    repo._mutate(lambda c: c.execute("UPDATE runtime_turns SET result_json='bad-json'"))
    before = repo.db_path.read_bytes()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]
    assert report["turn_effects"] == {"status": "unreadable"}
    assert report["turns"]["state_counts"] == {"terminal": 1}
    assert repo.db_path.read_bytes() == before


def test_completed_effect_flag_is_resolved_without_claiming_liveness(tmp_path):
    from test_runtime_core import Backend

    from ahc.application.runtime_service import RuntimeService

    repo, snapshot, request = setup(tmp_path)
    RuntimeService(repo, Backend()).execute(snapshot, request)
    before = repo.db_path.read_bytes()
    report = inspect_runtime_health(repo, snapshot.session)
    assert report["observation_scope"] == "persisted-state-counts"
    assert report["domains"]["turn_effects"] == {
        "status": "observed", "state_counts": {"resolved": 1}}
    assert repo.db_path.read_bytes() == before
