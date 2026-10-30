"""Core runtime tables, created with the existing working-continuity database."""

from .child_artifact_schema import TABLES as CHILD_ARTIFACT_TABLES
from .runtime_learning_queue import TABLES as LEARNING_TABLES

_TABLES = CHILD_ARTIFACT_TABLES + LEARNING_TABLES + (
    """CREATE TABLE IF NOT EXISTS runtime_native_artifacts (
    child_id TEXT NOT NULL REFERENCES runtime_children(child_id), native_artifact_id TEXT NOT NULL,
    reference_json TEXT NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES runtime_artifacts(artifact_id),
    PRIMARY KEY(child_id,native_artifact_id))""",
    """CREATE TABLE IF NOT EXISTS runtime_native_budgets (
    child_id TEXT PRIMARY KEY REFERENCES runtime_children(child_id),
    parent_operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
    limits_json TEXT NOT NULL, allocation_json TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_native_children (
    child_id TEXT PRIMARY KEY REFERENCES runtime_children(child_id),
    envelope_digest TEXT NOT NULL, binding_json TEXT NOT NULL, binding_digest TEXT NOT NULL,
    operation_id TEXT, receipt_json TEXT, receipt_digest TEXT)""",
    """CREATE TABLE IF NOT EXISTS runtime_channel_progress (
    operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id), call_id TEXT NOT NULL,
    route_json TEXT NOT NULL, text_digest TEXT NOT NULL, attempted_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('claimed','sent','unknown')),
    claim_token TEXT, receipt_json TEXT, PRIMARY KEY(operation_id,call_id))""",
    """CREATE TABLE IF NOT EXISTS runtime_channel_attachments (
    operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
    ordinal INTEGER NOT NULL, route_json TEXT NOT NULL, selection_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','claimed','sent','unknown')),
    claim_token TEXT, receipt_json TEXT, PRIMARY KEY(operation_id,ordinal))""",
    """CREATE TABLE IF NOT EXISTS runtime_artifacts (
    artifact_id TEXT PRIMARY KEY, manifest_json TEXT NOT NULL, content BLOB NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_channel_routes (
    operation_id TEXT PRIMARY KEY, route_json TEXT NOT NULL, event_id TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_channel_outbox (
    operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),
    route_json TEXT NOT NULL, payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','claimed','sent','unknown')),
    claim_token TEXT, receipt_json TEXT)""",
    """CREATE TABLE IF NOT EXISTS workspace_heads (
    session_json TEXT NOT NULL, workspace_id TEXT NOT NULL,
    generation INTEGER NOT NULL, revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('ready','executing')),
    PRIMARY KEY(session_json, workspace_id))""",
    """CREATE TABLE IF NOT EXISTS workspace_checkpoints (
    session_json TEXT NOT NULL, workspace_id TEXT NOT NULL,
    generation INTEGER NOT NULL, revision INTEGER NOT NULL,
    digest TEXT NOT NULL UNIQUE, origin_json TEXT NOT NULL, values_json TEXT NOT NULL,
    PRIMARY KEY(session_json, workspace_id, generation, revision))""",
    """CREATE TABLE IF NOT EXISTS runtime_turns (
    operation_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL,
    session_json TEXT NOT NULL, admitted_snapshot TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('submitted','terminal')),
    result_json TEXT)""",
    """CREATE TABLE IF NOT EXISTS runtime_answer_seals (
    operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),
    request_digest TEXT NOT NULL, session_json TEXT NOT NULL, result_json TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_intents (
    operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),
    kind TEXT NOT NULL CHECK(kind IN ('mailbox','parked')),
    session_json TEXT NOT NULL, payload_json TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_children (
    child_id TEXT PRIMARY KEY, session_json TEXT NOT NULL,
    parent_operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
    request_digest TEXT NOT NULL, envelope_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','claimed','cancelled','terminal','unknown')),
    claim_token TEXT, result_json TEXT)""",
    """CREATE TABLE IF NOT EXISTS runtime_batch_receipts (
    operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
    call_id TEXT NOT NULL, input_digest TEXT NOT NULL, receipt_json TEXT,
    PRIMARY KEY(operation_id,call_id))""",
    """CREATE TABLE IF NOT EXISTS runtime_schedules (
    schedule_id TEXT PRIMARY KEY, session_json TEXT NOT NULL,
    revision INTEGER NOT NULL, state TEXT NOT NULL,
    schedule_json TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_schedule_occurrences (
    schedule_id TEXT NOT NULL REFERENCES runtime_schedules(schedule_id),
    due_ms INTEGER NOT NULL, operation_id TEXT NOT NULL UNIQUE,
    schedule_revision INTEGER NOT NULL, PRIMARY KEY(schedule_id,due_ms))""",
    """CREATE TABLE IF NOT EXISTS runtime_schedule_cursors (
    schedule_id TEXT PRIMARY KEY REFERENCES runtime_schedules(schedule_id),
    through_ms INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_mcp_receipts (
    operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
    call_id TEXT NOT NULL, binding_digest TEXT NOT NULL, status TEXT,
    PRIMARY KEY(operation_id,call_id))""",
)
