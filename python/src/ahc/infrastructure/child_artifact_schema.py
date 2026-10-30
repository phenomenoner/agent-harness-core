"""Additive preparation for explicitly enabled child artifacts on older runtimes."""

TABLES = (
    """CREATE TABLE IF NOT EXISTS runtime_child_artifact_limits (
    child_id TEXT PRIMARY KEY REFERENCES runtime_children(child_id),
    max_count INTEGER NOT NULL, max_bytes INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runtime_child_artifacts (
    child_id TEXT NOT NULL REFERENCES runtime_children(child_id),
    artifact_id TEXT NOT NULL REFERENCES runtime_artifacts(artifact_id),
    PRIMARY KEY(child_id,artifact_id))""",
)


def prepare_child_artifact_schema(store, *, session=None, workspace_id=None):
    # Activation is owned by foreground composition, never doctor or constructor reads.
    with store.execution_lock(session=session, workspace_id=workspace_id):
        def write(connection):
            for statement in TABLES:
                connection.execute(statement)
        store._mutate(write)
