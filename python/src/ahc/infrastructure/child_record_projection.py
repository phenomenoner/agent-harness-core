"""Project validated child ledger state and optional durable observations."""

def project_record(row, envelope, result, connection=None):
    from .child_observations import read
    observations = () if connection is None else read(connection, envelope)
    from .child_token_observations import read as read_tokens
    tokens = None if connection is None else read_tokens(connection, envelope)
    return {
        **({"observations": list(observations)} if observations else {}),
        **({"token_observation": tokens} if tokens is not None else {}),
        "child_id": envelope.child_id,
        "parent": envelope.parent.to_dict(),
        "request_digest": envelope.request_digest,
        "task_digest": envelope.task_digest,
        "artifact_scope": envelope.artifact_scope,
        "deadline_unix_ms": envelope.deadline_unix_ms,
        "execution_kind": envelope.execution_kind,
        "state": row["state"],
        "result": result,
    }
