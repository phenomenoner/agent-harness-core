"""Bounded durable observations, never inferred provider causes or authority."""

from ahc.domain.sessions import ContractError

from .working_repository import WorkingSetCorrupt

CODES = frozenset({'cancellation-observed', 'deadline-observed',
    'parent-or-claim-changed', 'stop-observation-unavailable', 'factory-unverified',
    'execution-unverified', 'cleanup-unverified', 'port-invalid',
    'completion-reply-lost', 'completion-not-committed',
    'parent-retired', 'unfinished-claim-recovered'})


def record(connection, envelope, codes):
    codes = tuple(codes)
    if len(codes) > len(CODES) or any(code not in CODES for code in codes):
        raise ContractError('child observation invalid')
    if not codes:
        return
    connection.execute('''CREATE TABLE IF NOT EXISTS runtime_child_observations (
        child_id TEXT NOT NULL REFERENCES runtime_children(child_id),
        envelope_digest TEXT NOT NULL, code TEXT NOT NULL,
        PRIMARY KEY(child_id,code))''')
    for code in codes:
        connection.execute('INSERT OR IGNORE INTO runtime_child_observations VALUES(?,?,?)',
                           (envelope.child_id, envelope.request_digest, code))
    read(connection, envelope)


def read(connection, envelope):
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='runtime_child_observations'").fetchone() is None:
        return ()
    rows = connection.execute('SELECT envelope_digest,code FROM runtime_child_observations '
                              'WHERE child_id=? ORDER BY code', (envelope.child_id,)).fetchall()
    if len(rows) > len(CODES) or any(row[0] != envelope.request_digest or row[1] not in CODES
                                    for row in rows):
        raise WorkingSetCorrupt('child observation binding invalid')
    return tuple(row[1] for row in rows)


def reconcile_claims(children, parent):
    def write(connection):
        children._require_parent(connection, parent)
        # Validate before changing state so corrupt claim evidence is retained.
        children._records(connection, parent)
        for row in connection.execute(
                "SELECT * FROM runtime_children WHERE session_json=? "
                "AND parent_operation_id=? AND state='claimed'",
                (children._parent_json(parent), parent.operation_id)).fetchall():
            envelope, _ = children._decode(row)
            record(connection, envelope, ("unfinished-claim-recovered",))
        connection.execute(
            "UPDATE runtime_children SET state='unknown',claim_token=NULL "
            "WHERE session_json=? AND parent_operation_id=? AND state='claimed'",
            (children._parent_json(parent), parent.operation_id),
        )
        return children._records(connection, parent)

    with children.store.execution_lock():
        return children.store._mutate(write)
