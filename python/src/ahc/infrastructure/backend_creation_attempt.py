"""Persist pre-effect attempt keys and reconcile them without replaying work."""

import json
from dataclasses import asdict

from ahc.domain.backend_creation import BackendCreationAttempt, BackendCreationCancelled
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.domain.native_process_identity import (
    NativeCreationAttempt, attempt_from_dict, matches_attempt,
)


def initialize(repository):
    repository._mutate(lambda c: c.execute(
        "CREATE TABLE IF NOT EXISTS backend_creation_attempts ("
        "owner_key TEXT PRIMARY KEY, workspace_json TEXT NOT NULL, "
        "attempt_json TEXT NOT NULL, record_digest TEXT NOT NULL)"))


def persist(custody, lease, workspace, attempt):
    workspace_json = json.dumps(workspace.to_dict(), sort_keys=True)
    if attempt is not None and type(attempt) not in (BackendCreationAttempt, NativeCreationAttempt):
        raise ContractError("invalid prepared backend attempt")

    def write(c):
        c.execute("INSERT INTO backend_creation_intents VALUES (?,?,'pending') "
                  "ON CONFLICT(owner_key) DO UPDATE SET workspace_json=excluded.workspace_json, "
                  "state='pending'", (lease.key, workspace_json))
        c.execute("DELETE FROM backend_creation_attempts WHERE owner_key=?", (lease.key,))
        if attempt is not None:
            record = {"owner_key": lease.key, "workspace": workspace.to_dict(),
                      "attempt": asdict(attempt)}
            c.execute("INSERT INTO backend_creation_attempts VALUES (?,?,?,?)",
                      (lease.key, workspace_json, json.dumps(asdict(attempt), sort_keys=True),
                       digest_json(record)))
    custody.repository._mutate(write)


def reconcile(custody, lease, runner):
    # Caller holds the owner guard and execution lock for observation + commit.
    from .backend_custody import BackendCustodyUnavailable
    from .backend_creation_rejection import resolve_rejected_intent

    with custody.repository._read() as c:
        pending = c.execute("SELECT * FROM backend_creation_intents "
                            "WHERE owner_key=? AND state='pending'", (lease.key,)).fetchone()
        if pending is None:
            return
        row = c.execute("SELECT * FROM backend_creation_attempts WHERE owner_key=?",
                        (lease.key,)).fetchone()
    if row is None or not callable(getattr(runner, "reconcile_creation", None)):
        raise BackendCustodyUnavailable("backend creation indeterminate; replay prohibited")
    try:
        workspace = WorkspaceIdentity.from_dict(json.loads(row["workspace_json"]))
        attempt = attempt_from_dict(json.loads(row["attempt_json"]))
        if (row["workspace_json"] != pending["workspace_json"]
                or workspace.session != lease.session or workspace.workspace_id != lease.workspace
                or digest_json({"owner_key": lease.key, "workspace": workspace.to_dict(),
                                "attempt": asdict(attempt)}) != row["record_digest"]):
            raise ValueError()
    except (ValueError, TypeError, KeyError, ContractError):
        raise BackendCustodyUnavailable("backend creation attempt binding invalid") from None
    result = runner.reconcile_creation(attempt)
    if type(result) is BackendCreationCancelled and result.attempt == attempt:
        resolve_rejected_intent(custody, lease, workspace)
    elif matches_attempt(result, attempt):
        # Recovery may close an older workspace generation. This records custody
        # for cleanup only; attachment/admission still require their own guards.
        custody._record_created(lease, workspace, result, resolve_intent=True, recovering=True)
    else:
        raise BackendCustodyUnavailable("backend creation reconciliation unverified")
