"""Successful-create custody in the runtime DB, guarded by an OS lifetime lease."""

import json
import re

import portalocker

from ahc.adapters.docker_workspace import OwnedContainer, WorkspaceUnavailable
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity

from .working_db import require_regular_file
from .working_repository import WorkingSetCorrupt, WorkingWriterBusy

_TABLE = """CREATE TABLE IF NOT EXISTS workspace_custody (
container_id TEXT PRIMARY KEY, owner_key TEXT NOT NULL,
record_json TEXT NOT NULL, record_digest TEXT NOT NULL,
state TEXT NOT NULL CHECK(state IN ('held','removed')))"""


class CustodyLease:
    def __init__(self, custody, session, workspace_id, lock):
        self.custody = custody
        self.session = session
        self.workspace_id = workspace_id
        self.key = digest_json({"session": session.to_dict(), "workspace": workspace_id})
        self.lock = lock
        self.active = True

    def close(self):
        if self.active:
            self.lock.release()
            self.active = False


class WorkspaceCustody:
    def __init__(self, repository):
        self.repository = repository
        repository._mutate(lambda connection: connection.execute(_TABLE))

    def acquire(self, session, workspace_id):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("custody requires exact session")
        _identifier(workspace_id, "workspace")
        key = digest_json({"session": session.to_dict(), "workspace": workspace_id})
        path = self.repository.root / ("workspace-owner-" + key + ".lock")
        require_regular_file(path, allow_absent=True)
        lock = portalocker.Lock(
            str(path), flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0
        )
        try:
            lock.acquire()
        except portalocker.exceptions.LockException:
            raise WorkingWriterBusy("workspace owner remains active") from None
        return CustodyLease(self, session, workspace_id, lock)

    def _require(self, lease):
        if type(lease) is not CustodyLease or lease.custody is not self or not lease.active:
            raise ContractError("active workspace custody lease required")

    def record_created(self, lease, identity, owned):
        self._require(lease)
        if (
            type(identity) is not WorkspaceIdentity
            or identity.session != lease.session
            or identity.workspace_id != lease.workspace_id
            or type(owned) is not OwnedContainer
            or owned.removed
        ):
            raise ContractError("successful creation with exact workspace owner required")
        self.repository.require_workspace(identity)
        identifier = owned.container_id
        if re.fullmatch(r"[0-9a-f]{64}", identifier) is None:
            raise ContractError("full created container ID required")
        record = {
            "identity": identity.to_dict(),
            "container_id": identifier,
            "nonce": owned.nonce,
            "command": list(owned.command),
        }
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"))
        digest = digest_json(record)

        def write(connection):
            row = connection.execute(
                "SELECT * FROM workspace_custody WHERE container_id=?", (identifier,)
            ).fetchone()
            if row is not None:
                if row["record_digest"] != digest or row["state"] != "held":
                    raise ContractError("container custody already bound")
                return
            connection.execute(
                "INSERT INTO workspace_custody VALUES (?,?,?,?, 'held')",
                (identifier, lease.key, encoded, digest),
            )

        self.repository._mutate(write)

    def held(self, lease):
        self._require(lease)
        with self.repository._read() as connection:
            rows = connection.execute(
                "SELECT * FROM workspace_custody WHERE owner_key=? AND state='held'", (lease.key,)
            ).fetchall()
        records = []
        for row in rows:
            try:
                value = json.loads(row["record_json"])
                identity = WorkspaceIdentity.from_dict(value["identity"])
                if (
                    digest_json(value) != row["record_digest"]
                    or value["container_id"] != row["container_id"]
                    or re.fullmatch(r"[0-9a-f]{64}", value["container_id"]) is None
                    or identity.session != lease.session
                    or identity.workspace_id != lease.workspace_id
                ):
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                raise WorkingSetCorrupt("workspace custody record invalid") from None
            records.append(value)
        return tuple(records)

    def recover(self, lease, runner):
        """No live owner holds this lease; remove only persisted successful IDs."""
        self._require(lease)
        with self.repository.execution_lock():
            records = self.held(lease)
            recovered = []
            for record in records:
                identifier = record["container_id"]
                query = [
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--filter",
                    "id=" + identifier,
                    "--format",
                    "{{.ID}}",
                ]
                before = runner(query)
                if before.code != 0 or before.stdout.strip() not in ("", identifier):
                    raise WorkspaceUnavailable("custody container presence is unverified")
                if before.stdout.strip():
                    runner(["rm", "--force", identifier])
                after = runner(query)
                if after.code != 0 or after.stdout.strip():
                    raise WorkspaceUnavailable("custody container absence is unverified")
                record_digest = digest_json(record)
                self.repository._mutate(
                    lambda connection, target=identifier, digest=record_digest: (
                        connection.execute(
                            "UPDATE workspace_custody SET state='removed' "
                            "WHERE container_id=? AND owner_key=? AND record_digest=?",
                            (target, lease.key, digest),
                        )
                    )
                )
                recovered.append(identifier)
            return tuple(recovered)
