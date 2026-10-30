"""Persist Linux unit identities under role-specific lifetime leases.
Native runners must enforce full stop identity; name lookup alone is insufficient.
"""

import json
import os
import threading
from contextlib import contextmanager
from dataclasses import asdict

import portalocker

from ahc.domain.backend_identity import (
    LinuxUnitIdentity as LinuxUnitIdentity,
)
from ahc.domain.backend_identity import (
    UnitObservation as UnitObservation,
)
from ahc.domain.backend_identity import (
    UnitRetirement as UnitRetirement,
)
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.native_process_identity import (
    NativeProcessIdentity, NativeProcessObservation, custody_key, identity_from_dict,
)
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity

from .working_db import require_regular_file
from .working_repository import WorkingSetCorrupt, WorkingWriterBusy


class BackendCustodyUnavailable(RuntimeError):
    pass


class BackendLease:
    def __init__(self, custody, session, workspace, key, lock):
        self.custody, self.session, self.workspace = custody, session, workspace
        self.key, self.lock = key, lock
        self.active, self.pid = True, os.getpid()
        self.guard = threading.Lock()

    def close(self):
        # The owner cannot be handed off while a recovery operation is in flight.
        with self.guard:
            if self.active:
                if self.pid != os.getpid():
                    raise ContractError("backend lease belongs to another process")
                self.lock.release()
                self.active = False


class BackendCustody:
    def _execution_lock(self, lease):
        return self.repository.backend_execution_lock(
            lease.session, lease.workspace, resource_role=self.resource_role
        )

    @contextmanager
    def attachment(self, lease, unit):
        """Keep ownership stable while handing off a recorded unit's stream."""
        self._require(lease)
        with (
            lease.guard,
            self._execution_lock(lease),
        ):
            self._require(lease)
            self._no_pending_creation(lease)
            if type(unit) not in (LinuxUnitIdentity, NativeProcessIdentity) or not any(
                value == unit for _, _, value in self._held(lease)
            ):
                raise ContractError("attachment requires a persisted held unit")
            yield

    def __init__(self, repository, *, resource_role="backend"):
        if type(resource_role) is not str or resource_role not in ("backend", "python-workspace"):
            raise ContractError("unsupported process custody role")
        self._resource_role = resource_role
        self.repository = repository
        repository._mutate(
            lambda c: c.execute(
                "CREATE TABLE IF NOT EXISTS backend_custody ("
                "unit_key TEXT PRIMARY KEY, owner_key TEXT NOT NULL, record_json TEXT NOT NULL, "
                "record_digest TEXT NOT NULL, state TEXT NOT NULL "
                "CHECK(state IN ('held','cleaned')))"
            )
        )
        repository._mutate(
            lambda c: c.execute(
                "CREATE TABLE IF NOT EXISTS backend_creation_intents ("
                "owner_key TEXT PRIMARY KEY, workspace_json TEXT NOT NULL, "
                "state TEXT NOT NULL CHECK(state IN ('pending','recorded')))"
            )
        )
        from .backend_creation_attempt import initialize
        initialize(repository)

    def _no_pending_creation(self, lease):
        with self.repository._read() as c:
            row = c.execute(
                "SELECT state FROM backend_creation_intents WHERE owner_key=?", (lease.key,)
            ).fetchone()
        if row is not None and row["state"] != "recorded":
            raise BackendCustodyUnavailable("backend creation indeterminate; replay prohibited")

    def create(self, lease, workspace, runner, command, *, lifetime_seconds):
        """Persist intent before effects; retain it on any interrupted handoff."""
        self._require(lease)
        with (
            lease.guard,
            self._execution_lock(lease),
        ):
            self._require(lease)
            self._no_pending_creation(lease)
            if (
                type(workspace) is not WorkspaceIdentity
                or workspace.session != lease.session
                or workspace.workspace_id != lease.workspace
            ):
                raise ContractError("backend workspace owner mismatch")
            self.repository.require_workspace(workspace)
            if self._held(lease):
                raise BackendCustodyUnavailable("backend cleanup required before creation")
            from .backend_creation_attempt import persist
            prepare = getattr(runner, "prepare_creation", None)
            attempt = prepare(command, lifetime_seconds=lifetime_seconds) if callable(prepare) else None
            persist(self, lease, workspace, attempt)
            from .backend_creation_rejection import create_or_resolve_rejection

            unit = create_or_resolve_rejection(
                self, lease, workspace, runner, command, lifetime_seconds, attempt=attempt)
            self._record_created(lease, workspace, unit, resolve_intent=True)
            return unit

    def acquire(self, session, workspace):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("backend custody requires exact session")
        _identifier(workspace, "workspace")
        key = digest_json(
            {"session": session.to_dict(), "workspace": workspace, **self._role_binding()}
        )
        path = self.repository.root / ("backend-owner-" + key + ".lock")
        require_regular_file(path, allow_absent=True)
        lock = portalocker.Lock(
            str(path), flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0
        )
        try:
            lock.acquire()
        except portalocker.exceptions.LockException:
            raise WorkingWriterBusy("backend owner remains active") from None
        return BackendLease(self, session, workspace, key, lock)

    @property
    def resource_role(self):
        return self._resource_role

    def _role_binding(self):
        return {} if self.resource_role == "backend" else {"resource_role": self.resource_role}

    def _require(self, lease):
        if (
            type(lease) is not BackendLease
            or lease.custody is not self
            or not lease.active
            or lease.pid != os.getpid()
        ):
            raise ContractError("active backend custody lease required")

    def record_created(self, lease, workspace, unit):
        self._require(lease)
        with (
            lease.guard,
            self._execution_lock(lease),
        ):
            self._no_pending_creation(lease)
            self._record_created(lease, workspace, unit)

    def _record_created(self, lease, workspace, unit, *, resolve_intent=False, recovering=False):
        self._require(lease)
        if (
            type(workspace) is not WorkspaceIdentity
            or type(unit) not in (LinuxUnitIdentity, NativeProcessIdentity)
            or workspace.session != lease.session
            or workspace.workspace_id != lease.workspace
        ):
            raise ContractError("backend workspace owner mismatch")
        if not recovering:
            self.repository.require_workspace(workspace)
        record = {"workspace": workspace.to_dict(), "unit": asdict(unit), **self._role_binding()}
        digest = digest_json(record)
        key = custody_key(unit)

        def write(c):
            row = c.execute("SELECT * FROM backend_custody WHERE unit_key=?", (key,)).fetchone()
            if row is not None:
                if (
                    row["owner_key"] != lease.key
                    or row["record_digest"] != digest
                    or row["state"] != "held"
                ):
                    raise ContractError("backend unit cannot be rebound")
                return
            c.execute(
                "INSERT INTO backend_custody VALUES (?,?,?,?, 'held')",
                (key, lease.key, json.dumps(record, sort_keys=True), digest),
            )
            if resolve_intent:
                changed = c.execute(
                    "UPDATE backend_creation_intents SET state='recorded' "
                    "WHERE owner_key=? AND workspace_json=? AND state='pending'",
                    (lease.key, json.dumps(workspace.to_dict(), sort_keys=True)),
                ).rowcount
                if changed != 1:
                    raise WorkingSetCorrupt("backend creation intent unavailable")

        self.repository._mutate(write)

    def _held(self, lease):
        with self.repository._read() as c:
            rows = c.execute(
                "SELECT * FROM backend_custody WHERE owner_key=? AND state='held'", (lease.key,)
            ).fetchall()
        result = []
        for row in rows:
            try:
                record = json.loads(row["record_json"])
                workspace = WorkspaceIdentity.from_dict(record["workspace"])
                unit = identity_from_dict(record["unit"])
                key = custody_key(unit)
                if (
                    digest_json(record) != row["record_digest"]
                    or key != row["unit_key"]
                    or workspace.session != lease.session
                    or workspace.workspace_id != lease.workspace
                    or record != {"workspace": workspace.to_dict(), "unit": asdict(unit),
                                  **self._role_binding()}
                ):
                    raise ValueError()
            except (KeyError, ValueError, TypeError, ContractError):
                raise WorkingSetCorrupt("backend custody record invalid") from None
            result.append((key, row["record_digest"], unit))
        return result

    @staticmethod
    def _observe(runner, unit):
        value = runner.observe(unit)
        if type(unit) is NativeProcessIdentity:
            if (type(value) is not NativeProcessObservation or value.identity != unit
                    or value.state not in ("running", "stopped", "absent")
                    or type(value.cgroup_empty) is not bool
                    or (value.state == "absent" and not value.cgroup_empty)):
                raise BackendCustodyUnavailable("native process identity or presence unverified")
            return value
        retired = False
        if (
            type(value) is UnitObservation
            and (value.distribution, value.unit) == (unit.distribution, unit.unit)
            and value.boot_id != unit.boot_id
            and value.state == "absent"
            and value.invocation_id is None
            and value.cgroup_empty is True
            and callable(getattr(runner, "confirm_retirement", None))
        ):
            proof = runner.confirm_retirement(unit)
            retired = (
                type(proof) is UnitRetirement
                and proof.identity == unit
                and proof.current_boot_id == value.boot_id
            )
        if (
            type(value) is not UnitObservation
            or value.state not in ("running", "stopped", "absent")
            or (value.distribution, value.unit) != (unit.distribution, unit.unit)
            or (value.boot_id != unit.boot_id and not retired)
            or (value.state != "absent" and value.invocation_id != unit.invocation_id)
            or (value.state == "absent" and value.invocation_id is not None)
        ):
            raise BackendCustodyUnavailable("backend unit identity or presence unverified")
        return value

    def recover(self, lease, runner):
        self._require(lease)
        with (
            lease.guard,
            self._execution_lock(lease),
        ):
            self._require(lease)
            from .backend_creation_attempt import reconcile
            reconcile(self, lease, runner)
            self._no_pending_creation(lease)
            recovered = []
            for key, digest, unit in self._held(lease):
                before = self._observe(runner, unit)
                stop_error = None
                if before.state == "running" or before.cgroup_empty is not True:
                    if before.state == "absent":
                        raise BackendCustodyUnavailable("backend cgroup absence unverified")
                    try:
                        runner.stop(unit)
                    except BackendCustodyUnavailable as exc:
                        # A stop reply can be lost after its effect. Reconcile
                        # once through the existing read-only identity check;
                        # never replay the destructive action.
                        stop_error = exc
                try:
                    after = self._observe(runner, unit)
                except BackendCustodyUnavailable:
                    if stop_error is not None:
                        raise stop_error
                    raise
                if after.state not in ("stopped", "absent") or after.cgroup_empty is not True:
                    if stop_error is not None:
                        raise stop_error
                    raise BackendCustodyUnavailable("backend cleanup unverified")
                self.repository._mutate(
                    lambda c, key=key, digest=digest: c.execute(
                        "UPDATE backend_custody SET state='cleaned' "
                        "WHERE unit_key=? AND owner_key=? AND record_digest=? AND state='held'",
                        (key, lease.key, digest),
                    )
                )
                recovered.append(unit)
            return tuple(recovered)
