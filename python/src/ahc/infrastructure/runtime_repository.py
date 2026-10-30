"""Turn/intent transactions in the existing working-continuity database."""

import json
from contextlib import ExitStack, contextmanager

import portalocker

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.runtime_turn import RuntimeTurn, request_fingerprint
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition

from . import runtime_repository_workspace as _workspace
from .runtime_approval_store import persist_policy
from .runtime_foreground_lease import acquire_foreground_lease
from .runtime_repository_schema import _TABLES
from .working_db import insert_snapshot, require_regular_file, snapshot_pointer
from .working_repository import WorkingSetConflict, WorkingSetRepository, WorkingWriterBusy


class RuntimeRepository(WorkingSetRepository):
    foreground_lease = acquire_foreground_lease
    @contextmanager
    def execution_lock(self, *, restore=False, session=None, workspace_id=None):
        """Independent scopes share the root; recovery keeps exclusive root ownership."""
        scope = None
        if session is not None or workspace_id is not None:
            if restore or type(session) is not VirtualSessionIdentity:
                raise ContractError("scoped execution requires a session and cannot restore")
            scope = (session, _identifier(workspace_id, "execution workspace"))
        path = self.root / "runtime-execution.lock"
        require_regular_file(path, allow_absent=True)
        try:
            with ExitStack() as stack:
                mode = portalocker.LOCK_EX if scope is None else portalocker.LOCK_SH
                stack.enter_context(portalocker.Lock(
                    str(path), flags=mode | portalocker.LOCK_NB, timeout=0))
                if scope is not None:
                    key = digest_json({"session": session.to_dict(), "workspace": workspace_id})
                    scoped_path = self.root / ("runtime-execution-" + key + ".lock")
                    require_regular_file(scoped_path, allow_absent=True)
                    stack.enter_context(portalocker.Lock(
                        str(scoped_path), flags=portalocker.LOCK_EX | portalocker.LOCK_NB, timeout=0))
                from .runtime_execution_scope import held_execution

                with held_execution(self, scope=scope):
                    if not restore and any((self.root / n).exists() for n in (
                            "restore-pending.json", "sidecar-pending.json")):
                        raise ContractError("runtime restore requires reconciliation")
                    yield
        except portalocker.exceptions.LockException as exc:
            raise WorkingWriterBusy("runtime execution or recovery is busy") from exc

    def turn_execution_lock(self, request):
        return self.execution_lock(session=request.session, workspace_id=request.workspace_scope)

    @contextmanager
    def workspace_turn_scope(self, request):
        from .runtime_execution_scope import workspace_turn

        turn = self.read_turn(request.session, request.operation_id)
        if (
            turn is None
            or turn.status != "submitted"
            or turn.request_digest != request_fingerprint(request)
        ):
            raise ContractError("workspace grant requires a durable pending turn")
        with workspace_turn(self, request):
            yield

    @contextmanager
    def backend_execution_lock(self, session, workspace_id, *, resource_role="backend"):
        """Reuse only the active same-thread, same-owner durable turn grant."""
        from .runtime_execution_scope import (
            permits_evaluation_workspace,
            permits_workspace_scope,
            permits_workspace_startup,
        )

        if (resource_role == "python-workspace"
                and permits_evaluation_workspace(self, session, workspace_id)) or (
            permits_workspace_scope(self, session, workspace_id)
        ) or (
            resource_role == "python-workspace"
            and permits_workspace_startup(self, session, workspace_id)
        ):
            yield
        else:
            with self.execution_lock(session=session, workspace_id=workspace_id):
                yield

    @contextmanager
    def workspace_creation_lock(self, session, workspace_id):
        from .runtime_execution_scope import permits_evaluation_workspace

        if permits_evaluation_workspace(self, session, workspace_id):
            yield
        else:
            with self.execution_lock(session=session, workspace_id=workspace_id):
                yield

    @contextmanager
    def workspace_startup_scope(self, identity):
        from .runtime_execution_scope import workspace_startup

        self.require_workspace(identity)
        with workspace_startup(self, identity):
            yield

    @contextmanager
    def workspace_execution_lock(self, identity):
        from .runtime_execution_scope import (
            permits_evaluation_workspace,
            permits_workspace,
            workspace_startup,
        )

        if (permits_evaluation_workspace(self, identity.session, identity.workspace_id)
                or permits_workspace(self, identity)):
            yield
        else:
            with self.execution_lock(session=identity.session, workspace_id=identity.workspace_id):
                self.require_workspace(identity)
                with workspace_startup(self, identity):
                    yield

    @classmethod
    def initialize(cls, root):
        repository = super().initialize(root)

        def operation(connection):
            for statement in _TABLES:
                connection.execute(statement)

        repository._mutate(operation)
        return repository

    @staticmethod
    def _turn(row) -> RuntimeTurn:
        return RuntimeTurn(
            row["operation_id"],
            row["request_digest"],
            row["status"],
            None if row["result_json"] is None else json.loads(row["result_json"]),
        )

    @staticmethod
    def _advance(connection, current, successor):
        validate_working_set_transition(current, successor)
        insert_snapshot(connection, successor)
        pointer = snapshot_pointer(successor)
        connection.execute(
            "UPDATE current_working_sets SET generation=?,revision=?,agent_id=?,route_id=?,"
            "snapshot_digest=? WHERE virtual_session_id=?",
            (*pointer[1:], pointer[0]),
        )

    def begin(self, expected: WorkingSetSnapshot, request: AgentTurnRequest):
        if expected.session != request.session or expected.goal.digest != request.goal_digest:
            raise ContractError("runtime admission session or goal mismatch")
        fingerprint = request_fingerprint(request)

        def operation(connection):
            from .runtime_inbox import RuntimeInbox

            if RuntimeInbox._exists(connection):
                submission = connection.execute(
                    "SELECT * FROM runtime_submissions WHERE operation_id=?",
                    (request.operation_id,),
                ).fetchone()
                if submission is not None and RuntimeInbox._decode(
                    submission
                ) != RuntimeInbox._payload(
                    request.session, request.operation_id, request.goal_digest, request.input_text
                ):
                    raise ContractError("runtime request differs from local submission")
            row = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            if row is not None:
                if row["request_digest"] != fingerprint:
                    raise ContractError("runtime operation already bound to different request")
                return self._turn(row), False
            from .channel_control_store import ChannelSubmissionHeld, restart_pending
            from .runtime_channel_store import _exists

            if restart_pending(connection, request.session):
                raise ChannelSubmissionHeld("new admission fenced by pending service restart")

            if _exists(connection, "channel_held") and connection.execute(
                    "SELECT 1 FROM channel_held WHERE operation_id=?",
                    (request.operation_id,)).fetchone():
                raise ChannelSubmissionHeld("submission held by a new conversation")
            current = self._load_current(connection, request.session.ref.virtual_session_id)
            if (
                current != expected
                or current.status != "active"
                or current.active_turn_id is not None
            ):
                raise WorkingSetConflict("runtime admission lost current session authority")
            successor = current.revise(active_turn_id=request.operation_id)
            self._advance(connection, current, successor)
            connection.execute(
                "INSERT INTO runtime_turns VALUES(?,?,?,?,?,NULL)",
                (
                    request.operation_id,
                    fingerprint,
                    canonical_json(request.session.to_dict()).decode(),
                    successor.snapshot_digest,
                    "submitted",
                ),
            )
            persist_policy(connection, request)
            from .runtime_learning_queue import prepare_source
            prepare_source(connection, request)
            return RuntimeTurn(request.operation_id, fingerprint, "submitted", None), True

        return self._mutate(operation)

    def finish(self, request, result, *, fail_before_commit=False):
        fingerprint = request_fingerprint(request)

        def operation(connection):
            row = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            if row is None or row["request_digest"] != fingerprint:
                raise ContractError("runtime terminal has no matching admission")
            if row["status"] == "terminal":
                return self._turn(row)
            return self._terminal(connection, row, result)

        return self._mutate(operation, fail_before_commit=fail_before_commit)

    def seal_observation(self, request, result):
        """Persist one exact accepted answer before any later resource cleanup."""
        from ahc.domain.runtime_turn import answer_seal_projection

        fingerprint = request_fingerprint(request)
        projection = answer_seal_projection(result)
        encoded = canonical_json(projection).decode()

        def operation(connection):
            connection.execute(
                "CREATE TABLE IF NOT EXISTS runtime_answer_seals ("
                "operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),"
                "request_digest TEXT NOT NULL,session_json TEXT NOT NULL,result_json TEXT NOT NULL)"
            )
            row = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            if (
                row is None
                or row["request_digest"] != fingerprint
                or row["session_json"] != canonical_json(request.session.to_dict()).decode()
            ):
                raise ContractError("answer seal has no matching admission")
            existing = connection.execute(
                "SELECT * FROM runtime_answer_seals WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_digest"] != fingerprint
                    or existing["session_json"] != row["session_json"]
                    or canonical_json(answer_seal_projection(
                        json.loads(existing["result_json"]))).decode() != encoded
                ):
                    raise ContractError("answer seal differs from the recorded completion")
                return
            current = self._load_current(connection, request.session.ref.virtual_session_id)
            if (
                row["status"] != "submitted"
                or current.status != "active"
                or current.active_turn_id != request.operation_id
                or current.snapshot_digest != row["admitted_snapshot"]
            ):
                raise ContractError("answer seal requires the current active admission")
            connection.execute(
                "INSERT INTO runtime_answer_seals VALUES(?,?,?,?)",
                (request.operation_id, fingerprint, row["session_json"], encoded),
            )

        self._mutate(operation)

    def read_answer_seal(self, session, operation_id):
        with self._read() as connection:
            if not connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_answer_seals'"
            ).fetchone():
                return None
            row = connection.execute(
                "SELECT * FROM runtime_answer_seals WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                return None
            if row["session_json"] != canonical_json(session.to_dict()).decode():
                raise ContractError("answer seal owner mismatch")
            return json.loads(row["result_json"])

    def _terminal(self, connection, row, result):
        from .child_work_repository import ChildWorkRepository
        from .runtime_channel_store import enqueue_final
        ChildWorkRepository.retire_parent(connection, row["operation_id"])
        session = VirtualSessionIdentity.from_dict(json.loads(row["session_json"]))
        if (
            type(result) is dict
            and result.get("result_schema_version") is not None
            and (result.get("accepted") is True or result.get("report_accepted") is True)
        ):
            from ahc.domain.runtime_turn import answer_seal_projection

            projection = canonical_json(answer_seal_projection(result)).decode()
            seal = connection.execute(
                "SELECT * FROM runtime_answer_seals WHERE operation_id=?", (row["operation_id"],)
            ).fetchone()
            if (
                seal is None
                or seal["request_digest"] != row["request_digest"]
                or seal["session_json"] != row["session_json"]
                or canonical_json(answer_seal_projection(
                    json.loads(seal["result_json"]))).decode() != projection
            ):
                raise ContractError("runtime answer seal is unavailable or mismatched")
        current = self._load_current(connection, session.ref.virtual_session_id)
        if current.snapshot_digest == row["admitted_snapshot"] and current.status == "active":
            from .runtime_result_artifacts import validate_selected
            validate_selected(connection, session, result)
            self._advance(connection, current, current.revise(active_turn_id=None))
        else:
            result = {
                "disposition": "unknown",
                "accepted": False,
                "unresolved_effects": True,
                "output_text": "",
                "reason": "superseded",
            }
        encoded = canonical_json(result).decode()
        connection.execute(
            "UPDATE runtime_turns SET status='terminal',result_json=? WHERE operation_id=?",
            (encoded, row["operation_id"]),
        )
        connection.execute(
            "INSERT INTO runtime_intents VALUES(?,?,?,?)",
            (
                row["operation_id"],
                "mailbox" if result.get("accepted") is True else "parked",
                row["session_json"],
                encoded,
            ),
        )
        enqueue_final(connection, row, result)
        from .runtime_learning_queue import settle_source
        settle_source(connection, row, result)
        return RuntimeTurn(row["operation_id"], row["request_digest"], "terminal", result)

    def reconcile(self, session: VirtualSessionIdentity) -> tuple[RuntimeTurn, ...]:
        """Explicit recovery only; never invokes a backend or asserts no external effect."""

        def operation(connection):
            rows = connection.execute(
                "SELECT * FROM runtime_turns WHERE session_json=? AND status='submitted'",
                (canonical_json(session.to_dict()).decode(),),
            ).fetchall()
            return tuple(
                self._terminal(
                    connection,
                    row,
                    {
                        "disposition": "unknown",
                        "accepted": False,
                        "unresolved_effects": True,
                        "output_text": "",
                        "reason": "restart-unreconciled",
                    },
                )
                for row in rows
            )

        with self.execution_lock():
            return self._mutate(operation)

    def read_turn(self, session, operation_id):
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                return None
            if json.loads(row["session_json"]) != session.to_dict():
                raise ContractError("runtime read owner mismatch")
            return self._turn(row)

    def intents(self, session):
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_intents WHERE session_json=?",
                (canonical_json(session.to_dict()).decode(),),
            ).fetchall()
        return tuple(
            {
                "operation_id": row["operation_id"],
                "kind": row["kind"],
                "session": json.loads(row["session_json"]),
                "result": json.loads(row["payload_json"]),
            }
            for row in rows
        )

    def recent_terminal_turns(self, session, operation_id, *, limit=16):
        """Bounded exact-generation history anchored before a durable operation.

        Replaying an older operation never includes later turns. Reading this
        projection creates neither runtime state nor recovery authority.
        """
        if type(session) is not VirtualSessionIdentity or type(limit) is not int or not 1 <= limit <= 16:
            raise ContractError("turn history scope or limit invalid")
        encoded = canonical_json(session.to_dict()).decode()
        with self._read() as connection:
            anchor = connection.execute(
                "SELECT rowid,session_json,status FROM runtime_turns WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if anchor is None:
                return ()
            if anchor["session_json"] != encoded:
                raise ContractError("turn history owner mismatch")
            if anchor["status"] != "terminal":
                return ()
            rows = connection.execute(
                "SELECT * FROM runtime_turns WHERE session_json=? AND rowid<=? "
                "AND status='terminal' ORDER BY rowid DESC LIMIT ?",
                (encoded, anchor["rowid"], limit),
            ).fetchall()
        return tuple(self._turn(row) for row in reversed(rows))

    def save_workspace_checkpoint(self, expected, checkpoint, *, fail_before_commit=False):
        return _workspace.save_workspace_checkpoint(
            self, expected, checkpoint, fail_before_commit=fail_before_commit
        )

    def read_workspace_checkpoint(self, session, workspace_id, digest):
        return _workspace.read_workspace_checkpoint(self, session, workspace_id, digest)

    def reserve_workspace_generation(self, expected, workspace_id, checkpoint=None):
        return _workspace.reserve_workspace_generation(self, expected, workspace_id, checkpoint)

    def _require_workspace(self, connection, identity, state):
        return _workspace.require_workspace(self, connection, identity, state)

    def require_workspace(self, identity):
        return _workspace.require_workspace_ready(self, identity)

    def observe_workspace_step(self, identity):
        return _workspace.observe_workspace_step(self, identity)

    def begin_workspace_step(self, identity):
        return _workspace.begin_workspace_step(self, identity)

    def finish_workspace_step(self, identity):
        return _workspace.finish_workspace_step(self, identity)
