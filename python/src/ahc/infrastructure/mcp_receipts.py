"""Exact-parent MCP call reservations; no arguments, credentials or result bodies."""

from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex


class McpReceipts:
    def __init__(self, store, request, *, profile_digest=None):
        self.store, self.request = store, request
        if profile_digest is not None:
            _sha256_hex(profile_digest)
        self.profile_digest = profile_digest

    def _parent(self, connection, *, active=False):
        request = self.request
        row = connection.execute("SELECT * FROM runtime_turns WHERE operation_id=?",
                                 (request.operation_id,)).fetchone()
        if (row is None or row["request_digest"] != request_fingerprint(request)
                or row["session_json"] != canonical_json(request.session.to_dict()).decode()
                or (active and row["status"] != "submitted")):
            raise ContractError("MCP receipt parent invalid")
        if active:
            current = self.store._load_current(connection, request.session.ref.virtual_session_id)
            if (current.session != request.session or current.status != "active"
                    or current.goal.digest != request.goal_digest):
                raise ContractError("MCP receipt current owner invalid")

    def begin(self, session, call_id, server_id, capability_digest, tool, arguments):
        if session != self.request.session:
            raise ContractError("MCP receipt session mismatch")
        _identifier(call_id, "MCP call")
        _identifier(server_id, "MCP server")
        _sha256_hex(capability_digest)
        binding = digest_json({"profile": self.profile_digest,
                               "server": server_id, "capabilities": capability_digest,
                               "tool": tool, "arguments": arguments})

        def write(connection):
            self._parent(connection, active=True)
            if connection.execute(
                "SELECT 1 FROM runtime_mcp_receipts WHERE operation_id=? AND call_id=?",
                (self.request.operation_id, call_id),
            ).fetchone() is not None:
                raise ContractError("durable MCP call cannot be replayed")
            connection.execute("INSERT INTO runtime_mcp_receipts VALUES(?,?,?,NULL)",
                               (self.request.operation_id, call_id, binding))
        self.store._mutate(write)

    def finish(self, call_id, status):
        if status not in ("observed", "tool-error", "unknown"):
            raise ContractError("MCP receipt status invalid")

        def write(connection):
            self._parent(connection)
            changed = connection.execute(
                "UPDATE runtime_mcp_receipts SET status=? "
                "WHERE operation_id=? AND call_id=? AND status IS NULL",
                (status, self.request.operation_id, call_id),
            ).rowcount
            if changed != 1:
                raise ContractError("MCP receipt absent or already completed")
        self.store._mutate(write)

    def read(self):
        with self.store._read() as connection:
            self._parent(connection)
            return _rows(connection, self.request.operation_id)


def _rows(connection, operation_id):
    rows = connection.execute(
        "SELECT * FROM runtime_mcp_receipts WHERE operation_id=? ORDER BY rowid",
        (operation_id,),
    ).fetchall()
    result = []
    for row in rows:
        _sha256_hex(row["binding_digest"])
        _identifier(row["call_id"], "MCP call")
        if row["status"] not in (None, "observed", "tool-error", "unknown"):
            raise ContractError("MCP receipt corrupted")
        result.append({"call_id": row["call_id"], "binding_digest": row["binding_digest"],
            "status": row["status"] or "unknown",
            "completion_recorded": row["status"] is not None})
    return tuple(result)


def read_mcp_receipts(store, session, operation_id):
    from ahc.domain.sessions import VirtualSessionIdentity

    if type(session) is not VirtualSessionIdentity:
        raise ContractError("MCP receipt session invalid")
    _identifier(operation_id, "MCP parent")
    with store._read() as connection:
        connection.execute("BEGIN")
        row = connection.execute(
            "SELECT session_json FROM runtime_turns WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if row is None:
            return ()
        if row["session_json"] != canonical_json(session.to_dict()).decode():
            raise ContractError("MCP receipt owner mismatch")
        return _rows(connection, operation_id)
