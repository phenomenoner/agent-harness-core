"""Agent tools for bound memory; approval issuance remains an operator command."""

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


class WorkspaceNativeMemory:
    def __init__(self, observe, store, *, revisions=None):
        self.observe, self.store = observe, store
        self.revisions = revisions

    def invoke(self, workspace, arguments):
        action = None
        try:
            if type(arguments) is not dict or set(arguments) != {"expected", "action", "payload"}:
                raise ContractError("native memory tool fields invalid")
            expected = WorkspaceIdentity.from_dict(arguments["expected"])
            if getattr(workspace, "closed", False) or expected != workspace.identity:
                raise ContractError("native memory workspace mismatch")
            action, payload = arguments["action"], arguments["payload"]
            fields = {
                "status": set(),
                "get": {"record_ref"},
                "resolve": {"row_id"},
                "propose": {"operation_id", "observation", "origin", "expires_at"},
                "store": {"operation_id", "observation"},
                "execute": {"proposal_digest"},
                "reconcile": {"proposal_digest"},
            }
            if self.revisions is not None:
                fields.update({
                    "revise": {"operation_id", "expected_ref", "observation"},
                    "tombstone": {"operation_id", "expected_ref"},
                    "reconcile_revision": {"revision_digest"},
                })
            if (
                type(action) is not str
                or action not in fields
                or type(payload) is not dict
                or set(payload) != fields[action]
            ):
                raise ContractError("native memory action invalid")
            if action == "status":
                result = self.observe.status(expected.session)
            elif action == "get":
                result = self.observe.get(expected.session, payload["record_ref"])
            elif action == "resolve":
                result = self.observe.resolve(expected.session, payload["row_id"])
            elif action == "propose":
                result = {
                    "status": "pending-review",
                    "proposal_digest": self.store.propose(expected.session, **payload),
                }
            elif action == "execute":
                result = self.store.execute(expected.session, payload["proposal_digest"])
            elif action == "store":
                result = self.store.store_trusted(expected.session, **payload)
            elif action in ("revise", "tombstone"):
                if action == "revise" and payload["observation"] is None:
                    raise ContractError("correction requires an observation")
                result = self.revisions.revise_trusted(expected.session, **payload)
            elif action == "reconcile_revision":
                result = self.revisions.reconcile_only(expected.session, payload["revision_digest"])
            else:
                result = self.store.reconcile_only(expected.session, payload["proposal_digest"])
            unknown = result.get("status") == "unknown"
            return {
                "disposition": "unknown" if unknown else "observed",
                "result": result,
                "unresolved_effects": unknown,
            }
        except ContractError:
            return {
                "disposition": "rejected",
                "reason": "native-memory-request-invalid",
                "unresolved_effects": False,
            }
        except Exception:
            unknown = action in ("execute", "store", "revise", "tombstone")
            return {
                "disposition": "unknown" if unknown else "unavailable",
                "reason": "native-memory-unavailable",
                "unresolved_effects": unknown,
            }
