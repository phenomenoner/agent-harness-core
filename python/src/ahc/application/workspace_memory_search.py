"""Exact workspace-bound search over an already composed memory resource."""

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity

from .memory_search_service import MemorySearchUnavailable


def search(workspace, service, arguments):
    try:
        if (type(arguments) is not dict or not {"expected", "query"} <= set(arguments)
                or set(arguments) - {"expected", "query", "limit"}):
            raise ContractError("memory search fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        if expected != workspace.identity:
            raise ContractError("memory search workspace mismatch")
        result = service.search(expected.session, arguments["query"],
                                limit=arguments.get("limit", 8))
        return {"disposition": "observed", "result": result, "unresolved_effects": False}
    except ContractError:
        return {"disposition": "rejected", "reason": "invalid-memory-search-request",
                "unresolved_effects": False}
    except MemorySearchUnavailable:
        return {"disposition": "unavailable", "reason": "memory-search-unavailable",
                "unresolved_effects": False}
