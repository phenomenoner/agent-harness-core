"""Read-only recall through a trusted profile-bound service, never model-selected scope."""

from ahc.application.memory_pack_service import MemoryPackUnavailable
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def recall(workspace, service, arguments):
    try:
        if (type(arguments) is not dict
                or not {"expected", "query"} <= set(arguments)
                or set(arguments) - {"expected", "query", "budget_tokens", "max_items"}):
            raise ContractError("memory Pack tool fields invalid")
        expected = WorkspaceIdentity.from_dict(arguments["expected"])
        if expected != workspace.identity:
            raise ContractError("memory Pack workspace mismatch")
        result = service.pack(expected.session, arguments["query"],
                              budget_tokens=arguments.get("budget_tokens", 1200),
                              max_items=arguments.get("max_items", 8))
        return {"disposition": "observed", "result": result.to_dict(),
                "provenance": result.summary(), "content_authority": "untrusted-data",
                "unresolved_effects": False}
    except ContractError:
        return {"disposition": "rejected", "reason": "invalid-memory-pack-request",
                "unresolved_effects": False}
    except MemoryPackUnavailable:
        return {"disposition": "unavailable", "reason": "memory-pack-unavailable",
                "unresolved_effects": False}
