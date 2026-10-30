"""Explicit local read tools; neither initialization nor runtime dispatch."""

import json

from ahc.application.tool_batch import BatchRejected
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.turns import _sha256_hex

from .artifact_repository import ArtifactRepository
from .runtime_inbox import RuntimeInbox

CAPABILITIES = ("runtime.result.read", "runtime.artifact.manifest")


class RuntimeQueryPort:
    # Every repository read creates and closes its own readonly SQLite connection.
    concurrent_reads = True

    def __init__(self, store, session):
        self.store, self.session = store, session

    def invoke(self, session, call, deadline, cancelled):
        try:
            if session != self.session or call.resource_id != "runtime":
                raise ContractError("query binding invalid")
            arguments = json.loads(call.arguments_json)
            if call.capability_id == "runtime.result.read":
                if set(arguments) != {"operation_id"}:
                    raise ContractError("query fields invalid")
                _identifier(arguments["operation_id"], "operation_id")
            elif call.capability_id == "runtime.artifact.manifest":
                if set(arguments) != {"artifact_id"}:
                    raise ContractError("query fields invalid")
                _sha256_hex(arguments["artifact_id"])
            else:
                raise ContractError("query unsupported")
        except (ContractError, ValueError, TypeError):
            raise BatchRejected("invalid local query") from None
        if call.capability_id == "runtime.result.read":
            return RuntimeInbox(self.store).query(session, arguments["operation_id"])
        value = ArtifactRepository(self.store).read(session, arguments["artifact_id"])
        return None if value is None else value[0].to_dict()
