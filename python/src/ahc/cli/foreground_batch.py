"""Explicit owner-selected read and reversible-local grants for trusted foreground v2."""

from ahc.application.capability_view import _issue_agent_capability_view
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_local_batch import PAUSE
from ahc.infrastructure.runtime_query_port import CAPABILITIES

LOCAL_CAPABILITIES = (*CAPABILITIES, PAUSE)


def parse_batch(raw, session):
    if "batch" not in raw:
        return {}
    value = raw["batch"]
    if (
        type(value) is not dict
        or set(value) != {"agent_id", "capabilities"}
        or value["agent_id"] != session.agent_id
        or type(value["capabilities"]) is not list
        or not 1 <= len(value["capabilities"]) <= len(LOCAL_CAPABILITIES)
        or any(type(name) is not str or name not in LOCAL_CAPABILITIES
               for name in value["capabilities"])
        or len(set(value["capabilities"])) != len(value["capabilities"])
    ):
        raise ContractError("foreground batch grants invalid")
    return {"batch": {"agent_id": session.agent_id,
                      "capabilities": list(value["capabilities"])}}


def compose_batch(config, store):
    from ahc.infrastructure.runtime_local_batch import RuntimeLocalBatch
    from ahc.infrastructure.runtime_read_batch import RuntimeReadBatch

    value = config.to_dict().get("batch")
    if value is None:
        return None
    names = value["capabilities"]
    view = _issue_agent_capability_view(
        tuple(CapabilityDefinition(name, ProfileResourceKind.WORKSPACE,
                                   EffectClass.REVERSIBLE_LOCAL if name == PAUSE
                                   else EffectClass.READ_ONLY, ApprovalMode.NONE)
              for name in names),
        tuple((name, "runtime") for name in names),
    )
    batch_type = RuntimeLocalBatch if PAUSE in names else RuntimeReadBatch
    return batch_type(store, config.session, config.workspace_id, view)
