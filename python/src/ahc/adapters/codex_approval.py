"""One bounded translation from pinned Codex approval callbacks to AHC decisions."""

from ahc.application.approval_port import UnavailableApproval
from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json

from .codex_approval_display import approval_summary

_METHODS = frozenset({"item/commandExecution/requestApproval", "item/fileChange/requestApproval"})
_COMMAND_FIELDS = frozenset(
    {
        "additionalPermissions",
        "approvalId",
        "availableDecisions",
        "command",
        "commandActions",
        "cwd",
        "environmentId",
        "itemId",
        "kind",
        "networkApprovalContext",
        "proposedExecpolicyAmendment",
        "proposedNetworkPolicyAmendments",
        "reason",
        "startedAtMs",
        "threadId",
        "turnId",
    }
)
_FILE_FIELDS = frozenset({"grantRoot", "itemId", "reason", "startedAtMs", "threadId", "turnId"})


def effective_configuration_digest(base, policy):
    """Extend, but never alter, legacy and named-profile configuration identities."""
    return digest_json(
        {
            "base": base,
            "effective_policy": policy.to_dict(),
            "approval_bridge": "codex-vendor-v1",
        }
    )


def policy_readback_matches(response, policy, *, platform_os=None):
    expected = {
        "read-only": {"type": "readOnly", "networkAccess": False},
        "workspace-write": {
            "type": "workspaceWrite",
            "writableRoots": [],
            "networkAccess": False,
            "excludeTmpdirEnvVar": False,
            "excludeSlashTmp": False,
        },
        "danger-full-access": {"type": "dangerFullAccess"},
    }[policy.sandbox_mode]
    # The pinned Linux backend can explicitly enumerate /tmp and omit an unset
    # TMPDIR from its legacy policy readback. Admit only this observed projection;
    # cwd, additional roots, network, approval, and other platforms stay guarded.
    allowed = [expected]
    if platform_os == "linux" and policy.sandbox_mode == "workspace-write":
        allowed.append(dict(expected, writableRoots=["/tmp"], excludeTmpdirEnvVar=True))
    sandbox = response.get("sandbox")
    sandbox_matches = type(sandbox) is dict and any(
        canonical_json(sandbox) == canonical_json(candidate) for candidate in allowed
    )
    return (
        response.get("approvalPolicy") == policy.codex_approval_policy
        and sandbox_matches
        and (
            policy.codex_approvals_reviewer is None
            or response.get("approvalsReviewer") == policy.codex_approvals_reviewer
        )
    )


def _request_id(value):
    if type(value) is int and -(2**53 - 1) <= value <= 2**53 - 1:
        return value
    if (
        type(value) is str
        and 0 < len(value) <= 128
        and not any(ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF for char in value)
    ):
        return value
    raise ContractError("Codex approval request ID invalid")


def _bounded_text(value, label, *, nullable=True):
    if value is None and nullable:
        return
    if (
        type(value) is not str
        or len(value) > 4096
        or any(0xD800 <= ord(char) <= 0xDFFF for char in value)
    ):
        raise ContractError(f"Codex approval {label} invalid")


def _validate_params(method, params, thread, turn):
    if type(params) is not dict:
        raise ContractError("Codex approval params invalid")
    if len(canonical_json(params)) > 32_768:
        raise ContractError("Codex approval params exceed bound")
    expected = _COMMAND_FIELDS if method.startswith("item/command") else _FILE_FIELDS
    if set(params) - expected or not {"itemId", "startedAtMs", "threadId", "turnId"} <= set(params):
        raise ContractError("Codex approval params unsupported")
    _identifier(params["itemId"], "Codex approval item")
    if params["threadId"] != thread or params["turnId"] != turn:
        raise ContractError("Codex approval thread or turn mismatch")
    if type(params["startedAtMs"]) is not int or not -(2**63) <= params["startedAtMs"] < 2**63:
        raise ContractError("Codex approval timestamp invalid")
    for key in ("reason", "grantRoot", "command", "cwd", "approvalId", "environmentId"):
        if key in params:
            _bounded_text(params[key], key)
    if method.startswith("item/command"):
        if params.get("kind", "command") not in ("command", "writeStdin"):
            raise ContractError("Codex approval kind unsupported")
        for key in (
            "availableDecisions",
            "commandActions",
            "proposedExecpolicyAmendment",
            "proposedNetworkPolicyAmendments",
        ):
            if params.get(key) is not None and type(params[key]) is not list:
                raise ContractError("Codex approval optional list invalid")
        for key in ("additionalPermissions", "networkApprovalContext"):
            if params.get(key) is not None and type(params[key]) is not dict:
                raise ContractError("Codex approval optional object invalid")


class CodexApprovalBridge:
    """Correlate one current turn's vendor prompts without retaining vendor payloads."""

    def __init__(self, request, policy, thread, turn, *, clock, wall_clock, approval_port=None):
        self.request = request
        self.policy = policy
        self.thread = _identifier(thread, "backend thread")
        self.turn = _identifier(turn, "backend turn")
        self.clock = clock
        self.wall_clock = wall_clock
        self.port = UnavailableApproval() if approval_port is None else approval_port
        self.seen = set()

    def respond(self, message, *, cancelled):
        if type(message) is not dict or set(message) != {"id", "method", "params"}:
            raise ContractError("Codex approval request shape invalid")
        vendor_id = _request_id(message["id"])
        method = message["method"]
        if method not in _METHODS:
            raise ContractError("Codex approval method unsupported")
        identity = (type(vendor_id).__name__, vendor_id)
        if identity in self.seen:
            raise ContractError("Codex approval request duplicate")
        self.seen.add(identity)
        params = message["params"]
        _validate_params(method, params, self.thread, self.turn)
        outcome = "cancelled" if cancelled() else "deny"
        if self.policy.approval_mode == "always-approve" and outcome != "cancelled":
            outcome = "approve-once"
        elif self.policy.approval_mode in ("manual", "auto-approve") and outcome != "cancelled":
            remaining = min(self.request.effective_deadline - self.clock(), self.policy.approval_wait_seconds)
            if remaining <= 0:
                outcome = "timeout"
            else:
                approval = ApprovalRequest(
                    self.request.session,
                    self.request.operation_id,
                    self.policy.digest,
                    self.thread,
                    self.turn,
                    vendor_id,
                    digest_json({"method": method, "params": params}),
                    self.wall_clock() + remaining,
                    approval_summary(method, params),
                )
                decision = self.port.decide(approval, cancelled=cancelled)
                if type(decision) is not ApprovalDecision:
                    raise ContractError("Codex approval port response invalid")
                if self.clock() >= self.request.effective_deadline and not cancelled():
                    outcome = "timeout"
                else:
                    outcome = decision.resolve(
                        approval, now=self.wall_clock(), cancelled=cancelled()
                    )
        allowed = params.get("availableDecisions")
        can_accept = allowed is None or "accept" in allowed
        vendor_decision = (
            "accept"
            if outcome == "approve-once" and can_accept
            else ("cancel" if outcome == "cancelled" else "decline")
        )
        return {"id": vendor_id, "result": {"decision": vendor_decision}}


def validate_thread_configuration(client, response):
    """Read back exact selected vendor settings before admitting a turn."""
    if (
        response.get("cwd") != client.cwd
        or response.get("model") != client.model
        or response.get("modelProvider") != "openai"
        or (
            client.service_tier is not None
            and response.get("serviceTier") != client.service_tier
        )
        or (client._effective_policy is None and response.get("approvalPolicy") != "never")
        or (
            client._permissions is None
            and client._effective_policy is None
            and response.get("sandbox") != {"type": "readOnly", "networkAccess": False}
        )
        or (
            client._permissions is not None
            and response.get("activePermissionProfile")
            not in (
                {"id": client._permissions},
                {"id": client._permissions, "extends": None},
            )
        )
    ):
        raise ContractError("Codex thread configuration readback mismatch")
    if client._effective_policy is not None:
        if not policy_readback_matches(
            response, client._effective_policy, platform_os=client.platform_os
        ):
            raise ContractError("Codex thread configuration readback mismatch")
