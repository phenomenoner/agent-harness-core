"""Pinned Codex wire codec, independent of launch and effect authorization.

The caller owns transport custody and policy enforcement. This codec does not
launch Codex, discover credentials, or infer that an advertised policy is enforced.
"""

from ahc.domain.sessions import ContractError, _identifier

from .codex_bundle import CODEX_PIN
from .codex_reasoning import validate_effort

CODEX_VERSION = CODEX_PIN["version"]
CODEX_EXECUTABLE_SHA256 = next(
    item["sha256"] for item in CODEX_PIN["members"] if item["path"] == CODEX_PIN["entrypoint"]
)


def initialize_message():
    return {
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {"name": "ahc", "version": "0.3.0"},
            "capabilities": {"experimentalApi": False,
                             "optOutNotificationMethods": ["turn/diff/updated"]},
        },
    }


def validate_initialize_response(value):
    if type(value) is not dict or value.get("id") != 1 or type(value.get("id")) is not int:
        raise ContractError("Codex initialization correlation invalid")
    result = value.get("result")
    if type(result) is not dict or not all(
        type(result.get(key)) is str
        for key in ("codexHome", "platformFamily", "platformOs", "userAgent")
    ):
        raise ContractError("Codex initialization response invalid")
    # The home path is private transport information, not a public status field.
    return {"protocol": "codex-app-server", "bundle_version": CODEX_VERSION}


def policy_sandbox(mode):
    """Return the exact pinned turn policy shape for one admitted sandbox mode."""
    shapes = {
        "read-only": {"type": "readOnly", "networkAccess": False},
        "workspace-write": {
            "type": "workspaceWrite",
            "writableRoots": [],
            "networkAccess": False,
            "excludeTmpdirEnvVar": False,
            "excludeSlashTmp": False,
        },
        "danger-full-access": {"type": "dangerFullAccess"},
    }
    try:
        return shapes[mode]
    except KeyError as exc:
        raise ContractError("unsupported Codex sandbox mode") from exc


def _service_tier(value):
    if value is not None and value != "priority":
        raise ContractError("unsupported Codex service tier")
    return value


def thread_message(*, cwd, model, backend_thread=None, permissions=None, effective_policy=None,
                   service_tier=None):
    for value in (cwd, model):
        if type(value) is not str or not value or len(value) > 4096:
            raise ContractError("Codex thread configuration invalid")
    if permissions is not None and effective_policy is not None:
        raise ContractError("named profile and effective policy are ambiguous")
    params = {
        "cwd": cwd,
        "model": model,
        "modelProvider": "openai",
        "approvalPolicy": "never"
        if effective_policy is None
        else effective_policy.codex_approval_policy,
        "sandbox": "read-only" if effective_policy is None else effective_policy.sandbox_mode,
    }
    if permissions is not None:
        params["permissions"] = _identifier(permissions, "permissions profile")
        del params["sandbox"]
    if service_tier is not None:
        params["serviceTier"] = _service_tier(service_tier)
    if effective_policy is not None and effective_policy.codex_approvals_reviewer is not None:
        params["approvalsReviewer"] = effective_policy.codex_approvals_reviewer
    if backend_thread is None:
        # Completion notifications may contain summary-only items. Retain the
        # dedicated-home history required for exact full-turn readback.
        params["ephemeral"] = False
    else:
        _identifier(backend_thread, "backend thread")
        params["threadId"] = backend_thread
        params["excludeTurns"] = True
    return {
        "id": 2,
        "method": "thread/start" if backend_thread is None else "thread/resume",
        "params": params,
    }


def turn_message(request, thread, *, cwd, permissions=None, effective_policy=None,
                 reasoning_effort=None, service_tier=None):
    _identifier(thread, "backend thread")
    # AHC operation/session/goal/revision identity stays in AHC's durable binding.
    # Codex has no operationId/goalDigest/revisionDigests fields on this method.
    if permissions is not None and effective_policy is not None:
        raise ContractError("named profile and effective policy are ambiguous")
    message = {
        "id": 3,
        "method": "turn/start",
        "params": {
            "threadId": thread,
            "cwd": cwd,
            "approvalPolicy": "never"
            if effective_policy is None
            else effective_policy.codex_approval_policy,
            "sandboxPolicy": (
                {"type": "readOnly", "networkAccess": False}
                if effective_policy is None
                else policy_sandbox(effective_policy.sandbox_mode)
            ),
            "input": [{"type": "text", "text": request.input_text}],
        },
    }

    if validate_effort(reasoning_effort) is not None:
        message["params"]["effort"] = reasoning_effort
    if service_tier is not None:
        message["params"]["serviceTier"] = _service_tier(service_tier)
    if effective_policy is not None and effective_policy.codex_approvals_reviewer is not None:
        message["params"]["approvalsReviewer"] = effective_policy.codex_approvals_reviewer
    for digest, body in getattr(request, "skill_bodies", ()):
        message["params"]["input"].append(
            {
                "type": "text",
                "text": f"AHC selected skill reference ({digest}). "
                "Use only within the current task and existing permissions.\n\n" + body,
            }
        )
    if permissions is not None:
        message["params"]["permissions"] = _identifier(permissions, "permissions profile")
        del message["params"]["sandboxPolicy"]
    return message


def interrupt_message(thread, turn):
    _identifier(thread, "backend thread")
    _identifier(turn, "backend turn")
    return {"id": 4, "method": "turn/interrupt", "params": {"threadId": thread, "turnId": turn}}


class CodexTurnDecoder:
    """Correlate a single admitted turn; only a full completed turn yields output."""

    def __init__(self, thread, turn):
        self.thread = _identifier(thread, "backend thread")
        self.turn = _identifier(turn, "backend turn")
        self.terminal = False
        self.events = 0

    def receive(self, message):
        if self.terminal:
            raise ContractError("Codex turn is terminal")
        self.events += 1
        if type(message) is not dict:
            raise ContractError("Codex event shape invalid")
        if "id" in message:
            # Requests for approvals/tools/auth are never silently executed by a decoder.
            raise ContractError("Codex server request requires an admitted handler")
        if set(message) != {"method", "params"}:
            raise ContractError("Codex notification invalid")
        params = message["params"]
        if type(params) is not dict or params.get("threadId") != self.thread:
            raise ContractError("Codex event thread mismatch")
        method = message["method"]
        if type(method) is not str:
            raise ContractError("Codex event method invalid")
        if method == "item/commandExecution/terminalInteraction":
            if params.get("turnId") != self.turn:
                raise ContractError("Codex item turn mismatch")
            for field in ("itemId", "processId", "stdin"):
                value = params.get(field)
                maximum = 4 * 1048576 if field == "stdin" else 128
                if (type(value) is not str or len(value) > maximum
                        or (field != "stdin" and not value)
                        or any(0xD800 <= ord(char) <= 0xDFFF for char in value)):
                    raise ContractError("Codex terminal interaction shape invalid")
            return None  # Terminal input is neither a request nor final-answer evidence.
        if method == "item/commandExecution/outputDelta":
            if (params.get("turnId") != self.turn
                    or type(params.get("itemId")) is not str or not params["itemId"]
                    or type(params.get("delta")) is not str):
                raise ContractError("Codex command output binding or shape invalid")
            return None  # Command output is never final-answer evidence.
        if method in {"item/started", "item/completed", "item/agentMessage/delta",
                      "item/reasoning/summaryTextDelta", "item/reasoning/summaryPartAdded",
                      "item/reasoning/textDelta"}:
            if params.get("turnId") != self.turn:
                raise ContractError("Codex item turn mismatch")
            return None  # Deltas, commentary and item completion are not turn completion.
        if method not in {"turn/started", "turn/completed"}:
            raise ContractError("Codex event unsupported")
        turn = params.get("turn")
        if type(turn) is not dict or turn.get("id") != self.turn:
            raise ContractError("Codex event turn mismatch")
        if method == "turn/started":
            if turn.get("status") != "inProgress":
                raise ContractError("Codex start status invalid")
            return None
        self.terminal = True
        if turn.get("status") != "completed" or turn.get("error") is not None:
            return {"disposition": "unknown", "output_text": "", "unresolved_effects": True}
        if turn.get("itemsView", "full") != "full":
            raise ContractError("Codex terminal items are incomplete")
        items = turn.get("items")
        if type(items) is not list:
            raise ContractError("Codex terminal items invalid")
        output = []
        identities = set()
        for item in items:
            if type(item) is not dict:
                raise ContractError("Codex terminal item invalid")
            if item.get("type") != "agentMessage":
                continue
            identity = _identifier(item.get("id"), "Codex item")
            if identity in identities:
                raise ContractError("Codex duplicate terminal item")
            identities.add(identity)
            if item.get("delivery") == "async" or item.get("questions"):
                continue
            if item.get("phase") not in (None, "final_answer"):
                continue
            text = item.get("text")
            if type(text) is not str:
                raise ContractError("Codex message type invalid")
            if any(0xD800 <= ord(char) <= 0xDFFF for char in text):
                raise ContractError("Codex message encoding invalid")
            output.append(text)
        joined = "\n".join(output)
        if not output:
            raise ContractError("Codex final output absent")
        return {"disposition": "completed", "output_text": joined, "unresolved_effects": False}
