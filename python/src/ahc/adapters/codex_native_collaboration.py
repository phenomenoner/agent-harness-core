"""Correlate native descendant notifications without granting parent tool authority.

The app-server owns native agents. This conversation-local observer correlates
spawn events; it never launches, resumes, interrupts or bills a child itself.
"""

from types import SimpleNamespace

from ahc.domain.sessions import ContractError, _identifier

from .codex_protocol import CodexTurnDecoder
from .codex_reasoning import validate_effort
from .codex_status_notifications import consume_status

_TOOLS = frozenset({"spawnAgent", "sendInput", "resumeAgent", "wait", "closeAgent",
                    "sendMessage", "followupTask", "interruptAgent", "listAgents"})
_STATES = frozenset({"pendingInit", "running", "interrupted", "completed", "errored",
                     "shutdown", "notFound"})


class NativeCollaboration:
    def __init__(self, client):
        self.client = client
        self.children = {}
        self.spawns = {}
        self.seen_spawns = set()
        self.seen_calls = set()
        self.calls = {}
        self.pending = []
        self.activities = set()

    def consume(self, message):
        """Return true only for internally retained or validated child frames."""
        client = self.client
        if "id" in message or client.thread is None or client.turn is None:
            return False  # Requests retain the existing strict parent bridges.
        params = message.get("params")
        if type(params) is not dict:
            return False
        method = message.get("method")
        thread = params.get("threadId")
        if method == "thread/started":
            value = params.get("thread")
            thread = value.get("id") if type(value) is dict else None
        if thread is None:
            return False
        if thread == client.thread:
            self._collaboration(message, client)
            if method == "turn/completed":
                try:
                    self.require_settled()
                except ContractError:
                    record = getattr(client, "_recovery_record", None)
                    if record is not None:
                        record["state"] = "blocked"
                    raise
            return False
        if set(message) != {"method", "params"}:
            raise ContractError("Codex native notification shape invalid")
        _identifier(thread, "native child thread")
        if thread not in self.children:
            # V2 can announce provenance before its parent activity item. This
            # establishes an observed descendant, never host-tool authority.
            if method == "thread/started" and not self.spawns:
                value = params.get("thread", {})
                source = value.get("source") if type(value) is dict else None
                subagent = source.get("subAgent") if type(source) is dict else None
                spawn = subagent.get("thread_spawn") if type(subagent) is dict else None
                parent = spawn.get("parent_thread_id") if type(spawn) is dict else None
                sender = client if parent == client.thread else self.children.get(parent)
                if sender is not None and getattr(sender, "turn", None) is not None:
                    if getattr(sender, "terminal", False):
                        raise ContractError("Codex native spawn parent is terminal")
                    self._enroll(sender, thread, None, None, spawn.get("agent_path"))
                    self._child(message, self.children[thread])
                    return True
            # Native turns can arrive before either provenance or parent activity.
            # Retain telemetry until exact enrollment; it grants no host authority.
            self.pending.append(message)
            return True
        self._child(message, self.children[thread])
        return True

    def _collaboration(self, message, sender):
        params = message["params"]
        item = params.get("item")
        if (message.get("method") in {"item/started", "item/completed"}
                and type(item) is dict and item.get("type") == "subAgentActivity"):
            self._activity(message, sender)
            return
        if (message.get("method") not in {"item/started", "item/completed"}
                or type(item) is not dict or item.get("type") != "collabAgentToolCall"):
            return
        if (set(message) != {"method", "params"} or params.get("turnId") != sender.turn
                or item.get("senderThreadId") != sender.thread):
            raise ContractError("Codex native collaboration sender mismatch")
        identity = _identifier(item.get("id"), "native spawn item")
        receivers, states = item.get("receiverThreadIds"), item.get("agentsStates")
        if (item.get("tool") not in _TOOLS
                or item.get("status") not in {"inProgress", "completed", "failed", "interrupted"}
                or type(receivers) is not list
                or any(type(value) is not str for value in receivers)
                or len(set(receivers)) != len(receivers) or type(states) is not dict
                or set(states) - set(receivers)):
            raise ContractError("Codex native collaboration shape invalid")
        for receiver in receivers:
            _identifier(receiver, "native receiver")
        for state in states.values():
            if (type(state) is not dict or state.get("status") not in _STATES
                    or set(state) - {"status", "message"}
                    or (state.get("message") is not None and type(state["message"]) is not str)):
                raise ContractError("Codex native agent state invalid")
        key = (sender.thread, sender.turn, identity)
        if item["tool"] != "spawnAgent":
            if any(value != self.client.thread and value not in self.children
                   for value in receivers):
                raise ContractError("Codex native receiver is not owned")
            if message["method"] == "item/started":
                if (key in self.seen_calls or key in self.seen_spawns
                        or item["status"] != "inProgress"):
                    raise ContractError("Codex native call start invalid")
                self.seen_calls.add(key)
                prior = {value: self.children[value].terminal for value in receivers
                         if value in self.children}
                self.calls[key] = (item["tool"], tuple(receivers), prior)
                if item["tool"] in {"followupTask", "sendInput", "resumeAgent"}:
                    for value in prior:
                        self.children[value].terminal = False
            else:
                call = self.calls.get(key)
                if (call is None or call[0] != item["tool"]
                        or item["status"] not in {"completed", "failed", "interrupted"}):
                    raise ContractError("Codex native call completion mismatch")
                if item["tool"] == "wait" and item["status"] == "completed":
                    # Wait returns only observed terminal targets; a timeout
                    # returns none. Neither observation settles child turns.
                    if (not set(receivers).issubset(call[1])
                            or set(states) != set(receivers)):
                        raise ContractError("Codex native wait completion mismatch")
                elif call[1] != tuple(receivers):
                    raise ContractError("Codex native call completion mismatch")
                del self.calls[key]
                # Failed calls remain unresolved if they might have awakened work.
            return  # Native tool status cannot establish child turn completion.
        model = item.get("model")
        if model is not None:
            _identifier(model, "native child model")
        effort = validate_effort(item.get("reasoningEffort"))
        binding = (model, effort)
        if message["method"] == "item/started":
            if (key in self.seen_spawns or key in self.seen_calls or receivers or states
                    or item["status"] != "inProgress"):
                raise ContractError("Codex native spawn start invalid")
            self.seen_spawns.add(key)
            self.spawns[key] = binding
            return
        if key not in self.spawns or self.spawns.pop(key) != binding:
            raise ContractError("Codex native spawn completion mismatch")
        if item["status"] != "completed":
            if item["status"] not in {"failed", "interrupted"} or receivers:
                raise ContractError("Codex native failed spawn invalid")
        else:
            if (len(receivers) != 1 or receivers[0] == self.client.thread
                    or receivers[0] in self.children):
                raise ContractError("Codex native child enrollment invalid")
            self._enroll(sender, receivers[0],
                         model if model is not None else sender.model,
                         effort if effort is not None else sender.reasoning_effort)

    def _enroll(self, sender, thread, model, effort, agent_path=None):
        if thread == self.client.thread or thread in self.children:
            raise ContractError("Codex native child enrollment invalid")
        child = SimpleNamespace(**{name: getattr(sender, name) for name in (
            "cwd", "service_tier", "_permissions", "_effective_policy", "platform_os")})
        child.thread, child.turn = thread, None
        child.model, child.reasoning_effort = model, effort
        child.token_observation, child._warning_owner = None, self.client
        child.parent, child.terminal, child.started = sender.thread, False, False
        child.turns, child.agent_path, child.activity_id = set(), None, None
        self.children[thread] = child
        if agent_path is not None:
            self._bind_path(child, agent_path)
        buffered, self.pending = self.pending, []
        for event in buffered:
            p = event["params"]
            identity = p.get("threadId", p.get("thread", {}).get("id"))
            if identity in self.children:
                self._child(event, self.children[identity])
            else:
                self.pending.append(event)

    @staticmethod
    def _path_parent(path):
        # Agent paths are provider labels, never host filesystem paths. Names
        # need no invented alphabet; only the canonical tree shape is relevant.
        if (type(path) is not str or not (path == "/root" or path.startswith("/root/"))
                or any(part in {"", ".", ".."} for part in path.split("/")[1:])):
            raise ContractError("Codex native activity path invalid")
        return None if path == "/root" else path.rsplit("/", 1)[0]

    def _bind_path(self, owner, path):
        # A legacy enrollment can have no path. Infer missing ancestor labels
        # only along the already-owned graph, checking every known binding.
        bindings = []
        while owner is not self.client:
            parent_path = self._path_parent(path)
            if (parent_path is None
                    or (owner.agent_path is not None and owner.agent_path != path)
                    or any(other is not owner and other.agent_path == path
                           for other in self.children.values())):
                raise ContractError("Codex native activity provenance mismatch")
            bindings.append((owner, path))
            owner = (self.client if owner.parent == self.client.thread
                     else self.children.get(owner.parent))
            if owner is None:
                raise ContractError("Codex native activity parent mismatch")
            path = parent_path
        if path != "/root":
            raise ContractError("Codex native activity parent mismatch")
        for owner, path in bindings:
            owner.agent_path = path

    def _activity(self, message, sender):
        params, item = message["params"], message["params"]["item"]
        if (set(message) != {"method", "params"} or params.get("turnId") != sender.turn
                or not {"type", "id", "kind", "agentThreadId", "agentPath"} <= set(item)
                or item.get("kind") not in {"started", "interacted", "interrupted", "completed"}):
            raise ContractError("Codex native activity binding invalid")
        identity = _identifier(item["id"], "native activity item")
        thread = _identifier(item["agentThreadId"], "native child thread")
        path = item["agentPath"]
        parent_path = self._path_parent(path)
        child = self.client if thread == self.client.thread else self.children.get(thread)
        if item["kind"] == "started":
            if parent_path is None:
                raise ContractError("Codex native activity path invalid")
            self._bind_path(sender, parent_path)
        elif child is None:
            raise ContractError("Codex native activity receiver is not owned")
        else:
            self._bind_path(child, path)
        if message["method"] == "item/started":
            return  # V2 direct-spawn evidence is its completed activity item.
        key = (sender.thread, sender.turn, identity)
        if key in self.activities or key in self.seen_calls or key in self.seen_spawns:
            raise ContractError("Codex native activity reused")
        self.activities.add(key)
        if item["kind"] == "started":
            if child is None:
                self._enroll(sender, thread, None, None, path)
                child = self.children[thread]
            if (child is self.client or child.parent != sender.thread
                    or child.activity_id is not None):
                raise ContractError("Codex native activity child mismatch")
            self._bind_path(child, path)
            child.activity_id = identity
        # Interactions and completion-shaped activity do not prove a child turn
        # started, finished, delivered a result, or gained parent tool authority.

    def respond(self, message):
        """Refuse an owned child's parent-host request without killing its parent."""
        if message.get("method") not in {
            "item/tool/call", "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval", "item/permissions/requestApproval",
        }:
            return False
        params = message.get("params")
        child = self.children.get(params.get("threadId")) if type(params) is dict else None
        if child is None or "id" not in message:
            return False
        from .codex_approval import _request_id

        identity = _request_id(message["id"])
        if child.turn is None or child.terminal or params.get("turnId") != child.turn:
            raise ContractError("Codex native child request binding invalid")
        if message.get("method") == "item/tool/call":
            self.client._send({"id": identity, "result": {"success": False, "contentItems": [{
                "type": "inputText", "text": "Native child cannot use the parent AHC workspace. "
                "Return the limitation to the parent; use an AHC-managed child for host tools.",
            }]}})
            return True
        if message.get("method") in {
            "item/commandExecution/requestApproval", "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
        }:
            self.client._send({"id": identity, "error": {
                "code": -32601, "message": "Native child cannot borrow parent approval authority"}})
            return True
        return False

    def _child(self, message, child):
        method, params = message["method"], message["params"]
        if method == "thread/started":
            thread = params.get("thread")
            if type(thread) is not dict or set(params) != {"thread"} or child.started:
                raise ContractError("Codex native thread notification invalid")
            source = thread.get("source")
            if type(source) is not dict or type(source.get("subAgent")) is not dict:
                raise ContractError("Codex native thread provenance absent")
            spawn = source["subAgent"].get("thread_spawn")
            if (type(spawn) is not dict or spawn.get("parent_thread_id") != child.parent
                    or thread.get("id") != child.thread):
                raise ContractError("Codex native thread provenance mismatch")
            child.started = True
            path = spawn.get("agent_path")
            if path is not None:
                self._bind_path(child, path)
            return
        if method == "turn/started":
            turn = params.get("turn")
            if (type(turn) is not dict or turn.get("status") != "inProgress"
                    or (child.turn is not None and not child.decoder.terminal)):
                raise ContractError("Codex native child turn start invalid")
            identifier = _identifier(turn.get("id"), "native child turn")
            if identifier in child.turns:
                raise ContractError("Codex native child turn reused")
            child.turns.add(identifier)
            child.turn, child.terminal = identifier, False
            child.decoder = CodexTurnDecoder(child.thread, identifier)
            child.decoder.receive(message)
            return
        if method == "thread/settings/updated" and child.model is None:
            settings = params.get("threadSettings")
            if type(settings) is not dict:
                raise ContractError("Codex native settings invalid")
            child.model = _identifier(settings.get("model"), "native observed model")
            child.reasoning_effort = validate_effort(settings.get("effort"))
        if consume_status(child, message):
            return
        if child.turn is None or child.terminal:
            raise ContractError("Codex native event has no active child turn")
        if method == "turn/completed":
            turn = params.get("turn")
            if (type(turn) is not dict or turn.get("id") != child.turn
                    or child.decoder.terminal
                    or turn.get("status") not in {"completed", "failed", "interrupted"}
                    or (turn["status"] == "completed" and turn.get("error") is not None)):
                raise ContractError("Codex native terminal binding invalid")
            child.terminal = True
            child.decoder.terminal = True
            return  # Child summary/final is never a parent answer or sealed result.
        self._collaboration(message, child)
        child.decoder.receive(message)

    def require_settled(self):
        if (self.spawns or self.calls
                or any(not c.terminal for c in self.children.values())):
            raise ContractError("Codex native descendants unresolved")
        # Unproven notifications are not evidence of owned work. They cannot
        # supply a parent result, authorize tools or make the parent wait forever.
        self.pending.clear()
