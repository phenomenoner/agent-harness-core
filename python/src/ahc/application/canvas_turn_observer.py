"""Optional metadata-only task maps derived from existing durable observations."""

import hashlib
import json
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .symbolic_canvas import build

MAX_OUTPUT = 262144
MAX_FILES = 2048


class CanvasTurnObserver:
    def __init__(self, store, session, *, min_turns=2, trigger_mode="qualified"):
        if type(min_turns) is not int or not 1 <= min_turns <= 16 or trigger_mode not in ("qualified", "all"):
            raise ContractError("canvas observer configuration invalid")
        self.store, self.session = store, session
        self.min_turns, self.trigger_mode = min_turns, trigger_mode
        self.last_status = {"status": "idle", "observation_scope": "last-turn-hook"}

    def status(self):
        return json.loads(json.dumps(self.last_status))

    def observe(self, request, turn):
        # A diagnostic must never replace the already accepted answer or interfere
        # with cleanup. No exception text or source message is copied into status.
        self.last_status = {"status": "unavailable", "observation_scope": "last-turn-hook"}
        try:
            self.last_status.update(self._observe(request, turn))
        except Exception:
            self.last_status["status"] = "artifact-unavailable"
        return self.status()

    def _observe(self, request, turn):
        if request.session != self.session or request.operation_id != turn.operation_id:
            raise ContractError("canvas observer owner mismatch")
        if self.store.read_turn(self.session, turn.operation_id) != turn:
            raise ContractError("canvas observation differs from durable turn")
        if turn.status != "terminal" or turn.result is None:
            return {"status": "not-terminal"}
        result = turn.result
        explicit = any(key in request.input_text.casefold() for key in (
            "symbolic-canvas", "task canvas", "mermaid", "任務圖", "任務地圖"))
        qualified = (explicit or bool(result.get("artifacts"))
                     or result.get("unresolved_effects") is True
                     or result.get("disposition") != "completed")
        if self.trigger_mode == "qualified" and not qualified:
            return {"status": "routine-skipped"}
        turns = self.store.recent_terminal_turns(self.session, turn.operation_id)
        if len(turns) < self.min_turns:
            return {"status": "below-threshold", "turns": len(turns)}
        nodes, edges, refs = [], [], []
        for index, item in enumerate(turns):
            observed = item.result or {}
            state = observed.get("disposition", "unknown")
            if state not in ("completed", "unknown", "unsupported", "not-submitted"):
                state = "unknown"
            node = "turn-" + item.request_digest
            nodes.append({"id": node, "label": "Turn " + str(index + 1), "state": state})
            if index:
                edges.append(["turn-" + turns[index - 1].request_digest, node, "sequence"])
            refs.append({"source_id": node, "operation_id": item.operation_id,
                         "request_digest": item.request_digest,
                         "unresolved_effects": observed.get("unresolved_effects") is True,
                         "cleanup_pending": observed.get("cleanup_pending") is True,
                         "artifact_ids": [a["artifact_id"] for a in observed.get("artifacts", [])]})
        session_digest = digest_json(self.session.to_dict())
        raw = canonical_json({"task_id": session_digest, "nodes": nodes, "edges": edges})
        document = build(raw, Path(self.store.root))
        document.update(observation_kind="runtime-turn-navigation", session_digest=session_digest,
                        anchor_operation=turn.operation_id, anchor_request_digest=turn.request_digest,
                        runtime_references=refs, history_limit=16, goal_completion_verified=False)
        pair = (canonical_json(document) + b"\n", document["mermaid"].encode())
        if sum(map(len, pair)) > MAX_OUTPUT:
            raise ContractError("canvas output bound exceeded")
        directory = Path(self.store.root)
        for part in ("symbolic-canvas-auto", session_digest):
            directory = directory / part
            if directory.is_symlink() or getattr(directory, "is_junction", lambda: False)():
                raise ContractError("canvas directory cannot be a link")
            directory.mkdir(exist_ok=True)
        stem = hashlib.sha256(turn.operation_id.encode()).hexdigest()
        paths = (directory / (stem + ".json"), directory / (stem + ".mmd"))
        self.last_status["output_paths"] = [str(path) for path in paths]
        # Never silently overwrite a previous or partial observation. Identical
        # bytes allow a known missing counterpart to be completed on replay.
        existing = []
        for path, content in zip(paths, pair, strict=True):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise ContractError("canvas output cannot be a link")
            present = path.exists()
            if present:
                if not path.is_file() or path.stat().st_size > MAX_OUTPUT:
                    raise ContractError("canvas output is not a bounded regular file")
                with path.open("rb") as stream:
                    if stream.read(MAX_OUTPUT + 1) != content:
                        raise ContractError("canvas output conflicts with this observation")
            existing.append(present)
        if not all(existing):
            count = 0
            if existing.count(False) > MAX_FILES:
                return {"status": "quota-reached"}
            for _ in directory.iterdir():
                count += 1
                if count + existing.count(False) > MAX_FILES:
                    return {"status": "quota-reached"}
            for path, content, present in zip(paths, pair, existing, strict=True):
                if not present:
                    with path.open("xb") as stream:
                        stream.write(content)
        return {"status": "reused" if all(existing) else "written",
                "json_path": str(paths[0]), "mermaid_path": str(paths[1]),
                "json_sha256": hashlib.sha256(pair[0]).hexdigest(),
                "mermaid_sha256": hashlib.sha256(pair[1]).hexdigest(),
                "turns": len(turns), "goal_completion_verified": False}
