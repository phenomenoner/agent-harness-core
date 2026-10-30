"""Model-directed skill trials over harness-owned experience references."""

import json
from dataclasses import replace

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import MARKDOWN_SKILL_SCHEMA
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity


class WorkspaceLearningTools:
    def __init__(self, experiments, session, *, evaluations=None, handoff=None):
        if experiments.owner != session.agent_id:
            raise ContractError("learning agent binding mismatch")
        self.experiments, self.session = experiments, session
        if evaluations is not None and (
            evaluations.experiments is not experiments or evaluations.factory.session != session
        ):
            raise ContractError("learning evaluation binding mismatch")
        self.evaluations = evaluations
        self.handoff = handoff
        self.controller = None
        self.context_id = digest_json(
            [session.agent_id, session.route_id, session.ref.virtual_session_id]
        )
        self.capture_gap = False

    def record_input(self, request):
        if request.session != self.session:
            raise ContractError("learning source session mismatch")
        identity = digest_json([request.session.to_dict(), request.operation_id])
        try:
            return self.experiments.record_experience(
                source_ref="input:" + identity,
                kind="user-input",
                source_digest=digest_json(request.input_text),
                receipt={
                    "operation_id": request.operation_id,
                    "learning_context": self.context_id,
                    "session": request.session.to_dict(),
                    "content": "retained-in-runtime",
                    "selected_revisions": list(getattr(request, "revision_digests", ())),
                    "prepared_body_revisions": [
                        digest for digest, _ in getattr(request, "skill_bodies", ())
                    ],
                    "delivery_or_material_use_verified": False,
                },
            )
        except Exception:
            # Missing learning evidence must not replay or block the user's task.
            self.capture_gap = True
            return None

    def record_tool(self, request, call_id, capability, result):
        if request.session != self.session:
            raise ContractError("learning tool source session mismatch")
        facts = {
            "operation_id": request.operation_id,
            "capability": capability,
            "learning_context": self.context_id,
            "disposition": result.get("disposition"),
            "unresolved_effects": result.get("unresolved_effects"),
            "ok": result.get("result", {}).get("ok")
            if type(result.get("result")) is dict
            else None,
        }
        if facts["ok"] is False or facts["disposition"] in ("unknown", "rejected"):
            facts["failure_signature"] = digest_json(
                [capability, result.get("reason"), facts["disposition"], facts["ok"]]
            )
        if (capability == "workspace.skills_read" and facts["disposition"] == "observed"
                and type(result.get("result")) is dict):
            value = result.get("result", {})
            # These are the harness read's coordinates, never copied skill text
            # or proof that the backend received or followed those instructions.
            facts["skill_read"] = {key: value.get(key) for key in (
                "revision_digest", "path", "offset", "next_offset", "offset_unit", "total_chars")}
            facts["delivery_or_material_use_verified"] = False
        try:
            return self.experiments.record_experience(
                source_ref="tool:"
                + digest_json([request.session.to_dict(), request.operation_id, call_id]),
                kind="tool-result",
                source_digest=digest_json(result),
                receipt=facts,
            )
        except Exception:
            self.capture_gap = True
            return None

    def record_result(self, request, turn):
        """Capture durable outcome facts, without treating completion as benefit."""
        if request.session != self.session:
            raise ContractError("learning task source session mismatch")
        try:
            value = turn.result or {}
            facts = {
                "operation_id": request.operation_id,
                "learning_context": self.context_id,
                "disposition": value.get("disposition"),
                "accepted": value.get("accepted") is True,
                "unresolved_effects": value.get("unresolved_effects") is True,
                "delivery_or_material_use_verified": False,
            }
            if not facts["accepted"]:
                facts["failure_signature"] = digest_json(
                    [facts["disposition"], value.get("reason")])
            return self.experiments.record_experience(
                source_ref="task:" + digest_json([request.session.to_dict(), request.operation_id]),
                kind="task-result", source_digest=digest_json(value), receipt=facts)
        except Exception:
            self.capture_gap = True
            return None

    def _proposal(self, payload, *, catalog=None, allow_assets=False):
        required = {"skill_name", "body", "description", "source_ref", "cause", "hypothesis"}
        optional = {"required_tools", "triggers"} | ({"assets"} if allow_assets else set())
        if (
            type(payload) is not dict
            or not required <= payload.keys()
            or payload.keys() - required - optional
        ):
            raise ContractError("learning proposal fields invalid")
        self._require_source(payload["source_ref"])
        old = (next((r for r in catalog if r.skill_name == payload["skill_name"]), None)
               if catalog is not None else self.experiments.repository.get_active(payload["skill_name"]))
        if old is None:
            candidate = SkillRevision(
                MARKDOWN_SKILL_SCHEMA,
                payload["skill_name"],
                "0.1.0",
                payload["description"],
                "how-procedure",
                ("procedure",),
                (self.experiments.owner,),
                tuple(payload.get("required_tools", ())),
                tuple(sorted(payload.get("triggers", ("when useful",)))),
                ("bounded instruction trial",),
                ("use when relevant",),
                ("unrelated tasks",),
                (),
                None,
                payload["body"],
            )
        else:
            major, minor, patch = map(int, old.version.split("."))
            candidate = replace(
                old,
                schema_version=MARKDOWN_SKILL_SCHEMA,
                version=f"{major}.{minor}.{patch + 1}",
                predecessor_digest=old.revision_digest,
                body=payload["body"],
                description=payload["description"],
                required_tools=tuple(payload.get("required_tools", old.required_tools)),
                triggers=tuple(sorted(payload.get("triggers", old.triggers))),
                body_digest=None,
                revision_digest=None,
            )
        asset_values = None
        if "assets" in payload:
            from ahc.domain.skill_assets import SkillAsset
            from ahc.domain.skill_values import digest_bytes
            values = payload["assets"]
            if (type(values) is not list or len(values) > 8
                    or any(type(item) is not dict or set(item) != {"path", "text"}
                           or type(item["text"]) is not str for item in values)):
                raise ContractError("trial resource proposal invalid")
            asset_values = {item["path"]: item["text"].encode("utf-8") for item in values}
            if len(asset_values) != len(values) or sum(map(len, asset_values.values())) > 8192:
                raise ContractError("trial resource proposal exceeds bound")
            assets = tuple(SkillAsset(path, digest_bytes(raw), len(raw))
                           for path, raw in sorted(asset_values.items()))
            candidate = replace(candidate, assets=assets, revision_digest=None)
        trial = self.experiments.propose(
            candidate=candidate,
            source_ref=payload["source_ref"],
            cause=payload["cause"],
            hypothesis=payload["hypothesis"],
            baseline=old, scoped=catalog is not None, allow_assets=allow_assets, asset_values=asset_values,
        )
        return self.experiments.get(trial)

    def _require_source(self, ref):
        with self.experiments.repository._read() as db:
            row = db.execute(
                "SELECT 1 FROM learning_experiences WHERE ref=? AND owner=? "
                "AND json_extract(receipt_json,'$.receipt.learning_context')=?",
                (ref, self.experiments.owner, self.context_id),
            ).fetchone()
        if row is None:
            raise ContractError("learning source belongs to another conversation")

    def _trials(self, payload):
        if set(payload) - {"before", "limit"}:
            raise ContractError("learning trial query fields invalid")
        before, limit = payload.get("before"), payload.get("limit", 16)
        if (
            (before is not None and (type(before) is not int or not 1 <= before < 2**63))
            or type(limit) is not int or not 1 <= limit <= 16
        ):
            raise ContractError("learning trial query bounds invalid")
        with self.experiments.repository._read() as db:
            rows = db.execute(
                "SELECT t.rowid AS cursor,t.trial_id,t.skill_name,t.candidate,t.source_ref,"
                "t.state,t.observation_ref FROM learning_trials t "
                "JOIN learning_experiences e ON e.ref=t.source_ref "
                "WHERE t.owner=? AND e.owner=? "
                "AND json_extract(e.receipt_json,'$.receipt.learning_context')=? "
                "AND (? IS NULL OR t.rowid<?) ORDER BY t.rowid DESC LIMIT ?",
                (self.experiments.owner, self.experiments.owner, self.context_id,
                 before, before, limit + 1),
            ).fetchall()
        entries = [{key: row[key] for key in (
            "trial_id", "skill_name", "candidate", "source_ref", "state", "observation_ref")}
            for row in rows[:limit]]
        return {"trials": entries,
                "next_before": rows[limit - 1]["cursor"] if len(rows) > limit else None}

    def invoke(self, workspace, arguments):
        try:
            if (
                workspace.closed or type(arguments) is not dict
                or set(arguments) != {"expected", "action", "payload"}
                or type(arguments["payload"]) is not dict
                or WorkspaceIdentity.from_dict(arguments["expected"]) != workspace.identity
                or workspace.identity.session != self.session
            ):
                raise ContractError("learning workspace identity mismatch")
            action, payload = arguments["action"], arguments["payload"]
            if (action in ("promote", "rollback") and self.controller is not None
                    and getattr(self.controller, "closed_loop", None) is not None):
                raise ContractError("configured scoped trials require host comparison and epoch guards")
            if action == "status" and not payload:
                with self.experiments.repository._read() as db:
                    control = db.execute(
                        "SELECT enabled,daily_limit FROM learning_control"
                    ).fetchone()
                    rows = db.execute(
                        "SELECT ref,kind,receipt_json FROM learning_experiences "
                        "WHERE owner=? AND json_extract(receipt_json,"
                        "'$.receipt.learning_context')=? "
                        "ORDER BY sequence DESC LIMIT 16",
                        (self.experiments.owner, self.context_id),
                    ).fetchall()
                experiences = []
                for row in rows:
                    item = {"ref": row[0], "kind": row[1],
                            "facts": json.loads(row[2])["receipt"]}
                    if len(json.dumps(experiences + [item]).encode("utf-8")) > 12000:
                        break
                    experiences.append(item)
                result = {
                    "automatic_enabled": bool(control[0]),
                    "trial_activation_enabled": bool(control[0]),
                    "autonomous_reflection_enabled": self.controller is not None,
                    "reflection": self.controller.last if self.controller is not None else None,
                    "scoped_learning": (self.controller.closed_loop.last
                                        if getattr(self.controller, "closed_loop", None) is not None else None),
                    "handoff": (self.handoff.summary(self.session, reflection_enabled=self.controller is not None)
                                if self.handoff is not None else None),
                    "daily_limit": control[1],
                    "capture_gap": self.capture_gap,
                    "isolated_evaluation_available": self.evaluations is not None,
                    "benefit": "not-assessed",
                    "experiences": experiences,
                    "experiences_truncated": len(experiences) < len(rows),
                    **self._trials({}),
                }
            elif action == "trials":
                result = self._trials(payload)
            elif action == "propose":
                result = self._proposal(payload)
            elif action == "evaluate" and set(payload) == {"run_id", "trial_id", "code"}:
                if self.evaluations is None:
                    raise ContractError("isolated evaluation unavailable")
                self._require_source(self.experiments.get(payload["trial_id"])["source_ref"])
                result = self.evaluations.run(**payload)
            elif action == "evaluation_get" and set(payload) == {"run_id"}:
                if self.evaluations is None:
                    raise ContractError("isolated evaluation unavailable")
                result = self.evaluations.get(payload["run_id"])
                self._require_source(self.experiments.get(result["request"]["trial_id"])["source_ref"])
            elif action in ("get", "promote", "reject", "rollback") and set(payload) == {
                "trial_id"
            }:
                self._require_source(self.experiments.get(payload["trial_id"])["source_ref"])
                result = getattr(self.experiments, action)(payload["trial_id"])
            elif action == "observe" and set(payload) == {"trial_id", "source_ref", "judgment"}:
                self._require_source(self.experiments.get(payload["trial_id"])["source_ref"])
                self._require_source(payload["source_ref"])
                result = self.experiments.observe(**payload)
            else:
                raise ContractError("learning action or payload invalid")
            pending = action in ("evaluate", "evaluation_get") and result["state"] == "pending"
            return {"disposition": "unknown" if pending else "observed", "result": result,
                    "unresolved_effects": pending}
        except (ContractError, ValueError, TypeError, KeyError):
            return {
                "disposition": "rejected",
                "reason": "learning-contract-rejected",
                "unresolved_effects": False,
            }
        except Exception:
            return {
                "disposition": "unknown",
                "reason": "learning-operation-unverified",
                "unresolved_effects": True,
            }


__all__ = ["WorkspaceLearningTools"]
