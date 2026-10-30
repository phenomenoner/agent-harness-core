"""Once-reserved evaluation cases, using existing workspace and learning custody.

The owner injects the trusted isolated factory. No model tool can configure it.
Case success and supplied candidate text are observations, never proof of benefit.
"""

import base64
import json
from dataclasses import replace
from hashlib import sha256

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest, digest_json


class SkillEvaluationRuns:
    def __init__(self, experiments, factory, *, max_runs_per_trial):
        if (
            factory.session.agent_id != experiments.owner
            or factory.skill_root != experiments.repository.root
            or type(max_runs_per_trial) is not int
            or not 1 <= max_runs_per_trial <= 32
        ):
            raise ContractError("evaluation owner or finite run budget invalid")
        self.experiments, self.factory = experiments, factory
        self.repository, self.owner = experiments.repository, experiments.owner
        self.max_runs_per_trial = max_runs_per_trial
        self.repository._mutate(
            lambda db: db.execute(
                "CREATE TABLE IF NOT EXISTS learning_evaluation_runs ("
                "owner TEXT NOT NULL, run_id TEXT NOT NULL, trial_id TEXT NOT NULL, "
                "request_digest TEXT NOT NULL, request_json TEXT NOT NULL, "
                "state TEXT NOT NULL CHECK(state IN ('pending','observed')), result_json TEXT, "
                "PRIMARY KEY(owner,run_id))"
            )
        )

    def get(self, run_id):
        _identifier(run_id, "evaluation run")
        with self.repository._read() as db:
            row = db.execute(
                "SELECT * FROM learning_evaluation_runs WHERE owner=? AND run_id=?",
                (self.owner, run_id),
            ).fetchone()
        if row is None:
            raise ContractError("evaluation run not found for owner")
        return {
            "request": json.loads(row["request_json"]),
            "state": row["state"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "replay_allowed": False,
        }

    def _reserve(self, request):
        encoded = canonical_json(request).decode()
        request_digest = digest_json(request)

        def reserve(db):
            old = db.execute(
                "SELECT request_digest FROM learning_evaluation_runs WHERE owner=? AND run_id=?",
                (self.owner, request["run_id"]),
            ).fetchone()
            if old:
                if old[0] != request_digest:
                    raise ContractError("evaluation run ID already binds another request")
                return False
            if db.execute(
                "SELECT 1 FROM learning_evaluation_runs WHERE owner=? AND trial_id=? "
                "AND state='pending'",
                (self.owner, request["trial_id"]),
            ).fetchone():
                raise ContractError("evaluation trial has an unresolved run")
            count = db.execute(
                "SELECT count(*) FROM learning_evaluation_runs WHERE owner=? AND trial_id=?",
                (self.owner, request["trial_id"]),
            ).fetchone()[0]
            if count >= self.max_runs_per_trial:
                raise ContractError("evaluation trial run budget exhausted")
            db.execute(
                "INSERT INTO learning_evaluation_runs VALUES(?,?,?,?,?,'pending',NULL)",
                (self.owner, request["run_id"], request["trial_id"], request_digest, encoded),
            )
            return True

        return self.repository._mutate(reserve)

    def _publish(self, run_id):
        value = self.get(run_id)
        if value["state"] == "observed":
            result = value["result"]
            value["source_ref"] = self.experiments.record_experience(
                source_ref="evaluation:" + result["request_digest"],
                kind="task-result",
                source_digest=digest_json(result),
                receipt=result,
            )
        return value

    def run(self, *, run_id, trial_id, code):
        _identifier(run_id, "evaluation run")
        if type(code) is not str:
            raise ContractError("evaluation case must be text")
        try:
            code_bytes = code.encode("utf-8")
        except UnicodeError:
            raise ContractError("evaluation case encoding invalid") from None
        if not 1 <= len(code_bytes) <= 16384:
            raise ContractError("evaluation case exceeds workspace request bound")
        trial = self.experiments.get(trial_id)
        candidate = self.repository.get_revision(trial["candidate"])
        payload = canonical_json(candidate.to_dict())
        if len(payload) > 131072:
            raise ContractError("evaluation candidate context exceeds bound")
        binding = self.factory.binding_digest
        digest(binding, "evaluation factory binding")
        request = {
            "run_id": run_id,
            "trial_id": trial_id,
            "owner": self.owner,
            "candidate": trial["candidate"],
            "case_digest": sha256(code_bytes).hexdigest(),
            "factory_binding": binding,
            "session": self.factory.session.to_dict(),
        }
        if not self._reserve(request):
            return self._publish(run_id)  # Read/publish recorded facts; never dispatch again.
        try:
            return self._execute_reserved(request, payload, code, binding)
        except Exception as error:
            # Even a contract error after reservation may follow execution. Do
            # not let a tool adapter classify that as a harmless rejected call.
            raise RuntimeError(
                "evaluation outcome or publication is unverified; read custody"
            ) from error

    def _execute_reserved(self, request, payload, code, binding):
        run_id, trial_id = request["run_id"], request["trial_id"]
        workspace_id = "eval-" + digest_json([self.owner, run_id])
        with self.factory.open(workspace_id) as workspace:
            if (
                workspace.identity.session != self.factory.session
                or workspace.identity.workspace_id != workspace_id
                or workspace.identity.revision != 0
                or self.factory.binding_digest != binding
            ):
                raise ContractError("evaluation workspace or configuration binding changed")

            def execute(source):
                expected = replace(workspace.identity, revision=workspace.identity.revision + 1)
                result = workspace.execute(workspace.identity, source)
                if (
                    workspace.identity != expected
                    or result.get("identity") != expected.to_dict()
                    or type(result.get("ok")) is not bool
                ):
                    raise ContractError("evaluation response owner or status differs")
                return result

            cells = ["_ahc_candidate_bytes=bytearray()"]
            for offset in range(0, len(payload), 8192):
                chunk = base64.b64encode(payload[offset : offset + 8192]).decode("ascii")
                cells.append(
                    "_ahc_candidate_bytes.extend(__import__('base64').b64decode("
                    + repr(chunk)
                    + "))"
                )
            cells.append(
                "candidate=__import__('json').loads(_ahc_candidate_bytes);del _ahc_candidate_bytes"
            )
            for cell in cells:
                if execute(cell)["ok"] is not True:
                    raise ContractError("evaluation candidate preparation failed")
            result = execute(code)
            if (
                type(result.get("output")) is not str
                or len(result["output"]) > 4096
                or type(result.get("output_truncated")) is not bool
            ):
                raise ContractError("evaluation output differs from workspace contract")
            observation = {
                "request_digest": digest_json(request),
                "trial_id": trial_id,
                "learning_context": digest_json(
                    [
                        self.factory.session.agent_id,
                        self.factory.session.route_id,
                        self.factory.session.ref.virtual_session_id,
                    ]
                ),
                "candidate": request["candidate"],
                "workspace": workspace.identity.to_dict(),
                "ok": result["ok"],
                "output": result["output"][:1024],
                "output_truncated": result["output_truncated"] or len(result["output"]) > 1024,
                "result_digest": digest_json(result),
                "cleanup": "complete",
                "candidate_use": "provided-not-verified",
                "benefit": "unmeasured",
                "output_authority": "untrusted-case-output",
            }

        # Cleanup must succeed before an observed terminal result may commit.
        def finish(db):
            changed = db.execute(
                "UPDATE learning_evaluation_runs SET state='observed',result_json=? "
                "WHERE owner=? AND run_id=? AND request_digest=? AND state='pending'",
                (
                    canonical_json(observation).decode(),
                    self.owner,
                    run_id,
                    observation["request_digest"],
                ),
            ).rowcount
            if changed != 1:
                raise ContractError("evaluation reservation changed before result commit")

        self.repository._mutate(finish)
        return self._publish(run_id)
