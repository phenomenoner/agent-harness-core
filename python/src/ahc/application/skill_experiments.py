"""Grounded, reversible instruction trials; structural admission is not benefit.

The control-plane owner supplies protection_check and records actual experience.
Neither surface is exposed as a model tool. A trusted unsandboxed workspace must
not assert protection merely because its dynamic tool schemas are restrictive.
"""

import json
from datetime import UTC, datetime

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import MARKDOWN_SKILL_SCHEMA
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import canonical_json, digest, digest_json
from ahc.infrastructure.skill_evaluator import evaluate_revision

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS learning_policy (id INTEGER PRIMARY KEY CHECK(id=1), "
    "policy_digest TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS learning_control (id INTEGER PRIMARY KEY CHECK(id=1), "
    "enabled INTEGER NOT NULL, daily_limit INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS learning_experiences ("
    "ref TEXT PRIMARY KEY, owner TEXT NOT NULL, source_id TEXT NOT NULL, kind TEXT NOT NULL, "
    "source_digest TEXT NOT NULL, receipt_json TEXT NOT NULL, sequence INTEGER UNIQUE NOT NULL, "
    "UNIQUE(owner,source_id))",
    "CREATE TABLE IF NOT EXISTS learning_trials ("
    "trial_id TEXT PRIMARY KEY, owner TEXT NOT NULL, skill_name TEXT NOT NULL, "
    "candidate TEXT NOT NULL, source_ref TEXT NOT NULL REFERENCES learning_experiences(ref), "
    "cause TEXT NOT NULL, cause_key TEXT NOT NULL, hypothesis TEXT NOT NULL, day TEXT NOT NULL, "
    "state TEXT NOT NULL, activation_sequence INTEGER, observation_ref TEXT, judgment TEXT)",
)


class SkillExperiments:
    def __init__(
        self,
        repository,
        *,
        owner,
        granted_tools,
        protected_names,
        protection_check,
        clock=lambda: datetime.now(UTC),
    ):
        if (
            type(owner) is not str
            or not owner
            or not callable(protection_check)
            or not callable(clock)
        ):
            raise ContractError("learning owner or protection check invalid")
        self.repository, self.owner = repository, owner
        self.granted_tools = frozenset(granted_tools)
        self.protected_names = frozenset(protected_names)
        self.protection_check, self.clock = protection_check, clock

        def initialize(db):
            for statement in _SCHEMA:
                db.execute(statement)
            db.execute("INSERT OR IGNORE INTO learning_control VALUES(1,0,3)")

        repository._mutate(initialize)

    def configure(self, *, enabled, daily_limit=3):
        """Operator-only kill switch; never a dynamic/model tool."""
        if type(enabled) is not bool or type(daily_limit) is not int or daily_limit < 1:
            raise ContractError("learning control invalid")
        self.repository._mutate(
            lambda db: db.execute(
                "UPDATE learning_control SET enabled=?,daily_limit=? WHERE id=1",
                (int(enabled), daily_limit),
            )
        )

    def configure_policy(self, *, policy_digest, enabled, daily_limit=3):
        """Apply an explicit configuration change once; reopen preserves a stop.

        The model cannot alter this policy or the durable kill switch. Proposal
        mode always disables promotion, including a previously enabled control.
        """
        digest(policy_digest, "learning policy digest")
        if type(enabled) is not bool or type(daily_limit) is not int or daily_limit < 1:
            raise ContractError("learning control invalid")

        def apply(db):
            old = db.execute("SELECT policy_digest FROM learning_policy WHERE id=1").fetchone()
            if not enabled or old is None or old[0] != policy_digest:
                db.execute("UPDATE learning_control SET enabled=?,daily_limit=? WHERE id=1",
                           (int(enabled), daily_limit))
                db.execute("INSERT INTO learning_policy VALUES(1,?) "
                           "ON CONFLICT(id) DO UPDATE SET policy_digest=excluded.policy_digest",
                           (policy_digest,))

        self.repository._mutate(apply)

    def record_experience(self, *, source_ref, kind, source_digest, receipt):
        """Harness-owned receipt, not a proposer-authored evaluation result.

        Receipt contains bounded observable facts/provenance, never hidden model
        reasoning. A user-input receipt does not assert it was a correction;
        the proposer makes that interpretation in its hypothesis.
        """
        digest(source_digest, "experience source digest")
        if (
            type(source_ref) is not str
            or not 1 <= len(source_ref) <= 256
            or kind not in ("user-input", "tool-result", "task-result")
            or type(receipt) is not dict
            or len(canonical_json(receipt)) > 16384
        ):
            raise ContractError("learning experience invalid")
        value = {
            "source_ref": source_ref,
            "owner": self.owner,
            "kind": kind,
            "source_digest": source_digest,
            "receipt": receipt,
        }
        ref = digest_json(value)
        encoded = canonical_json(value).decode()

        def record(db):
            old = db.execute(
                "SELECT receipt_json FROM learning_experiences WHERE owner=? AND source_id=?",
                (self.owner, source_ref),
            ).fetchone()
            if old:
                if old[0] != encoded:
                    raise ContractError("learning receipt changed")
                return ref
            sequence = db.execute(
                "SELECT coalesce(max(sequence),0)+1 FROM learning_experiences"
            ).fetchone()[0]
            db.execute(
                "INSERT INTO learning_experiences VALUES(?,?,?,?,?,?,?)",
                (ref, self.owner, source_ref, kind, source_digest, encoded, sequence),
            )
            return ref

        return self.repository._mutate(record)

    def _candidate(self, candidate, *, baseline=None, scoped=False, allow_assets=False):
        if (
            type(candidate) is not SkillRevision
            or candidate.schema_version != MARKDOWN_SKILL_SCHEMA
            or candidate.skill_name in self.protected_names
        ):
            raise ContractError("trial exceeds instruction or granted-tool boundary")
        old = baseline if scoped else self.repository.get_active(candidate.skill_name)
        permitted_tools = self.granted_tools if old is None else set(old.required_tools)
        if not set(candidate.required_tools) <= permitted_tools:
            raise ContractError('trial cannot expand the existing tool requirements')
        if old is None:
            if (candidate.assets and not allow_assets) or candidate.agent_allowlist != (self.owner,):
                raise ContractError("new trial must be owned instruction-only content")
        elif (
            (candidate.assets != old.assets and not allow_assets)
            or candidate.agent_allowlist != old.agent_allowlist
            or (old.agent_allowlist and self.owner not in old.agent_allowlist)
        ):
            raise ContractError("trial cannot change assets or agent ownership")
        return old

    def propose(self, *, candidate, source_ref, cause, hypothesis, baseline=None, scoped=False,
                allow_assets=False, asset_values=None):
        self._candidate(candidate, baseline=baseline, scoped=scoped, allow_assets=allow_assets)
        if (
            type(cause) is not str
            or not 1 <= len(cause) <= 256
            or type(hypothesis) is not str
            or not 1 <= len(hypothesis) <= 4096
        ):
            raise ContractError("trial needs a bounded cause and hypothesis")
        trial_id = digest_json(
            {
                "owner": self.owner,
                "candidate": candidate.revision_digest,
                "source": source_ref,
                "cause": cause,
                "hypothesis": hypothesis,
            }
        )
        today = self.clock().astimezone(UTC).date().isoformat()

        def admit(db):
            old = db.execute(
                "SELECT trial_id FROM learning_trials WHERE trial_id=?", (trial_id,)
            ).fetchone()
            if old:
                return trial_id
            source = db.execute(
                "SELECT owner,receipt_json FROM learning_experiences WHERE ref=?", (source_ref,)
            ).fetchone()
            if source is None or source[0] != self.owner:
                raise ContractError("trial requires a harness receipt owned by this agent")
            limit = db.execute("SELECT daily_limit FROM learning_control WHERE id=1").fetchone()[0]
            if (
                db.execute(
                    "SELECT count(*) FROM learning_trials WHERE owner=? AND day=?",
                    (self.owner, today),
                ).fetchone()[0]
                >= limit
            ):
                raise ContractError("daily proposal budget exhausted")
            if db.execute(
                "SELECT 1 FROM learning_trials WHERE skill_name=? "
                "AND state IN ('proposed','active')",
                (candidate.skill_name,),
            ).fetchone():
                raise ContractError("skill already has an active trial")
            facts = json.loads(source[1])["receipt"]
            cause_key = digest_json(facts.get("failure_signature", source_ref))
            if (
                db.execute(
                    "SELECT count(*) FROM learning_trials WHERE owner=? AND skill_name=? "
                    "AND cause_key=?",
                    (self.owner, candidate.skill_name, cause_key),
                ).fetchone()[0]
                >= 2
            ):
                raise ContractError("unchanged-cause trial budget exhausted")
            db.execute(
                "INSERT INTO learning_trials VALUES(?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
                (
                    trial_id,
                    self.owner,
                    candidate.skill_name,
                    candidate.revision_digest,
                    source_ref,
                    cause,
                    cause_key,
                    hypothesis,
                    today,
                    "proposed",
                ),
            )
            return trial_id

        # Reserve the durable slot before materialization, so rejected calls
        # cannot fill storage with arbitrary new immutable revision trees. A
        # crash between reservation and materialization resumes this exact ID.
        accepted = self.repository._mutate(admit)
        if asset_values is not None:
            if not allow_assets:
                raise ContractError("trial assets require owner authorization")
            self.repository.stage_assets(candidate.assets, asset_values)
        self.repository.register_revision(candidate)
        return accepted

    def get(self, trial_id):
        with self.repository._read() as db:
            row = db.execute(
                "SELECT * FROM learning_trials WHERE trial_id=? AND owner=?", (trial_id, self.owner)
            ).fetchone()
        if row is None:
            raise ContractError("learning trial not found for owner")
        return {
            **dict(row),
            "benefit": "unmeasured" if row["observation_ref"] is None else "model-assessment",
            "automatic_authority_expansion": False,
        }

    def promote(self, trial_id):
        trial = self.get(trial_id)
        with self.repository._read() as db:
            prior = db.execute(
                "SELECT receipt_json FROM receipts WHERE operation_key=?", ("trial:" + trial_id,)
            ).fetchone()
        if prior:
            return {
                "activation": json.loads(prior[0]),
                "benefit": "unmeasured",
                "mode": "reversible-trial",
            }
        candidate = self.repository.get_revision(trial["candidate"])
        self._candidate(candidate)
        active, materialized = self.repository.graph_revisions()
        report = evaluate_revision(
            candidate,
            active_revisions=active,
            materialized_revisions=materialized,
            evaluator_id="instruction-trial-admission-v1",
        )
        if report.structural_verdict != "accept":
            raise ContractError("trial failed structural admission")
        self.repository.save_evaluation(report)

        def guard(db, revision):
            row = db.execute(
                "SELECT state FROM learning_trials WHERE trial_id=?", (trial_id,)
            ).fetchone()
            if row is None or row[0] != "proposed":
                raise ContractError("trial state changed before activation")
            if not db.execute("SELECT enabled FROM learning_control WHERE id=1").fetchone()[0]:
                raise ContractError("automatic learning activation is disabled")
            if self.protection_check() is not True:
                raise ContractError("learning controls and evidence are not protected")
            sequence = db.execute(
                "SELECT coalesce(max(sequence),0) FROM learning_experiences"
            ).fetchone()[0]
            db.execute(
                "UPDATE learning_trials SET state='active',activation_sequence=? WHERE trial_id=?",
                (sequence, trial_id),
            )

        receipt = self.repository.activate(
            skill_name=candidate.skill_name,
            candidate_digest=candidate.revision_digest,
            expected_active_digest=candidate.predecessor_digest,
            evaluation_digest=report.evaluation_digest,
            input_digest=report.input_digest,
            operation_key="trial:" + trial_id,
            graph_input_digest=report.graph_input_digest,
            admission_guard=guard,
        )
        return {"activation": receipt, "benefit": "unmeasured", "mode": "reversible-trial"}

    def observe(self, trial_id, *, source_ref, judgment):
        if type(judgment) is not str or not 1 <= len(judgment) <= 4096:
            raise ContractError("trial observation judgment invalid")

        def update(db):
            trial = db.execute(
                "SELECT * FROM learning_trials WHERE trial_id=? AND owner=?", (trial_id, self.owner)
            ).fetchone()
            source = db.execute(
                "SELECT * FROM learning_experiences WHERE ref=? AND owner=?",
                (source_ref, self.owner),
            ).fetchone()
            if trial is None or source is None or trial["state"] != "active":
                raise ContractError("trial or observed source is unavailable")
            if source["sequence"] <= trial["activation_sequence"]:
                raise ContractError("trial observation must follow activation")
            db.execute(
                "UPDATE learning_trials SET state='observed',observation_ref=?,judgment=? "
                "WHERE trial_id=?",
                (source_ref, judgment, trial_id),
            )

        self.repository._mutate(update)
        return self.get(trial_id)

    def reject(self, trial_id):
        """Discard an unactivated experiment without deleting evidence or revisions."""

        def update(db):
            changed = db.execute(
                "UPDATE learning_trials SET state='rejected' "
                "WHERE trial_id=? AND owner=? AND state='proposed'",
                (trial_id, self.owner),
            ).rowcount
            if not changed:
                raise ContractError("only a proposed trial can be rejected")

        self.repository._mutate(update)

    def rollback(self, trial_id):
        trial = self.get(trial_id)
        if trial["state"] not in ("active", "observed", "rolled-back"):
            raise ContractError("only an activated trial can be rolled back")
        candidate = self.repository.get_revision(trial["candidate"])
        action = (
            self.repository.deactivate
            if candidate.predecessor_digest is None
            else self.repository.rollback
        )
        receipt = action(
            skill_name=trial["skill_name"],
            expected_active_digest=trial["candidate"],
            operation_key="trial-rollback:" + trial_id,
        )
        self.repository._mutate(
            lambda db: db.execute(
                "UPDATE learning_trials SET state='rolled-back' WHERE trial_id=?", (trial_id,)
            )
        )
        return {"rollback": receipt, "external_effects_undone": False}


__all__ = ["SkillExperiments"]
