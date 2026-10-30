"""Evaluation, direct activation, and exact rollback CAS methods."""
# ruff: noqa: E501

from __future__ import annotations

import json
import sqlite3
from typing import Any

from ahc.domain.sessions import ContractError
from ahc.domain.skill_graph import hypothetical_active_revisions, validate_active_graph
from ahc.domain.skill_values import canonical_json, digest, digest_json

from .skill_materializer import read_materialized
from .skill_repository_errors import (
    CompareAndSetError,
    OperationConflictError,
    RelationshipProjectionError,
    RepositoryError,
)


class SkillActivationStoreMixin:
    """CAS operations mixed into the one SkillRepository writer authority."""

    def save_evaluation(self, report: Any) -> str:
        report_dict = report.to_dict() if hasattr(report, "to_dict") else report
        candidate = report_dict.get("candidate_digest")
        input_digest = report_dict.get("input_digest")
        graph_input_digest = report_dict.get("graph_input_digest")
        evaluation_digest = report_dict.get("evaluation_digest") or digest_json(report_dict)
        if (
            not isinstance(candidate, str)
            or not isinstance(input_digest, str)
            or not isinstance(graph_input_digest, str)
        ):
            raise RepositoryError("evaluation report lacks candidate, input, and graph digests")
        encoded = canonical_json({**report_dict, "evaluation_digest": evaluation_digest}).decode(
            "utf-8"
        )

        def operation(connection: sqlite3.Connection) -> str:
            connection.execute(
                "INSERT OR IGNORE INTO evaluations(evaluation_digest, candidate_digest, input_digest, verdict, report_json, created_seq) VALUES(?,?,?,?,?,?)",
                (
                    evaluation_digest,
                    candidate,
                    input_digest,
                    report_dict.get("verdict", "block"),
                    encoded,
                    self._seq(connection),
                ),
            )
            row = connection.execute(
                "SELECT report_json FROM evaluations WHERE evaluation_digest = ?",
                (evaluation_digest,),
            ).fetchone()
            if row[0] != encoded:
                raise OperationConflictError(
                    "evaluation digest is already bound to different inputs"
                )
            return evaluation_digest

        return self._mutate(operation)

    def activate(
        self,
        *,
        skill_name: str,
        candidate_digest: str,
        expected_active_digest: str | None,
        evaluation_digest: str,
        input_digest: str,
        operation_key: str,
        graph_input_digest: str | None = None,
        admission_guard=None,
    ) -> dict[str, object]:
        def operation(connection: sqlite3.Connection) -> dict[str, object]:
            old = connection.execute(
                "SELECT receipt_json, operation_kind, skill_name, candidate_digest, expected_digest, evaluation_digest FROM receipts WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if old is not None:
                receipt = json.loads(old[0])
                stored_graph = receipt.get("graph_input_digest")
                requested_graph = stored_graph if graph_input_digest is None else graph_input_digest
                identity = (
                    old[1],
                    old[2],
                    old[3],
                    old[4],
                    old[5],
                    receipt.get("input_digest"),
                    stored_graph,
                )
                expected_identity = (
                    "activate",
                    skill_name,
                    candidate_digest,
                    expected_active_digest,
                    evaluation_digest,
                    input_digest,
                    requested_graph,
                )
                if identity != expected_identity:
                    raise OperationConflictError(
                        "activation operation key was reused with different inputs"
                    )
                return receipt
            current = connection.execute(
                "SELECT revision_digest FROM active_revisions WHERE skill_name = ?", (skill_name,)
            ).fetchone()
            current_digest = None if current is None else current[0]
            if current_digest != expected_active_digest:
                raise CompareAndSetError("active revision compare-and-set failed")
            candidate = connection.execute(
                "SELECT skill_name, predecessor_digest FROM revisions WHERE revision_digest = ?",
                (candidate_digest,),
            ).fetchone()
            if candidate is None or candidate[0] != skill_name:
                raise CompareAndSetError("candidate revision is not materialized for this skill")
            evaluation = connection.execute(
                "SELECT verdict, candidate_digest, input_digest, report_json FROM evaluations WHERE evaluation_digest = ?",
                (evaluation_digest,),
            ).fetchone()
            if (
                evaluation is None
                or evaluation[0] != "accept"
                or evaluation[1] != candidate_digest
                or evaluation[2] != input_digest
            ):
                raise CompareAndSetError("evaluation input or verdict is not accepted")
            try:
                candidate_revision = read_materialized(self.root, skill_name, candidate_digest)
                report = json.loads(evaluation[3])
            except (ContractError, OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise CompareAndSetError(
                    "candidate or evaluation bytes cannot be verified"
                ) from exc
            embedded_digest = report.pop("evaluation_digest", None)
            report_graph = report.get("graph_input_digest")
            try:
                if not isinstance(report_graph, str):
                    raise ContractError("evaluation has no graph input digest")
                digest(report_graph, "graph_input_digest")
            except ContractError as exc:
                raise CompareAndSetError("evaluation graph input binding is invalid") from exc
            if (graph_input_digest is not None and graph_input_digest != report_graph) or (
                report.get("candidate_digest") != candidate_digest
                or report.get("input_digest") != input_digest
                or candidate_revision.revision_digest != candidate_digest
                or digest_json(candidate_revision.to_dict()) != input_digest
                or embedded_digest != evaluation_digest
                or digest_json(report) != evaluation_digest
            ):
                raise CompareAndSetError("candidate or evaluation bytes drifted before activation")
            if candidate[1] != expected_active_digest:
                raise CompareAndSetError(
                    "candidate predecessor is not the expected active revision"
                )
            try:
                active_revisions, materialized_revisions = self._graph_revisions(
                    connection,
                    (candidate_revision,),
                )
            except RelationshipProjectionError as exc:
                raise CompareAndSetError(
                    "relationship projection disagrees with immutable revision metadata"
                ) from exc
            except (ContractError, OSError, UnicodeError, KeyError) as exc:
                raise CompareAndSetError("current graph revision bytes cannot be verified") from exc
            graph = validate_active_graph(
                hypothetical_active_revisions(candidate_revision, active_revisions),
                materialized_revisions,
            )
            if not graph.valid:
                raise CompareAndSetError(
                    f"hypothetical active graph is invalid: {graph.issues[0].code}"
                )
            if graph.graph_digest != report_graph:
                raise CompareAndSetError("graph input digest drifted before activation")
            receipt = {
                "operation_key": operation_key,
                "operation_kind": "activate",
                "skill_name": skill_name,
                "candidate_digest": candidate_digest,
                "expected_active_digest": expected_active_digest,
                "evaluation_digest": evaluation_digest,
                "input_digest": input_digest,
                "graph_input_digest": report_graph,
            }
            if admission_guard is not None:
                # Protected learning control and quota checks share the exact
                # writer transaction with activation; model text cannot supply
                # this callback. Legacy operator activation stays unchanged.
                admission_guard(connection, candidate_revision)
            connection.execute(
                "INSERT INTO active_revisions(skill_name, revision_digest, predecessor_digest, updated_seq) VALUES(?,?,?,?) ON CONFLICT(skill_name) DO UPDATE SET revision_digest=excluded.revision_digest, predecessor_digest=excluded.predecessor_digest, updated_seq=excluded.updated_seq",
                (skill_name, candidate_digest, expected_active_digest, self._seq(connection)),
            )
            encoded = canonical_json(receipt).decode("utf-8")
            connection.execute(
                "INSERT INTO receipts(operation_key, operation_kind, skill_name, candidate_digest, expected_digest, predecessor_digest, evaluation_digest, receipt_json, created_seq) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    operation_key,
                    "activate",
                    skill_name,
                    candidate_digest,
                    expected_active_digest,
                    expected_active_digest,
                    evaluation_digest,
                    encoded,
                    self._seq(connection),
                ),
            )
            return receipt

        return self._mutate(operation)

    def rollback(
        self, *, skill_name: str, expected_active_digest: str, operation_key: str
    ) -> dict[str, object]:
        def operation(connection: sqlite3.Connection) -> dict[str, object]:
            old = connection.execute(
                "SELECT receipt_json, operation_kind, skill_name, expected_digest FROM receipts WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if old is not None:
                if (old[1], old[2], old[3]) != (
                    "rollback",
                    skill_name,
                    expected_active_digest,
                ):
                    raise OperationConflictError(
                        "rollback operation key was reused with different inputs"
                    )
                return json.loads(old[0])
            row = connection.execute(
                "SELECT revision_digest, predecessor_digest FROM active_revisions WHERE skill_name = ?",
                (skill_name,),
            ).fetchone()
            if row is None or row[0] != expected_active_digest or row[1] is None:
                raise CompareAndSetError("rollback compare-and-set has no matching predecessor")
            predecessor = connection.execute(
                "SELECT skill_name, predecessor_digest FROM revisions WHERE revision_digest = ?",
                (row[1],),
            ).fetchone()
            if predecessor is None or predecessor[0] != skill_name:
                raise CompareAndSetError("rollback predecessor is not materialized for this skill")
            try:
                predecessor_revision = read_materialized(self.root, skill_name, row[1])
                active_revisions, materialized_revisions = self._graph_revisions(
                    connection,
                    (predecessor_revision,),
                )
            except RelationshipProjectionError as exc:
                raise CompareAndSetError(
                    "relationship projection disagrees with immutable revision metadata"
                ) from exc
            except (ContractError, OSError, UnicodeError, KeyError) as exc:
                raise CompareAndSetError(
                    "rollback graph revision bytes cannot be verified"
                ) from exc
            graph = validate_active_graph(
                hypothetical_active_revisions(predecessor_revision, active_revisions),
                materialized_revisions,
            )
            if not graph.valid:
                raise CompareAndSetError(
                    f"rollback would create an invalid active graph: {graph.issues[0].code}"
                )
            receipt = {
                "operation_key": operation_key,
                "operation_kind": "rollback",
                "skill_name": skill_name,
                "from_digest": row[0],
                "to_digest": row[1],
                "graph_digest": graph.graph_digest,
            }
            connection.execute(
                "UPDATE active_revisions SET revision_digest = ?, predecessor_digest = ?, updated_seq = ? WHERE skill_name = ? AND revision_digest = ?",
                (row[1], predecessor[1], self._seq(connection), skill_name, expected_active_digest),
            )
            encoded = canonical_json(receipt).decode("utf-8")
            connection.execute(
                "INSERT INTO receipts(operation_key, operation_kind, skill_name, candidate_digest, expected_digest, predecessor_digest, evaluation_digest, receipt_json, created_seq) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    operation_key,
                    "rollback",
                    skill_name,
                    row[1],
                    expected_active_digest,
                    row[1],
                    None,
                    encoded,
                    self._seq(connection),
                ),
            )
            return receipt

        return self._mutate(operation)

    def deactivate(self, *, skill_name: str, expected_active_digest: str,
                   operation_key: str) -> dict[str, object]:
        """Exact, reversible catalog removal; retains revisions and all new data."""
        def operation(connection):
            old = connection.execute(
                "SELECT receipt_json,operation_kind,skill_name,expected_digest FROM receipts "
                "WHERE operation_key=?", (operation_key,),
            ).fetchone()
            if old:
                if tuple(old[1:]) != ("deactivate", skill_name, expected_active_digest):
                    raise OperationConflictError("deactivation operation key changed")
                return json.loads(old[0])
            row = connection.execute(
                "SELECT revision_digest FROM active_revisions WHERE skill_name=?", (skill_name,),
            ).fetchone()
            if row is None or row[0] != expected_active_digest:
                raise CompareAndSetError("deactivation active revision changed")
            active, materialized = self._graph_revisions(connection)
            graph = validate_active_graph(
                tuple(item for item in active if item.skill_name != skill_name), materialized)
            if not graph.valid:
                raise CompareAndSetError("deactivation would break an active dependency")
            receipt = {"operation_key": operation_key, "operation_kind": "deactivate",
                       "skill_name": skill_name, "from_digest": expected_active_digest,
                       "to_digest": None, "graph_digest": graph.graph_digest}
            connection.execute("DELETE FROM active_revisions WHERE skill_name=?", (skill_name,))
            connection.execute(
                "INSERT INTO receipts(operation_key,operation_kind,skill_name,candidate_digest,"
                "expected_digest,predecessor_digest,evaluation_digest,receipt_json,created_seq) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (operation_key, "deactivate", skill_name, None, expected_active_digest, None,
                 None, canonical_json(receipt).decode(), self._seq(connection)),
            )
            return receipt
        return self._mutate(operation)


__all__ = ["SkillActivationStoreMixin"]
