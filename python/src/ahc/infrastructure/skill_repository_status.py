"""Read-only SQLite status projection for the skill repository."""

from __future__ import annotations


class SkillStatusStoreMixin:
    """Read-only status mixed into the one SkillRepository authority."""

    def status(self) -> dict[str, object]:
        with self._read() as connection:
            quick = connection.execute("PRAGMA quick_check").fetchone()[0]
            counts = {
                name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                for name in (
                    "revisions",
                    "active_revisions",
                    "manifests",
                    "episodes",
                    "events",
                    "beliefs",
                    "evaluations",
                    "receipts",
                    "judgment_records",
                )
            }
            return {
                "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                "quick_check": quick,
                "counts": counts,
            }


__all__ = ["SkillStatusStoreMixin"]
