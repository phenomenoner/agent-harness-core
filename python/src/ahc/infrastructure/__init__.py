"""SQLite and immutable-file infrastructure for the local ASI loop."""

from .skill_doctor import doctor
from .skill_evaluator import EvaluationCheck, EvaluationReport, evaluate_revision
from .skill_repository import (
    CompareAndSetError,
    NotInitializedError,
    OperationConflictError,
    RepositoryError,
    SkillRepository,
    WriterBusyError,
)
from .skill_root import SkillRootError, ensure_skill_layout, validate_skill_root
from .working_db import WORKING_DB_FILENAME, WORKING_DB_SCHEMA_VERSION, WORKING_LOCK_FILENAME
from .working_repository import (
    WorkingNotInitialized,
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
    WorkingSetNotFound,
    WorkingSetRepository,
    WorkingWriterBusy,
)
from .working_status import inspect_working_continuity

__all__ = [
    "CompareAndSetError",
    "EvaluationCheck",
    "EvaluationReport",
    "NotInitializedError",
    "OperationConflictError",
    "RepositoryError",
    "SkillRepository",
    "SkillRootError",
    "WriterBusyError",
    "WORKING_DB_FILENAME",
    "WORKING_DB_SCHEMA_VERSION",
    "WORKING_LOCK_FILENAME",
    "WorkingNotInitialized",
    "WorkingRepositoryError",
    "WorkingSetConflict",
    "WorkingSetCorrupt",
    "WorkingSetNotFound",
    "WorkingSetRepository",
    "WorkingWriterBusy",
    "doctor",
    "ensure_skill_layout",
    "evaluate_revision",
    "inspect_working_continuity",
    "validate_skill_root",
]
