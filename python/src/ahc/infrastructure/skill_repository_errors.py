"""Typed failure contracts for the local skill repository."""

from ahc.domain.sessions import ContractError


class RepositoryError(ContractError):
    """A durable skill operation cannot be completed safely."""


class RelationshipProjectionError(RepositoryError):
    """SQLite relationship rows disagree with immutable revision metadata."""


class WriterBusyError(RepositoryError):
    """Another writer currently owns the nonblocking authority lock."""


class CompareAndSetError(RepositoryError):
    """The expected active/evaluation/lineage value is stale."""


class OperationConflictError(RepositoryError):
    """An operation key was reused with different inputs."""


class NotInitializedError(RepositoryError):
    """A read-only command was pointed at absent state."""


__all__ = [
    "CompareAndSetError",
    "NotInitializedError",
    "OperationConflictError",
    "RelationshipProjectionError",
    "RepositoryError",
    "WriterBusyError",
]
