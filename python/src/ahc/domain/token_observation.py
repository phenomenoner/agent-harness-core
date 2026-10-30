"""Bounded informational token snapshots, never complete billing or quota authority."""

from dataclasses import dataclass

from .sessions import ContractError, _identifier


@dataclass(frozen=True, slots=True)
class TokenCounts:
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    reasoning_output_tokens: int
    total_tokens: int
    cache_write_input_tokens: int | None = None

    def __post_init__(self):
        values = (self.input_tokens, self.output_tokens, self.cached_input_tokens,
                  self.reasoning_output_tokens, self.total_tokens)
        if self.cache_write_input_tokens is not None:
            values += (self.cache_write_input_tokens,)
        if any(type(value) is not int or not 0 <= value <= 2**63 - 1 for value in values):
            raise ContractError("token observation counts invalid")

    def to_dict(self):
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class TokenObservation:
    backend_thread: str
    backend_turn: str
    last: TokenCounts
    total: TokenCounts
    model_context_window: int | None = None
    model: str | None = None
    compaction_count: int = 0
    compaction_before_tokens: int | None = None
    compaction_after_tokens: int | None = None
    compaction_threshold: int | None = None
    compaction_threshold_source: str | None = None

    def __post_init__(self):
        _identifier(self.backend_thread, "token observation thread")
        _identifier(self.backend_turn, "token observation turn")
        if type(self.last) is not TokenCounts or type(self.total) is not TokenCounts:
            raise ContractError("token observation snapshot invalid")
        if self.model is not None:
            _identifier(self.model, "token observation model")
        if (self.model_context_window is not None and (
                type(self.model_context_window) is not int or self.model_context_window <= 0)):
            raise ContractError("token observation context window invalid")
        if type(self.compaction_count) is not int or not 0 <= self.compaction_count <= 1000000:
            raise ContractError("token observation compaction count invalid")
        for value in (self.compaction_before_tokens, self.compaction_after_tokens):
            if value is not None and (type(value) is not int or value < 0):
                raise ContractError("token observation compaction tokens invalid")
        if self.compaction_threshold is not None and (
                type(self.compaction_threshold) is not int or self.compaction_threshold < 1):
            raise ContractError("token observation compaction threshold invalid")
        if self.compaction_threshold_source not in {None, "catalog", "backend-default"}:
            raise ContractError("token observation compaction threshold source invalid")

    def to_dict(self):
        return {"backend_thread": self.backend_thread, "backend_turn": self.backend_turn,
                "last": self.last.to_dict(), "total": self.total.to_dict(),
                "scope": "backend-notification-snapshot", "complete": False,
                **({"model_context_window": self.model_context_window}
                   if self.model_context_window is not None else {}),
                **({"model": self.model} if self.model is not None else {}),
                **({"compaction_threshold": self.compaction_threshold}
                   if self.compaction_threshold is not None else {}),
                **({"compaction_threshold_source": self.compaction_threshold_source}
                   if self.compaction_threshold_source is not None else {}),
                **({"context_compaction": {"count": self.compaction_count,
                    "before_tokens": self.compaction_before_tokens,
                    "after_tokens": self.compaction_after_tokens}}
                   if self.compaction_count else {})}


def durable_token_snapshot(value, backend_thread, backend_turn):
    """Validate exact backend identity before projecting counts into an AHC-owned result."""
    if value is None:
        return {}
    if (type(value) is not TokenObservation or value.backend_thread != backend_thread
            or value.backend_turn != backend_turn):
        raise ContractError("backend token observation binding invalid")
    result = value.to_dict()
    del result["backend_thread"], result["backend_turn"]
    return {"token_observation": result}
