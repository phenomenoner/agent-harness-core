"""Pure offline fake-turn application use case."""

from __future__ import annotations

from hashlib import sha256

from ahc.domain import (
    FAKE_TURN_ENGINE,
    FAKE_TURN_OUTPUT_PREFIX,
    FAKE_TURN_RESULT_SCHEMA_VERSION,
    FAKE_TURN_STATUS,
    ContractError,
    FakeTurnRequest,
    FakeTurnResult,
)


def execute_fake_turn(request: FakeTurnRequest) -> FakeTurnResult:
    """Execute one validated request without any external or ambient effect."""

    if type(request) is not FakeTurnRequest:
        raise ContractError("request must be a FakeTurnRequest")
    digest = sha256(request.input_text.encode("utf-8")).hexdigest()
    return FakeTurnResult(
        FAKE_TURN_RESULT_SCHEMA_VERSION,
        request.session,
        request.turn_id,
        FAKE_TURN_STATUS,
        FAKE_TURN_ENGINE,
        f"{FAKE_TURN_OUTPUT_PREFIX}{request.input_text}",
        digest,
    )


__all__ = ["execute_fake_turn"]
