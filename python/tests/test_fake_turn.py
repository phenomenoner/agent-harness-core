from __future__ import annotations

import builtins
import hashlib
import os
import random
import socket
import subprocess
import time
from pathlib import Path

import pytest

from ahc.application.fake_turn import execute_fake_turn
from ahc.domain import (
    FAKE_TURN_ENGINE,
    FAKE_TURN_OUTPUT_PREFIX,
    FAKE_TURN_REQUEST_SCHEMA_VERSION,
    FAKE_TURN_RESULT_SCHEMA_VERSION,
    FAKE_TURN_STATUS,
    VIRTUAL_SESSION_REF_SCHEMA_VERSION,
    VIRTUAL_SESSION_SCHEMA_VERSION,
    ContractError,
    FakeTurnRequest,
    FakeTurnResult,
)


def _request_payload(input_text: str = "hello 世界") -> dict[str, object]:
    return {
        "schema_version": FAKE_TURN_REQUEST_SCHEMA_VERSION,
        "session": {
            "schema_version": VIRTUAL_SESSION_SCHEMA_VERSION,
            "ref": {
                "schema_version": VIRTUAL_SESSION_REF_SCHEMA_VERSION,
                "virtual_session_id": "session-main",
                "generation": 1,
            },
            "agent_id": "agent-main",
            "route_id": "route-main",
            "predecessor": None,
        },
        "turn_id": "turn-main",
        "input_text": input_text,
    }


def test_request_result_round_trips_and_application_binding_is_exact() -> None:
    request = FakeTurnRequest.from_dict(_request_payload())
    result = execute_fake_turn(request)
    expected_digest = hashlib.sha256("hello 世界".encode()).hexdigest()

    assert request.to_dict() == _request_payload()
    assert result.to_dict() == {
        "schema_version": FAKE_TURN_RESULT_SCHEMA_VERSION,
        "session": request.session.to_dict(),
        "turn_id": "turn-main",
        "status": FAKE_TURN_STATUS,
        "engine": FAKE_TURN_ENGINE,
        "output_text": f"{FAKE_TURN_OUTPUT_PREFIX}hello 世界",
        "input_sha256": expected_digest,
    }
    assert FakeTurnResult.from_dict(result.to_dict()) == result
    assert execute_fake_turn(request) == result
    assert result.session is request.session
    assert result.turn_id == request.turn_id


@pytest.mark.parametrize("input_text", ["", " ", "\t\n", "x" * 4097, "bad\ud800"])
def test_request_input_rejects_blank_oversized_and_non_scalar_text(input_text: str) -> None:
    payload = _request_payload(input_text)

    with pytest.raises(ContractError, match="input_text"):
        FakeTurnRequest.from_dict(payload)


def test_request_accepts_the_exact_unicode_scalar_bound() -> None:
    request = FakeTurnRequest.from_dict(_request_payload("x" * 4096))

    assert len(request.input_text) == 4096
    assert execute_fake_turn(request).output_text == f"{FAKE_TURN_OUTPUT_PREFIX}{'x' * 4096}"


def test_request_and_result_reject_unknown_or_missing_nested_keys() -> None:
    missing = _request_payload()
    del missing["session"]["route_id"]  # type: ignore[index]
    with pytest.raises(ContractError, match="session: .*missing keys"):
        FakeTurnRequest.from_dict(missing)

    unknown = _request_payload()
    unknown["unexpected"] = True
    with pytest.raises(ContractError, match="unknown keys"):
        FakeTurnRequest.from_dict(unknown)

    result = execute_fake_turn(FakeTurnRequest.from_dict(_request_payload()))
    result_payload = result.to_dict()
    result_payload["session"]["unexpected"] = True  # type: ignore[index]
    with pytest.raises(ContractError, match="session: .*unknown keys"):
        FakeTurnResult.from_dict(result_payload)


def test_result_rejects_a_digest_that_does_not_bind_its_output_input() -> None:
    result = execute_fake_turn(FakeTurnRequest.from_dict(_request_payload()))
    tampered = result.to_dict()
    tampered["input_sha256"] = "0" * 64

    with pytest.raises(ContractError, match="does not match output_text"):
        FakeTurnResult.from_dict(tampered)


def test_application_seam_has_no_filesystem_environment_process_or_socket_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = FakeTurnRequest.from_dict(_request_payload("offline-only"))
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))

    def blocked(*args, **kwargs):
        raise AssertionError("forbidden external effect")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(builtins, "open", blocked)
    monkeypatch.setattr(os, "getenv", blocked)
    monkeypatch.setattr(random, "random", blocked)
    monkeypatch.setattr(time, "time", blocked)
    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(subprocess, "Popen", blocked)
    monkeypatch.setattr(subprocess, "run", blocked)

    assert execute_fake_turn(request).output_text == "FAKE: offline-only"
    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert after == before
