"""Fake-turn request parsing for the standalone CLI."""

from __future__ import annotations

import argparse
import json

from ahc.domain import (
    FAKE_TURN_REQUEST_SCHEMA_VERSION,
    VIRTUAL_SESSION_REF_SCHEMA_VERSION,
    VIRTUAL_SESSION_SCHEMA_VERSION,
    ContractError,
    FakeTurnRequest,
    VirtualSessionIdentity,
    VirtualSessionRef,
)

_FAKE_TURN_BUILD_FIELDS = (
    "input_text",
    "input_text_positional",
    "virtual_session_id",
    "generation",
    "predecessor_generation",
    "predecessor_id",
    "predecessor_json",
    "agent_id",
    "route_id",
    "turn_id",
)


def build_fake_turn_request(
    args: argparse.Namespace, usage_error: type[ValueError]
) -> FakeTurnRequest:
    request_json = getattr(args, "request_json", None)
    if request_json is not None:
        if any(getattr(args, name, None) is not None for name in _FAKE_TURN_BUILD_FIELDS):
            raise usage_error("--request-json cannot be combined with request-building options")
        try:
            payload = json.loads(request_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ContractError("--request-json must contain one valid JSON object") from exc
        return FakeTurnRequest.from_dict(payload)

    input_text = args.input_text
    if input_text is not None and args.input_text_positional is not None:
        raise usage_error("input text may be supplied once")
    if input_text is None:
        input_text = args.input_text_positional
    if input_text is None:
        raise usage_error("fake-turn requires input text")

    virtual_session_id = args.virtual_session_id or "offline-session"
    generation = 1 if args.generation is None else args.generation
    agent_id = args.agent_id or "offline-agent"
    route_id = args.route_id or "offline-route"
    turn_id = args.turn_id or "turn-1"

    predecessor_json = args.predecessor_json
    if predecessor_json is not None and (
        args.predecessor_generation is not None or args.predecessor_id is not None
    ):
        raise usage_error("--predecessor-json cannot be combined with predecessor options")
    if predecessor_json is not None:
        try:
            predecessor_payload = json.loads(predecessor_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ContractError("--predecessor-json must contain one valid JSON object") from exc
        predecessor = VirtualSessionRef.from_dict(predecessor_payload)
    elif args.predecessor_generation is not None or args.predecessor_id is not None:
        predecessor = VirtualSessionRef(
            VIRTUAL_SESSION_REF_SCHEMA_VERSION,
            args.predecessor_id or virtual_session_id,
            args.predecessor_generation
            if args.predecessor_generation is not None
            else generation - 1,
        )
    else:
        predecessor = None

    ref = VirtualSessionRef(
        VIRTUAL_SESSION_REF_SCHEMA_VERSION,
        virtual_session_id,
        generation,
    )
    identity = VirtualSessionIdentity(
        VIRTUAL_SESSION_SCHEMA_VERSION,
        ref,
        agent_id,
        route_id,
        predecessor,
    )
    return FakeTurnRequest(
        FAKE_TURN_REQUEST_SCHEMA_VERSION,
        identity,
        turn_id,
        input_text,
    )
