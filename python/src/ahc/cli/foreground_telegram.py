"""Explicit single-bot polling composition for a dedicated consumer."""

import logging
import os
import re
import time
from contextlib import nullcontext
from math import ceil

from ahc.adapters.telegram_poll import TelegramPoll, TelegramPollingUnavailable
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.telegram_inbox import TelegramInbox


def parse_telegram(raw, session):
    if "telegram_polling" not in raw:
        return {}
    value = raw["telegram_polling"]
    if type(value) is not dict or set(value) - {"include_files", "transport_recovery"} != {
            "enabled", "account_id", "token_env", "exclusive_consumer", "interval_seconds"}:
        raise ContractError("Telegram polling configuration fields invalid")
    if (type(value.get("include_files", False)) is not bool
            or type(value["enabled"]) is not bool or type(value["exclusive_consumer"]) is not bool
            or (value["enabled"] and not value["exclusive_consumer"])
            or type(value["interval_seconds"]) is not int
            or not 1 <= value["interval_seconds"] <= 300
            or type(value["token_env"]) is not str
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value["token_env"])):
        raise ContractError("Telegram polling configuration invalid")
    if "transport_recovery" in value:
        recovery = value["transport_recovery"]
        if (type(recovery) is not dict or set(recovery) != {"interval_seconds", "probe_limit"}
                or type(recovery["interval_seconds"]) is not int
                or not 30 <= recovery["interval_seconds"] <= 86400
                or type(recovery["probe_limit"]) is not int
                or not 1 <= recovery["probe_limit"] <= 10000):
            raise ContractError("Telegram transport recovery configuration invalid")
    TelegramPoll(value["account_id"], "validation")
    routes = [ChannelRoute.from_dict(row["route"]) for row in raw.get("channels", [])
              if row["route"]["platform"] == "telegram"
              and row["route"]["account_id"] == value["account_id"]]
    if not routes or any(route.session != session for route in routes):
        raise ContractError("Telegram polling requires configured routes")
    return {"telegram_polling": dict(value)}


def compose_telegram(config, store, *, concurrent=False):
    document = config.to_dict()
    value = document.get("telegram_polling")
    if value is None or not value["enabled"]:
        return None
    token = os.environ.get(value["token_env"])
    if token is None:
        raise ContractError("Telegram polling credential unavailable")
    interactions = (getattr(store, "channel_controls", None) is not None
                    and document.get("channel_control", {}).get("native_ui", False))
    poller = TelegramPoll(value["account_id"], token, interactions=interactions)
    routes = [ChannelRoute.from_dict(row["route"]) for row in document["channels"]
              if row["route"]["platform"] == "telegram"
              and row["route"]["account_id"] == value["account_id"]]
    consumer = TelegramInbox(store, routes, include_files=value.get("include_files", False))
    next_poll = 0.0
    status = {"state": "ready", "consecutive_failures": 0, "failure_limit": 2,
              "recovery_attempts": 0, "recovery_limit": 2, "failure_category": "none",
              "automatic_recovery": True, "transport_probe_attempts": 0, "held_updates": 0}
    # Legacy probe_limit stays parseable; request spacing, never process lifetime,
    # bounds recovery. Every configured entrance gets this default safety net.
    recovery = value.get("transport_recovery", {"interval_seconds": 60})
    status["recovery_interval_seconds"] = recovery["interval_seconds"]
    recoverable_incident = True
    needs_probe = False
    menu_attempted = False
    last_failure = None

    def failure(error):
        nonlocal last_failure
        status["failure_category"] = error.category
        status["http_status"] = error.http_status
        status["retry_after_seconds"] = error.retry_after or 0
        signature = (error.category, error.http_status, error.retry_after or 0)
        if signature != last_failure:
            logging.getLogger(__name__).warning(
                "Telegram receiver unavailable: category=%s http_status=%s "
                "retry_after_seconds=%s; automatic recovery enabled", *signature)
            last_failure = signature

    def receive(*, cancelled):
        nonlocal next_poll, menu_attempted, recoverable_incident, needs_probe, last_failure
        if cancelled() or time.monotonic() < next_poll:
            return
        if needs_probe:
            with (nullcontext() if concurrent else store.execution_lock()):
                if cancelled():
                    return
                next_poll = time.monotonic() + recovery["interval_seconds"]
                status["transport_probe_attempts"] = min(
                    status["transport_probe_attempts"] + 1, 2**31 - 1)
                try:
                    # getMe only: no updates, offset acknowledgement or durable admission.
                    poller.probe()
                except TelegramPollingUnavailable as error:
                    failure(error)
                    next_poll = time.monotonic() + max(
                        recovery["interval_seconds"], error.retry_after or 0)
                    return
                # Identity/transport recovery permits a fresh poll, not a readiness claim.
                needs_probe = False
                recoverable_incident = True
                status.update(state="backoff", consecutive_failures=0, recovery_attempts=0,
                              failure_category="none")
                next_poll = time.monotonic() + value["interval_seconds"]
            return
        next_poll = time.monotonic() + value["interval_seconds"]
        with (nullcontext() if concurrent else store.execution_lock()):
            if cancelled():
                return
            offset = consumer.offset()
            if status["state"] == "backoff":
                status["recovery_attempts"] += 1
            try:
                updates = poller.poll(offset)
            except TelegramPollingUnavailable as error:
                status["consecutive_failures"] += 1
                failure(error)
                recoverable_incident = recoverable_incident and error.retryable
                if status["consecutive_failures"] < status["failure_limit"]:
                    status["state"] = "degraded"
                    next_poll = time.monotonic() + max(
                        value["interval_seconds"], error.retry_after or 0)
                elif (recoverable_incident
                      and status["recovery_attempts"] < status["recovery_limit"]):
                    status["state"] = "backoff"
                    next_poll = time.monotonic() + max(
                        30 * (status["recovery_attempts"] + 1), error.retry_after or 0)
                else:
                    status["state"] = "backoff"
                    needs_probe = True
                    next_poll = time.monotonic() + max(
                        recovery["interval_seconds"], error.retry_after or 0)
                return
            if updates:
                status["state"] = "degraded"
                admitted = consumer.admit(updates, expected_offset=offset)
                held = admitted.get("held_count", 0)
                status["held_updates"] = min(status["held_updates"] + held, 2**31 - 1)
                if held:
                    logging.getLogger(__name__).warning(
                        "Telegram receiver retained %s invalid update(s); "
                        "automatic replay disabled", held)
                for callback in admitted.get("callbacks", []):
                    poller.answer_callback(callback)
            else:
                consumer.flush_albums()
            # Readiness follows durable admission, including raw custody of any
            # rejected input. A busy/corrupt writer cannot produce a ready claim.
            status.update(state="ready", consecutive_failures=0, recovery_attempts=0,
                          failure_category="none", http_status=None, retry_after_seconds=0)
            recoverable_incident = True
            last_failure = None
            if interactions and not menu_attempted:
                menu_attempted = True
                status["command_menu"] = ("configured" if poller.configure_commands(routes)
                                          else "unverified")

    def observe():
        return {**status, "next_retry_seconds": (
            max(0, ceil(next_poll - time.monotonic())) if status["state"] != "ready" else 0)}

    receive.status = observe
    return receive
