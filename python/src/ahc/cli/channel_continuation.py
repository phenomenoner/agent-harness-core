"""Operator CLI for an offline atomic channel session continuation.

Run with ``python -m ahc.cli.channel_continuation --help``. This command neither
stops an owner nor starts a channel. Select the emitted configuration only after
its applied/already-applied receipt is reconciled.
"""

import argparse
import json
import os
from pathlib import Path

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.channel_continuation import (
    continue_discord_session,
    continue_telegram_session,
)
from ahc.infrastructure.runtime_repository import RuntimeRepository

from .foreground import read_foreground_config
from .foreground_config_native import parse_trusted_foreground_config


def successor_document(value, original, successor):
    if isinstance(value, dict):
        if value == original:
            return successor
        return {key: successor_document(item, original, successor) for key, item in value.items()}
    if isinstance(value, list):
        return [successor_document(item, original, successor) for item in value]
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--expected-config-digest", required=True)
    parser.add_argument("--expected-snapshot", type=Path, required=True)
    position = parser.add_mutually_exclusive_group(required=True)
    position.add_argument("--expected-offset", type=int)
    position.add_argument("--expected-discord-resume", type=Path,
                          help="JSON null or the exact session_id/resume_url/sequence object")
    parser.add_argument("--supervisor-lock", type=Path, required=True)
    parser.add_argument("--output-config", type=Path, required=True)
    parser.add_argument("--target-skill-root", type=Path)
    parser.add_argument("--expected-skill-revisions", type=Path,
                        help="JSON array of exact approved revision digests")
    parser.add_argument("--retire-native-custody", action="store_true",
                        help="Reconcile terminated native resources; never stop a service")
    args = parser.parse_args(argv)
    config = parse_trusted_foreground_config(read_foreground_config(args.config))
    if config.configuration_digest != args.expected_config_digest:
        raise ContractError("continuation configuration changed")
    expected = WorkingSetSnapshot.from_dict(json.loads(args.expected_snapshot.read_text()))
    if config.session != expected.session:
        raise ContractError("continuation configuration and expected owner differ")
    document = config.to_dict()
    channels = document.get("channels", [])
    platforms = {row["route"]["platform"] for row in channels}
    if platforms == {"telegram"}:
        platform = "telegram"
        account = document.get("telegram_polling", {}).get("account_id")
        if args.expected_offset is None or args.expected_discord_resume is not None:
            raise ContractError("Telegram continuation requires an expected offset")
        position = {"expected_offset": args.expected_offset}
        continuation = continue_telegram_session
    elif platforms == {"discord"}:
        platform = "discord"
        account = document.get("discord_gateway", {}).get("account_id")
        if args.expected_discord_resume is None or args.expected_offset is not None:
            raise ContractError("Discord continuation requires an expected resume checkpoint")
        position = {"expected_resume": json.loads(args.expected_discord_resume.read_bytes())}
        continuation = continue_discord_session
    else:
        raise ContractError("continuation supports one complete channel consumer")
    if account is None or any(row["route"]["account_id"] != account for row in channels):
        raise ContractError(f"continuation supports one complete {platform.title()} consumer")
    routes = tuple(ChannelRoute.from_dict(row["route"]) for row in channels)
    successor = expected.session.successor()
    prepared = successor_document(document, expected.session.to_dict(), successor.to_dict())
    skill_repository = revisions = None
    if (args.target_skill_root is None) != (args.expected_skill_revisions is None):
        raise ContractError("target skill root requires exact expected revisions")
    if args.target_skill_root is not None:
        from ahc.infrastructure.skill_repository import SkillRepository

        prepared["skill_root"] = str(args.target_skill_root)
        value = json.loads(args.expected_skill_revisions.read_text(encoding="utf-8"))
        if type(value) is not list:
            raise ContractError("expected skill revisions must be a JSON array")
        revisions = tuple(value)
        skill_repository = SkillRepository(args.target_skill_root)
    validated = parse_trusted_foreground_config(prepared)
    encoded = (json.dumps(prepared, ensure_ascii=False, indent=2) + "\n").encode()
    # A staged config is inert until the operator intentionally selects it.
    # Exclusive publication prevents a retry from replacing unrelated bytes.
    if args.output_config.exists():
        if args.output_config.is_symlink() or args.output_config.read_bytes() != encoded:
            raise ContractError("continuation output already contains different bytes")
    else:
        descriptor = os.open(args.output_config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    store = RuntimeRepository(config.runtime_root)
    if args.retire_native_custody:
        from ahc.infrastructure.native_custody_retirement import retire_native_custody

        retire_native_custody(store, expected, config.workspace_id)
    result = continuation(
        store, args.supervisor_lock, expected, routes,
        skill_repository=skill_repository, expected_skill_revisions=revisions,
        **position,
    )
    result["configuration_digest"] = validated.configuration_digest
    result["output_config"] = str(args.output_config)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
