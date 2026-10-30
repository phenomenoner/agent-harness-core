"""Pinned offline consumer rebind from two foreground configurations."""

import json
from pathlib import Path

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_recovery_binding import rebind_recovered_consumer

from .foreground import read_foreground_config
from .foreground_config_native import parse_trusted_foreground_config


def register(parser):
    parser.add_argument("--original-config", type=Path, required=True)
    parser.add_argument("--recovered-config", type=Path, required=True)
    parser.add_argument("--expected-original-digest", required=True)
    parser.add_argument("--expected-recovered-digest", required=True)
    parser.add_argument("--platform", choices=("telegram", "discord"), required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--expected-checkpoint", required=True)


def run(store, args):
    routes = []
    for path, digest in ((args.original_config, args.expected_original_digest),
                         (args.recovered_config, args.expected_recovered_digest)):
        config = parse_trusted_foreground_config(read_foreground_config(path))
        if (config.configuration_digest != digest
                or Path(config.runtime_root).resolve() != store.root.resolve()):
            raise ContractError("consumer recovery configuration mismatch")
        routes.append(tuple(ChannelRoute.from_dict(row["route"])
            for row in config.to_dict().get("channels", [])
            if row["route"]["platform"] == args.platform
            and row["route"]["account_id"] == args.account_id))
    if len(args.expected_checkpoint) > 2048:
        raise ContractError("consumer recovery checkpoint exceeds bound")
    try:
        expected = json.loads(args.expected_checkpoint)
    except (ValueError, RecursionError):
        raise ContractError("consumer recovery checkpoint invalid") from None
    return rebind_recovered_consumer(store, args.supervisor_lock, *routes, expected=expected)
