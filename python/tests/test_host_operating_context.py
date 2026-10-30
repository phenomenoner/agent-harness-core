"""Explicit mappings survive configuration without pretending to verify access."""

from types import SimpleNamespace

import pytest

from ahc.cli.foreground_host_paths import parse_host_context
from ahc.cli.runtime_environment import environment_snapshot
from ahc.domain.channel_commands import discord_commands
from ahc.domain.sessions import ContractError


def test_mapping_and_operating_context_reach_resident_snapshot():
    raw = {"host_paths": [{"host_root": "D:/", "backend_root": "/mnt/d"}],
           "operating_context": {"source_roots": ["D:/project"],
                                 "memory_coverage": "qualified historical rows"}}
    document = parse_host_context(raw)
    assert document == raw
    config = SimpleNamespace(workspace={}, to_dict=lambda: document)
    value = environment_snapshot(config, None)
    assert value["host_path_access"]["mappings"] == raw["host_paths"]
    assert value["host_path_access"]["mounts"] == "not_probed"
    assert value["host_path_access"]["permissions"] == "existing_user_selected_policy"
    assert value["operating_context"] == raw["operating_context"]
    assert parse_host_context({}) == {}


@pytest.mark.parametrize("path", ["/", "relative", "/mnt/../d", "/mnt//d", "/mnt/d/",
                                  "/mnt/\ud800", "/mnt/\n", "/mnt/\\d", "/mnt/\x7f"])
def test_mapping_rejects_ambiguous_backend_paths(path):
    with pytest.raises(ContractError):
        parse_host_context({"host_paths": [{"host_root": "D:/", "backend_root": path}]})


def test_mapping_rejects_duplicate_routes_and_bad_context():
    for second in ({"host_root": "D:/", "backend_root": "/other"},
                   {"host_root": "E:/", "backend_root": "/mnt/d"}):
        with pytest.raises(ContractError):
            parse_host_context({"host_paths": [{"host_root": "D:/", "backend_root": "/mnt/d"},
                                              second]})
    for context in ({"unknown": "x"}, {"source_roots": "x"},
                    {"memory_coverage": "\ud800"}, {"operator_docs": ["\n"]}):
        with pytest.raises(ContractError):
            parse_host_context({"operating_context": context})


def test_native_commands_allow_full_steering_and_schedule_text():
    commands = {command["name"]: command for command in discord_commands()}
    assert commands["steer"]["options"][0]["max_length"] == 4000
    assert commands["cron"]["options"][0]["max_length"] == 4000
    assert commands["model"]["options"][0]["max_length"] == 128
