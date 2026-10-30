"""Recovery changes session authority and activation flags, retaining explicit bindings."""

import copy
import json

import pytest
from test_channel_recovery_dedup import recover
from test_foreground_mcp_config import configured
from test_runtime_core import setup

from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize('enable_backend', [False, True])
def test_recovery_preserves_explicit_configuration_inventory(tmp_path, enable_backend):
    repo, saved, _ = setup(tmp_path)
    raw = configured()
    raw['runtime_root'] = str(repo.root)
    raw['backend']['enabled'] = True
    raw['skill_root'] = str(tmp_path / 'retained-skills')
    raw['batch'] = {'agent_id': 'agent', 'capabilities': ['runtime.artifact.manifest']}
    raw['channels'] = [
        {'route': ChannelRoute(platform, '123', '456', '789', saved.session).to_dict(),
         'enabled': True, 'token_env': 'RECOVERY_TOKEN', 'typing': True, 'progress': True}
        for platform in ('telegram', 'discord')]
    raw['telegram_polling'] = {'account_id': '123', 'token_env': 'RECOVERY_TOKEN',
                              'enabled': True, 'exclusive_consumer': True,
                              'interval_seconds': 1}
    raw['discord_gateway'] = {'account_id': '123', 'token_env': 'RECOVERY_TOKEN',
                             'enabled': True, 'intents': 37376, 'connection_limit': 2}
    parsed = parse_foreground_config_v2(raw)
    original = tmp_path / 'original.json'
    original.write_text(json.dumps(raw), encoding='utf-8')
    before = original.read_bytes()
    successor = recover(repo, saved, tmp_path)
    database = repo.db_path.read_bytes()
    output = tmp_path / 'prepared-config'
    result = prepare_recovered_config(repo, original, parsed.configuration_digest, output,
                                      checkpoint_digest=None, enable_backend=enable_backend)
    expected = copy.deepcopy(raw)
    expected['session'] = successor.to_dict()
    expected['backend']['enabled'] = enable_backend
    for row in expected['channels']:
        row['route']['session'] = successor.to_dict()
        row.update(enabled=False, typing=False, progress=False)
    for receiver in ('telegram_polling', 'discord_gateway'):
        expected[receiver]['enabled'] = False
    for profile in expected['mcp']['profiles']:
        profile['enabled'] = False
    actual = json.loads((output / 'foreground.json').read_text())
    assert actual == expected
    assert parse_foreground_config_v2(actual).session == successor
    assert result['started'] is False
    assert original.read_bytes() == before and repo.db_path.read_bytes() == database
    assert not (tmp_path / 'retained-skills').exists()
    rejected = tmp_path / 'rejected-config'
    with pytest.raises(ContractError, match='identity mismatch'):
        prepare_recovered_config(repo, original, 'f' * 64, rejected, checkpoint_digest=None)
    assert not rejected.exists()
