"""Offline whole-root recovery includes both SQLite authority and immutable bodies."""

import hashlib
import shutil

import pytest
from skill_test_support import goal, revision, session
from test_rebaseline_selection import _activate

from ahc.application.skill_service import SkillTurnService
from ahc.domain.skill_models import SelectionRequest
from ahc.infrastructure.skill_repository import SkillRepository
from ahc.infrastructure.skill_repository_errors import RepositoryError


def inventory(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file() and p.name != 'writer.lock'}


def test_offline_skill_root_restores_catalog_and_session_pin(repository, tmp_path):
    candidate = revision()
    _activate(repository, candidate, 'first')
    owner = session(1, 'offline-recovery')
    request = SelectionRequest(owner, 'first', ('procedure',), 'agent-test', (),
                               'analyze task', goal().digest)
    selected = SkillTurnService(repository).select_and_pin(
        request, episode_id='first', goal=goal())
    original_pin = repository.pin_revision_availability(owner)
    backup = tmp_path / 'backup'
    with repository.writer_lock():
        assert not any(repository.root.glob('learning.db-*'))
        before = inventory(repository.root)
        shutil.copytree(repository.root, backup, ignore=shutil.ignore_patterns('writer.lock'))
        assert inventory(backup) == before == inventory(repository.root)
    # Later source changes cannot leak into the restored availability or catalog.
    _activate(repository, revision(name='late-skill', trigger='late'), 'late')
    changed_source = inventory(repository.root)
    restored_root = tmp_path / 'restored'
    shutil.copytree(backup, restored_root)
    restored = SkillRepository(restored_root)
    assert restored.active_revisions() == (candidate,)
    assert restored.pin_revision_availability(owner) == original_pin
    reread = SkillTurnService(restored).select_and_pin(
        request, episode_id='first', goal=goal())
    assert reread['manifest'] == selected['manifest']
    assert inventory(repository.root) == changed_source
    assert inventory(backup) == before


@pytest.mark.parametrize('fault', ['missing-body', 'changed-body'])
def test_partial_or_corrupt_skill_copy_cannot_supply_active_revision(
        repository, tmp_path, fault):
    candidate = revision()
    _activate(repository, candidate, 'first')
    before = inventory(repository.root)
    target = tmp_path / 'damaged'
    with repository.writer_lock():
        shutil.copytree(repository.root, target, ignore=shutil.ignore_patterns('writer.lock'))
    body = next(target.rglob('SKILL.md'))
    if fault == 'missing-body':
        body.rename(body.with_suffix('.retained'))
    else:
        body.write_text('changed synthetic body', encoding='utf-8')
    with pytest.raises(RepositoryError, match='cannot be verified'):
        SkillRepository(target).active_revisions()
    assert inventory(repository.root) == before
