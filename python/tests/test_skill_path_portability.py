"""A copied skill store remains diagnosable without rewriting historical rows."""
import pytest
from skill_test_support import revision
from ahc.infrastructure.skill_doctor import doctor


@pytest.mark.parametrize("legacy", [False, True])
def test_doctor_reads_portable_and_legacy_relative_paths(repository, legacy):
    item=revision()
    repository.register_revision(item)
    with repository._read() as c:
        recorded=c.execute('SELECT path FROM revisions').fetchone()[0]
    expected='/'.join(('revisions',item.skill_name,item.revision_digest))
    assert recorded==expected
    if legacy:
        repository._mutate(lambda c:c.execute('UPDATE revisions SET path=?',
                                             (expected.replace('/','\\'),)))
    before=repository.db_path.read_bytes()
    assert doctor(repository.root)['ok']
    assert repository.db_path.read_bytes()==before
    repository._mutate(lambda c:c.execute('UPDATE revisions SET path=?',('../outside',)))
    assert not doctor(repository.root)['ok']
