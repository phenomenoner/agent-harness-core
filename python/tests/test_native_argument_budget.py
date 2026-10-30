"""Protected launch wrappers exceed the old 256-argument admission cap."""
import pytest
from ahc.infrastructure.process import linux_native as native


class Admitted(Exception): pass


@pytest.mark.parametrize('size',[278,1024])
def test_bounded_protected_wrapper_reaches_existing_custody(monkeypatch,size):
    def admitted(): raise Admitted()
    monkeypatch.setattr(native,'LinuxProcessGroup',admitted)
    with pytest.raises(Admitted):
        native.spawn_native(('/bin/true',)+('literal',)*(size-1),cwd=None,env={})


def test_oversized_command_still_rejects_before_custody(monkeypatch):
    monkeypatch.setattr(native,'LinuxProcessGroup',lambda:pytest.fail('custody was reached'))
    with pytest.raises(ValueError):
        native.spawn_native(('/bin/true',)+('literal',)*1024,cwd=None,env={})


def test_invalid_argument_still_rejects_before_custody(monkeypatch):
    monkeypatch.setattr(native,'LinuxProcessGroup',lambda:pytest.fail('custody was reached'))
    with pytest.raises(ValueError):
        native.spawn_native(('/bin/true','bad'+chr(0)),cwd=None,env={})
