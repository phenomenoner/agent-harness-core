"""Adding protected stores must not fail an unrelated fixed path-count quota."""
from test_foreground_workspace_protection import protection
from ahc.cli.foreground_workspace_protection import parse_native_protection


def test_deep_workspace_masks_every_project_config_ancestor(tmp_path):
    import hashlib, sys
    from pathlib import Path
    import pytest
    if sys.platform != 'linux':
        pytest.skip('Linux namespace argument construction')
    from ahc.infrastructure.process.linux_backend_boundary import LinuxBackendBoundary
    work=tmp_path.joinpath(*(['nested']*35));work.mkdir(parents=True)
    controls=tmp_path/'controls';controls.mkdir()
    state=tmp_path/'state';state.mkdir()
    for name in ('sessions','archived_sessions','state'):
        (state/name).mkdir()
    executable=Path('/usr/bin/true')
    boundary=LinuxBackendBoundary(executable=str(executable),
        sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),workspace=str(work),
        readonly_roots=['/usr'],protected_roots=[str(controls)],state_root=str(state),
        home_bytes=32*1024**2,temporary_bytes=32*1024**2)
    command=boundary.command(('/usr/bin/true',))
    assert all(str(parent/'.codex') in command and str(parent/'.agents') in command
               for parent in (work,*work.parents))


def test_more_than_sixteen_explicit_protected_roots_remain_registered():
    value=protection()
    value['protected_roots']=[f'/home/ahc/product/state-{i}' for i in range(24)]
    value['readonly_roots']=[f'/opt/product/reference-{i}' for i in range(24)]
    parsed=parse_native_protection(value,None)
    assert parsed['protected_roots']==value['protected_roots']
    assert parsed['readonly_roots']==value['readonly_roots']
