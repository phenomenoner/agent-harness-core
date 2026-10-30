import pytest

from ahc.domain.native_service_owner import CONTROLLER_SUBGROUP, service_path, validate_owner

ROOT = "/user.slice/user-1000.slice/user@1000.service/app.slice/ahc.service"
INVOCATION = "a" * 32


def properties(subgroup=None):
    result = {"ControlGroup": ROOT, "InvocationID": INVOCATION,
              "Delegate": "yes", "KillMode": "control-group", "SendSIGKILL": "yes"}
    if subgroup is not None:
        result["DelegateSubgroup"] = subgroup
    return result


@pytest.mark.parametrize('declared', [None, ''])
def test_legacy_service_root_accepts_absent_or_empty_subgroup_property(declared):
    assert str(validate_owner(ROOT, INVOCATION, properties(declared))) == ROOT


def test_declared_controller_and_prestart_resolve_same_service_custody_root():
    fields = properties(CONTROLLER_SUBGROUP)
    assert str(validate_owner(ROOT + '/' + CONTROLLER_SUBGROUP, INVOCATION, fields)) == ROOT
    assert str(validate_owner(ROOT + '/.control', INVOCATION, fields, setup=True)) == ROOT


@pytest.mark.parametrize('relative,declared,setup', [
    (ROOT, CONTROLLER_SUBGROUP, False),
    (ROOT + '/' + CONTROLLER_SUBGROUP, None, False),
    (ROOT + '/.control', CONTROLLER_SUBGROUP, False),
    (ROOT + '/other', CONTROLLER_SUBGROUP, False),
    (ROOT + '/' + CONTROLLER_SUBGROUP + '/nested', CONTROLLER_SUBGROUP, False),
    (ROOT, CONTROLLER_SUBGROUP, True),
    (ROOT + '/' + CONTROLLER_SUBGROUP, CONTROLLER_SUBGROUP, True),
    (ROOT + '/.control', '', True),
])
def test_unconfigured_or_wrong_execution_subgroup_never_receives_service_authority(
        relative, declared, setup):
    with pytest.raises(ValueError):
        validate_owner(relative, INVOCATION, properties(declared), setup=setup)


@pytest.mark.parametrize('field,value', [
    ('ControlGroup', ROOT + 'other'), ('InvocationID', 'b' * 32),
    ('Delegate', 'no'), ('KillMode', 'process'), ('SendSIGKILL', 'no'),
])
def test_owner_property_drift_refuses_both_layouts(field, value):
    for subgroup in ('', CONTROLLER_SUBGROUP):
        fields = properties(subgroup)
        fields[field] = value
        with pytest.raises(ValueError):
            validate_owner(ROOT + ('/' + subgroup if subgroup else ''), INVOCATION, fields)


@pytest.mark.parametrize('path', [
    'relative.service', '//root/x.service', '/root/../x.service',
    '/root//x.service', '/root/-x.service', '/root/x.service/',
])
def test_noncanonical_service_paths_refuse(path):
    with pytest.raises(ValueError):
        service_path(path)
