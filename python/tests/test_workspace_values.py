import pytest

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceCheckpoint, WorkspaceIdentity


def identity():
    return WorkspaceIdentity(VirtualSessionIdentity.initial("s", "agent", "route"), "work")


@pytest.mark.parametrize(
    "values",
    [
        '{"x":1,"x":2}',
        '{"x":{"a":1,"a":2}}',
        '{"x":NaN}',
        '{"x":1.5}',
        '{"__builtins__":{}}',
        '{"x":"\\ud800"}',
        '{"x":{"\\ud800":1}}',
        '{"x":' + "[" * 10 + "1" + "]" * 10 + "}",
    ],
)
def test_checkpoint_rejects_ambiguous_or_nonportable_values(values):
    with pytest.raises(ContractError):
        WorkspaceCheckpoint(identity(), values)


def test_checkpoint_is_canonical_and_binds_owner():
    first = WorkspaceCheckpoint(identity(), '{"z":[true,null], "x":42}')
    second = WorkspaceCheckpoint(identity(), '{"x":42,"z":[true,null]}')
    assert first.values_json == second.values_json
    assert first.digest == second.digest
    other = WorkspaceIdentity(identity().session, "other")
    assert WorkspaceCheckpoint(other, first.values_json).digest != first.digest


def test_canonical_encoding_bound_applies_after_escape_expansion():
    import json

    values = {"x": ["中" * 4000, "中" * 4000]}
    with pytest.raises(ContractError):
        WorkspaceCheckpoint(identity(), json.dumps(values, ensure_ascii=False))
