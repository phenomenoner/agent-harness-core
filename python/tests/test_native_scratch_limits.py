from dataclasses import FrozenInstanceError

import pytest

from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize('value', [None, True, 'max', 0, -1, 1048577, 1024**3 + 65536])
@pytest.mark.parametrize('field', ['workspace_bytes', 'temporary_bytes'])
def test_invalid_scratch_capacities_are_not_silently_unlimited_or_rounded(field, value):
    values = dict(workspace_bytes=1024**2, temporary_bytes=1024**2)
    values[field] = value
    with pytest.raises(ContractError):
        NativeScratchLimits(**values)


def test_admitted_capacities_cannot_be_mutated_in_place():
    limits = NativeScratchLimits(1024**2, 2 * 1024**2)
    with pytest.raises(FrozenInstanceError):
        limits.workspace_bytes = 0
