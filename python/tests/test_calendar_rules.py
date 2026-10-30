import pytest

from ahc.domain.calendar_rules import DailyCalendar
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("hour,minute", [(True, 1), (24, 1), (1, 60), (1, False)])
def test_invalid_clock_fields(hour, minute):
    with pytest.raises(ContractError):
        DailyCalendar("UTC", hour, minute)


def test_missing_timezone_is_explicit():
    with pytest.raises(ContractError, match="timezone unavailable"):
        DailyCalendar("AHC/DefinitelyMissing", 12, 0)
