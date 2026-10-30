import json

from ahc.adapters.wsl_unit_runner import WslControlTransportUnavailable
from ahc.application.failure_trace import failure_trace


def test_control_category_survives_suppressed_cause_without_payload():
    try:
        try:
            raise WslControlTransportUnavailable('PRIVATE path and message',
                control_phase='receive', control_error_type='invalid-encoding')
        except WslControlTransportUnavailable:
            raise RuntimeError('PRIVATE wrapper') from None
    except RuntimeError as error:
        trace = failure_trace(error)
    assert trace['previous']['control_phase'] == 'receive'
    assert trace['previous']['control_error_type'] == 'invalid-encoding'
    assert 'PRIVATE' not in json.dumps(trace)


def test_unapproved_exception_attributes_are_not_projected():
    error = RuntimeError('PRIVATE message')
    error.control_phase = 'PRIVATE phase'
    error.control_error_type = 'PRIVATE category'
    error.payload = 'PRIVATE payload'
    trace = failure_trace(error)
    assert 'control_phase' not in trace and 'control_error_type' not in trace
    assert 'PRIVATE' not in json.dumps(trace)
