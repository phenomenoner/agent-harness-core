from dataclasses import replace

import pytest
from test_backend_custody import ready_to_create

from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable


class Transport:
    def __init__(self, reply=None):
        self.sent = []
        self.closed = False
        self.reply = {"attached": True} if reply is None else reply

    def send(self, request, **kwargs):
        self.sent.append(request)

    def receive(self, **kwargs):
        return self.reply

    def close(self):
        self.closed = True


def test_unrecorded_identity_cannot_send_attach(tmp_path):
    _, custody, lease, _, unit = ready_to_create(tmp_path)
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", unit.distribution)
    transport = Transport()
    runner._pending, runner._stream_identity = transport, unit
    try:
        with pytest.raises(ContractError, match="persisted"):
            runner.attach(unit, custody, lease)
        assert not transport.sent
        runner.close()
        assert transport.closed
    finally:
        lease.close()


@pytest.mark.parametrize("reply", [{"attached": True}, {"attached": 1}, {"attached": False}])
def test_recorded_attach_transfers_transport_only_on_exact_ack(tmp_path, reply):
    _, custody, lease, workspace, unit = ready_to_create(tmp_path)
    custody.record_created(lease, workspace, unit)
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", unit.distribution)
    transport = Transport(reply)
    runner._pending, runner._stream_identity = transport, unit
    try:
        with pytest.raises(ContractError):
            runner.attach(replace(unit, invocation_id="d" * 32), custody, lease)
        assert not transport.sent
        if reply["attached"] is True:
            assert runner.attach(unit, custody, lease) is transport
            runner.close()
            assert not transport.closed
            with pytest.raises(ContractError):
                runner.attach(unit, custody, lease)
            transport.close()
        else:
            with pytest.raises(BackendCustodyUnavailable):
                runner.attach(unit, custody, lease)
            runner.close()
            assert transport.closed
    finally:
        lease.close()
