import pytest

from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.adapters.wsl_unit_runner import WslCreationIndeterminate


@pytest.mark.parametrize("phase", ["helper", "open", "send", "receive", "decode"])
def test_stream_create_reports_phase_and_preserves_unknown(monkeypatch, phase):
    runner = WslStreamRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    calls = []
    class Transport:
        def send(self, *a, **kw):
            calls.append("send")
            if phase == "send":
                raise OSError("private")
        def receive(self, **kw):
            if phase == "receive":
                raise OSError("private")
            return {"invalid": True}
    def source():
        if phase == "helper":
            raise OSError("private")
        return "controlled"
    def open_transport(source):
        if phase == "open":
            raise OSError("private")
        return Transport()
    monkeypatch.setattr(runner, "_controller_source", source)
    monkeypatch.setattr(runner, "_open_transport", open_transport)
    with pytest.raises(WslCreationIndeterminate) as raised:
        runner.create(["/usr/bin/true"], lifetime_seconds=10)
    assert raised.value.control_phase == phase
    assert "private" not in str(raised.value)
    assert len(calls) <= 1
