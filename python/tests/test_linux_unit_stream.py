import io
import json
from types import SimpleNamespace

import pytest

from ahc.adapters import linux_unit_helper, linux_unit_stream


@pytest.mark.parametrize("size", [98304, 1048575, 1048576])
def test_attached_input_relay_accepts_disclosure_and_rejects_oversize(size):
    class Sink(io.BytesIO):
        def close(self):
            self.captured = self.getvalue()
            super().close()

    raw = b"x" * size + b"\n"
    sink = Sink()
    linux_unit_stream._relay_input(io.BytesIO(raw), sink)
    assert sink.captured == (raw if size < linux_unit_stream.MAX_INPUT_FRAME else b"")
    assert sink.closed


def _large_command_event(size):
    prefix = b'{"method":"item/commandExecution/completed","params":{"output":"'
    suffix = b'"}}\n'
    assert size > len(prefix) + len(suffix)
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


@pytest.mark.parametrize(
    ("size", "frame_limit", "accepted"),
    [
        (2_079_192, linux_unit_stream.MAX_RELAY_FRAME, True),
        (linux_unit_stream.MAX_RELAY_FRAME + 1, linux_unit_stream.MAX_RELAY_FRAME, False),
        (2_079_192, 65536, False),
    ],
)
def test_attached_stream_limits_large_vendor_frames(monkeypatch, size, frame_limit, accepted):
    identity = {
        "distribution": "AHC",
        "boot_id": "boot",
        "unit": "unit",
        "invocation_id": "invocation",
        "state": "running",
        "cgroup_empty": False,
    }
    event = _large_command_event(size)
    final = b'{"id":1,"result":{"status":"completed"}}\n'

    class Child:
        def __init__(self):
            self.stdin = io.BytesIO()
            self.stdout = io.BytesIO(event + final)
            self.terminated = False

        def poll(self):
            return 0 if self.terminated else None

        def terminate(self):
            self.terminated = True

    child = Child()

    def control(request, **kwargs):
        if request["action"] == "create":
            return kwargs["launch"](["/usr/bin/systemd-run", "--", "/fixture"], "unit")
        return identity

    helper = SimpleNamespace(
        control=control,
        _observe=lambda _unit: identity,
        _object=linux_unit_helper._object,
        ControlError=linux_unit_helper.ControlError,
    )
    control_input = (
        json.dumps({"action": "create"}) + "\n"
        + json.dumps({"action": "attach", **{
            key: identity[key] for key in ("distribution", "boot_id", "unit", "invocation_id")
        }}) + "\n"
    ).encode()
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream, "MAX_RELAY_FRAME", frame_limit)
    monkeypatch.setattr(linux_unit_stream.sys, "stdin", io.TextIOWrapper(io.BytesIO(control_input)))
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    monkeypatch.setattr(linux_unit_stream.subprocess, "Popen", lambda *args, **kwargs: child)

    linux_unit_stream.serve(helper)

    wire = output.getvalue()
    if accepted:
        assert wire.splitlines(keepends=True)[-2:] == [event, final]
    else:
        normalized = wire.replace(b"\r\n", b"\n")
        assert normalized.endswith(b'{"attached": true}\n')
        assert event[:128] not in normalized
        assert final not in normalized


def test_stream_controller_embeds_large_relay_bound():
    from ahc.adapters.wsl_stream_runner import WslStreamRunner

    source = WslStreamRunner("C:/Windows/System32/wsl.exe", "AHC")._controller_source()

    assert "MAX_RELAY_FRAME = 4 * 1048576" in source
    assert "child.stdout.readline(MAX_RELAY_FRAME + 1)" in source


@pytest.mark.parametrize(
    "change",
    [
        {"unit": "other"},
        {"invocation_id": "wrong"},
        {"boot_id": "other"},
        {"distribution": "Other"},
    ],
)
def test_attachment_mismatch_never_enters_vendor_relay(monkeypatch, change):
    identity = {
        "distribution": "AHC",
        "boot_id": "boot",
        "unit": "unit",
        "invocation_id": "invocation",
        "state": "running",
        "cgroup_empty": False,
    }
    calls = []

    def control(request, **kwargs):
        calls.append(request["action"])
        return identity

    helper = SimpleNamespace(
        control=control,
        _object=linux_unit_helper._object,
        ControlError=linux_unit_helper.ControlError,
    )
    attach = {
        "action": "attach",
        **{k: identity[k] for k in ("distribution", "boot_id", "unit", "invocation_id")},
        **change,
    }
    raw = (json.dumps({"action": "create"}) + "\n" + json.dumps(attach) + "\n").encode()
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    linux_unit_stream.serve(helper)
    frames = [json.loads(line) for line in output.getvalue().splitlines()]
    assert frames == [
        {"ok": True, "result": identity},
        {"ok": False, "error": "unit stream unverified"},
    ]
    assert calls == ["create"]


@pytest.mark.parametrize("reject", [True, False])
def test_protection_runs_under_control_before_process_creation(monkeypatch, reject):
    events = []
    owned = False
    arguments = ["/usr/bin/systemd-run", "--", "/fixture"]

    def control(request, *, launch):
        nonlocal owned
        owned = True
        try:
            return launch(arguments, "unit")
        finally:
            owned = False

    def protect(actual):
        assert owned and actual == arguments
        events.append("protect")
        if reject:
            raise ValueError("policy rejected")
        return ["/usr/bin/systemd-run", "--property=PrivateNetwork=yes", "--", "/fixture"]

    def popen(actual, **kwargs):
        assert owned
        assert actual == [
            "/usr/bin/systemd-run",
            "--quiet",
            "--pipe",
            "--wait",
            "--property=PrivateNetwork=yes",
            "--",
            "/fixture",
        ]
        events.append("spawn")
        raise OSError("stop fixture after observing executable boundary")

    helper = SimpleNamespace(
        control=control,
        _object=linux_unit_helper._object,
        ControlError=linux_unit_helper.ControlError,
    )
    output = io.BytesIO()
    monkeypatch.setattr(
        linux_unit_stream.sys, "stdin", io.TextIOWrapper(io.BytesIO(b'{"action":"create"}\n'))
    )
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    monkeypatch.setattr(linux_unit_stream.subprocess, "Popen", popen)
    linux_unit_stream.serve(helper, protect_arguments=protect)
    assert events == (["protect"] if reject else ["protect", "spawn"])
    assert json.loads(output.getvalue()) == {"ok": False, "error": "unit stream unverified"}
