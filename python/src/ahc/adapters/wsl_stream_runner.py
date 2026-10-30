"""Attached unit runner: validated identity first, durable custody before attach."""

import time
from dataclasses import asdict
from importlib.resources import files

from ahc.domain.backend_creation import BackendCreationRejected
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable, LinuxUnitIdentity
from .codex_transport import CodexTransportError

from .wsl_unit_runner import WslControlOperationUnavailable, WslUnitRunner


class WslStreamRunner(WslUnitRunner):
    def __init__(self, host_executable, distribution, *, persistent=False, long_deadlines=False,
                 owner_lifetime=False):
        super().__init__(host_executable, distribution)
        if type(persistent) is not bool or type(long_deadlines) is not bool:
            raise ContractError("stream modes must be explicit booleans")
        if type(owner_lifetime) is not bool or (owner_lifetime and not persistent):
            raise ContractError("owner lifetime requires a persistent workspace stream")
        self._persistent = persistent
        self._owner_lifetime = owner_lifetime
        self._transport_options = {
            "streaming": persistent,
            "max_wait": 3600 if persistent or long_deadlines else 300,
        }
        self._pending = None
        self._stream_identity = None

    @property
    def owner_lifetime(self):
        return self._owner_lifetime

    def _controller_source(self):
        helper = self._helper_source()
        stream = files("ahc.adapters").joinpath("linux_unit_stream.py").read_text(encoding="utf-8")
        return (
            "import types; h={'__name__':'controller'}; exec(" + repr(helper) + ",h); "
            "s={'__name__':'stream'}; exec(" + repr(stream) + ",s); "
            "s['serve'](types.SimpleNamespace(**h),persistent=" + repr(self._persistent)
            + ",owner_lifetime=" + repr(self._owner_lifetime) + ")"
        )

    def _call(self, request):
        if request["action"] != "create":
            return super()._call(request)
        if self._pending is not None:
            raise ContractError("previous stream still owned")
        phase = "helper"
        try:
            source = self._controller_source()
            phase = "open"
            self._pending = self._open_transport(source)
            deadline = time.monotonic() + 90
            phase = "send"
            self._pending.send(request, deadline=deadline)
            phase = "receive"
            reply = self._pending.receive(deadline=deadline)
            phase = "decode"
            if (self._persistent and type(reply) is dict
                    and set(reply) == {"ok", "error", "unit", "created"}
                    and reply["ok"] is False and reply["created"] is False
                    and reply["error"] == "interpreter-absent"
                    and reply["unit"] == request["unit"]):
                raise BackendCreationRejected("workspace interpreter absent before creation")
            value = self._decode(reply, request)
            if value.state != "running":
                raise BackendCustodyUnavailable("stream creation unverified")
            self._stream_identity = LinuxUnitIdentity(
                value.distribution, value.boot_id, value.unit, value.invocation_id
            )
            return value
        except BackendCreationRejected:
            raise
        except CodexTransportError as exc:
            raise WslControlOperationUnavailable(
                phase, control_error_type=exc.transport_error_type
            ) from None
        except Exception:
            raise WslControlOperationUnavailable(phase) from None

    def attach(self, unit, custody, lease):
        self._require(unit)
        if self._pending is None or unit != self._stream_identity:
            raise ContractError("stream identity mismatch or already attached")
        with custody.attachment(lease, unit):
            deadline = time.monotonic() + 30
            self._pending.send({"action": "attach", **asdict(unit)}, deadline=deadline)
            reply = self._pending.receive(deadline=deadline)
            if (
                type(reply) is not dict
                or set(reply) != {"attached"}
                or reply["attached"] is not True
            ):
                raise BackendCustodyUnavailable("stream attachment unverified")
            transport, self._pending = self._pending, None
            self._stream_identity = None
            return transport

    def close(self):
        if self._pending is not None:
            self._pending.close()
            self._pending = None
            self._stream_identity = None
