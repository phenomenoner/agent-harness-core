"""Explicit root control channel; workloads still execute as ordinary ahc.

This uses the existing bounded JSONL transport, not Codex launch/admission.
Loss after create is indeterminate. Windows Job cleanup only owns the client.
"""

import os
import time
from importlib.resources import files
from uuid import uuid4

from ahc.domain.backend_creation import (
    BackendCreationAttempt, BackendCreationCancelled, BackendCreationRejected,
)
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import (
    BackendCustodyUnavailable,
    LinuxUnitIdentity,
    UnitObservation,
    UnitRetirement,
)
from ahc.infrastructure.process.managed import spawn_managed

from .codex_transport import CodexStdioTransport, CodexTransportError
from .wsl_command import WslCommand


class WslCreationIndeterminate(BackendCustodyUnavailable):
    """Diagnostic attempt identity only; never sufficient authority for stop."""

    def __init__(self, distribution, unit, *, control_phase="unknown",
                 control_error_type="unknown"):
        super().__init__("WSL creation outcome indeterminate; automatic replay prohibited")
        self.distribution, self.unit = distribution, unit
        self.control_phase = _control_phase(control_phase)
        self.control_error_type = _control_error_type(control_error_type)


class WslControlTransportUnavailable(BackendCustodyUnavailable):
    """No validated control reply; distinct from a rejected identity or state."""

    def __init__(self, message="WSL unit operation unverified", *, control_phase="unknown",
                 control_error_type="unknown"):
        super().__init__(message)
        self.control_phase = _control_phase(control_phase)
        self.control_error_type = _control_error_type(control_error_type)


class WslControlOperationUnavailable(BackendCustodyUnavailable):
    """Sanitized last control phase, not proof of the remote operation's outcome."""

    def __init__(self, phase, *, control_error_type="unknown"):
        super().__init__("WSL control operation unverified")
        self.control_phase = _control_phase(phase)
        self.control_error_type = _control_error_type(control_error_type)


def _control_phase(value):
    return value if type(value) is str and value in {
        "helper", "open", "send", "receive", "close", "decode", "unknown"
    } else "unknown"


def _control_error_type(value):
    return value if type(value) is str and value in CodexTransportError._TYPES else "unknown"


class WslUnitRunner:
    def __init__(self, host_executable, distribution):
        # Reuse explicit host/distro validation; root belongs only to this controller.
        plan = WslCommand(host_executable, distribution, "ahc", "/", "/usr/bin/python3")
        self.host_executable, self.distribution = plan.host_executable, plan.distribution
        self.observation_transport_failures = 0

    def control_ready(self):
        """Read-only reachability probe; it grants no unit identity or stop authority."""
        import subprocess

        try:
            result = subprocess.run(
                [self.host_executable, "--distribution", self.distribution,
                 "--user", "ahc", "--exec", "/usr/bin/true"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=3, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return result.returncode == 0
        except (OSError, subprocess.SubprocessError):
            return False

    def _open_transport(self, source):
        environment = {
            key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ
        }
        owned = spawn_managed(
            (
                self.host_executable,
                "--distribution",
                self.distribution,
                "--user",
                "root",
                "--cd",
                "/",
                "--exec",
                "/usr/bin/python3",
                "-I",
                "-c",
                source,
            ),
            cwd=None,
            env=environment,
            protocol_pipes=True,
        )
        try:
            return CodexStdioTransport(owned, **getattr(self, "_transport_options", {}))
        except BaseException:
            owned.cleanup(5)
            raise

    def _helper_source(self):
        resources = files("ahc.adapters")
        metadata = resources.joinpath("linux_unit_metadata.py").read_text(encoding="utf-8")
        helper = resources.joinpath("linux_unit_helper.py").read_text(encoding="utf-8")
        return (
            "import sys,types\n"
            "_metadata=types.ModuleType('linux_unit_metadata')\n"
            "sys.modules['linux_unit_metadata']=_metadata\n"
            "exec(compile("
            + repr(metadata)
            + ", 'linux_unit_metadata.py', 'exec'),_metadata.__dict__)\n"
            + helper
        )

    def _call(self, request):
        phase = "helper"
        try:
            source = self._helper_source()
            phase = "open"
            with self._open_transport(source) as transport:
                deadline = time.monotonic() + 90
                phase = "send"
                transport.send(request, deadline=deadline)
                phase = "receive"
                reply = transport.receive(deadline=deadline)
                phase = "close"
            phase = "decode"
            return self._decode(reply, request)
        except BackendCreationRejected:
            raise
        except CodexTransportError as exc:
            raise WslControlTransportUnavailable(
                control_phase=phase,
                control_error_type=exc.transport_error_type,
            ) from None
        except Exception:
            raise WslControlOperationUnavailable(phase) from None

    def _decode(self, reply, request):
        try:
            if set(reply) != {"ok", "result"} or reply["ok"] is not True:
                raise ValueError()
            value = reply["result"]
            if request["action"] == "reconcile-create":
                if type(value) is not dict or set(value) != {"unit", "cancelled", "identity"}:
                    raise ValueError()
                if value["unit"] != request["unit"]:
                    raise ValueError()
                attempt = BackendCreationAttempt(self.distribution, request["unit"])
                if value["cancelled"] is True and value["identity"] is None:
                    return BackendCreationCancelled(attempt)
                if value["cancelled"] is not False:
                    raise ValueError()
                identity = LinuxUnitIdentity(**value["identity"])
                if (identity.distribution, identity.unit) != (attempt.distribution, attempt.unit):
                    raise ValueError()
                return identity
            if request["action"] == "retire":
                if type(value) is not dict or set(value) != {"identity", "current_boot_id"}:
                    raise ValueError()
                identity = LinuxUnitIdentity(**value["identity"])
                if identity != LinuxUnitIdentity(
                    self.distribution, request["boot_id"], request["unit"], request["invocation_id"]
                ):
                    raise ValueError()
                return UnitRetirement(identity, value["current_boot_id"])
            if type(value) is not dict or set(value) != {
                "distribution",
                "boot_id",
                "unit",
                "invocation_id",
                "state",
                "cgroup_empty",
            }:
                raise ValueError()
            observation = UnitObservation(**value)
            if (
                observation.distribution != self.distribution
                or observation.unit != request["unit"]
                or observation.state not in ("running", "stopped", "absent")
                or type(observation.cgroup_empty) is not bool
            ):
                raise ValueError()
            # Validate boot/unit even when a collected unit has no InvocationID.
            LinuxUnitIdentity(
                self.distribution,
                observation.boot_id,
                observation.unit,
                observation.invocation_id if observation.state != "absent" else "1" * 32,
            )
            if observation.state == "absent" and observation.invocation_id is not None:
                raise ValueError()
            return observation
        except (ValueError, TypeError, ContractError, CodexTransportError):
            raise BackendCustodyUnavailable("WSL unit operation unverified") from None

    def prepare_creation(self, command, *, lifetime_seconds):
        # Reject invalid plans before any client or Linux effect. Do not consume
        # arbitrary iterators or interpret a string as a sequence of arguments.
        if (
            type(command) not in (list, tuple)
            or not 1 <= len(command) <= 64
            or any(
                type(arg) is not str or not arg or len(arg) > 4096 or any(ord(c) < 32 for c in arg)
                for arg in command
            )
            or not command[0].startswith("/")
            or type(lifetime_seconds) is not int
            or not 1 <= lifetime_seconds <= 3600
        ):
            raise ContractError("invalid workload command or lifetime")
        return BackendCreationAttempt(self.distribution, "ahc-backend-" + uuid4().hex + ".service")

    def create(self, command, *, lifetime_seconds):
        attempt = self.prepare_creation(command, lifetime_seconds=lifetime_seconds)
        return self.create_attempt(attempt, command, lifetime_seconds=lifetime_seconds)

    def create_attempt(self, attempt, command, *, lifetime_seconds):
        if type(attempt) is not BackendCreationAttempt or attempt.distribution != self.distribution:
            raise ContractError("creation attempt belongs to another distribution")
        # Validate again for direct callers without changing the supplied attempt.
        self.prepare_creation(command, lifetime_seconds=lifetime_seconds)
        unit = attempt.unit
        try:
            value = self._call(
                {
                    "action": "create",
                    "unit": unit,
                    "command": list(command),
                    "lifetime_seconds": lifetime_seconds,
                }
            )
            if value.state != "running":
                raise BackendCustodyUnavailable("WSL creation identity unavailable")
            return LinuxUnitIdentity(
                value.distribution, value.boot_id, value.unit, value.invocation_id
            )
        except BackendCreationRejected:
            raise
        except Exception as exc:
            # Even client cleanup can fail after Linux created the unit. No retry,
            # stop-by-name, or invented full Linux identity follows this exception.
            raise WslCreationIndeterminate(
                self.distribution,
                unit,
                control_phase=getattr(exc, "control_phase", "unknown"),
                control_error_type=getattr(exc, "control_error_type", "unknown"),
            ) from None

    def _require(self, identity):
        if type(identity) is not LinuxUnitIdentity or identity.distribution != self.distribution:
            raise ContractError("WSL unit belongs to another distribution")

    def reconcile_creation(self, attempt):
        if type(attempt) is not BackendCreationAttempt or attempt.distribution != self.distribution:
            raise ContractError("creation attempt belongs to another distribution")
        return self._call({"action": "reconcile-create", "unit": attempt.unit})

    def observe(self, identity):
        self._require(identity)
        request = {"action": "observe", "unit": identity.unit}
        # Only this read-only action may reacquire a lost control reply. Each
        # attempt retains the existing transport bound; no mutation is replayed.
        for attempt in range(2):
            try:
                return self._call(request)
            except WslControlTransportUnavailable:
                self.observation_transport_failures += 1
                if attempt == 1:
                    raise

    def confirm_retirement(self, identity):
        self._require(identity)
        return self._call(
            {
                "action": "retire",
                "unit": identity.unit,
                "boot_id": identity.boot_id,
                "invocation_id": identity.invocation_id,
            }
        )

    def stop(self, identity):
        self._require(identity)
        return self._call(
            {
                "action": "stop",
                "unit": identity.unit,
                "boot_id": identity.boot_id,
                "invocation_id": identity.invocation_id,
            }
        )
