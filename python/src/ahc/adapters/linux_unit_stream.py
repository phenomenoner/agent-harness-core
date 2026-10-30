"""Stdlib controller stream. Injected helper owns reservation and identity queries.

No vendor bytes flow until the caller acknowledges durable full-identity custody.
This is transport gating, not executable or provider policy admission.
"""

import json
import os
import select
import subprocess
import sys
import threading
import time
from itertools import count

# Host-to-unit JSONL retains its existing 1 MiB bound.
MAX_INPUT_FRAME = 1048576
# Unit-to-host vendor JSONL accepts large command output.
MAX_RELAY_FRAME = 4 * 1048576


def _relay_input(source, target, persistent=False, disconnected=None):
    try:
        for _ in count():
            raw = source.readline(MAX_INPUT_FRAME + 1)
            if not raw:
                break
            if len(raw) > MAX_INPUT_FRAME or not raw.endswith(b"\n"):
                break
            target.write(raw)
            target.flush()
    except (OSError, ValueError):
        pass
    finally:
        try:
            target.close()
        finally:
            if disconnected is not None:
                disconnected()


def serve(helper, *, protect_arguments=None, persistent=False, owner_lifetime=False):
    if type(persistent) is not bool:
        raise ValueError("invalid stream mode")
    if type(owner_lifetime) is not bool or (owner_lifetime and not persistent):
        raise ValueError("owner lifetime requires persistent mode")
    child = None
    attached = False
    owned_identity = None

    def stop_owned():
        if not owner_lifetime or owned_identity is None:
            return
        try:
            helper.control({"action": "stop", **{
                key: owned_identity[key] for key in ("unit", "boot_id", "invocation_id")}})
        except (ValueError, OSError, helper.ControlError, subprocess.SubprocessError):
            # Windows retains durable custody and must still verify cleanup.
            pass

    def frame():
        if owner_lifetime and not select.select([sys.stdin.buffer], [], [], 30)[0]:
            raise ValueError("workspace attachment deadline expired")
        raw = sys.stdin.buffer.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b"\n"):
            raise ValueError("invalid control frame")
        return json.loads(raw, object_pairs_hook=helper._object)

    def launch(arguments, unit):
        nonlocal child, owned_identity
        # Trusted controller composition supplies this callback, never a wire
        # field. It runs under helper.control's unit lock before Popen.
        if protect_arguments is not None:
            arguments = protect_arguments(arguments)
        if owner_lifetime:
            boundary = arguments.index("--")
            arguments = [
                "--property=RuntimeMaxSec=infinity"
                if arg.startswith("--property=RuntimeMaxSec=") else arg
                for arg in arguments[:boundary]
            ] + arguments[boundary:]
        child = subprocess.Popen(
            [arguments[0], "--quiet", "--pipe", "--wait", *arguments[1:]],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env={"PATH": "/usr/bin:/bin", "LANG": "C"},
        )
        deadline = time.monotonic() + 15
        # One bounded startup observation budget; no controller retry/recreation.
        for _ in range(40):
            value = helper._observe(unit)
            if value["state"] == "running" and value["invocation_id"]:
                owned_identity = value
                return value
            if child.poll() is not None or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        raise ValueError("unit start unverified")

    try:
        request = frame()
        if type(request) is not dict or request.get("action") != "create":
            raise ValueError("stream requires create")
        # Only persistent Python uses this pre-create absence check. A positive
        # stat is not executable admission; all later failures remain unknown.
        command = request.get("command")
        if persistent and type(command) is list and command and type(command[0]) is str:
            try:
                os.stat(command[0])
            except FileNotFoundError:
                print(json.dumps({"ok": False, "error": "interpreter-absent",
                                  "unit": request.get("unit"), "created": False}), flush=True)
                return
        identity = helper.control(request, launch=launch)
        # control has released its root unit lock before waiting for attachment.
        print(json.dumps({"ok": True, "result": identity}), flush=True)
        expected = {
            "action": "attach",
            **{key: identity[key] for key in ("distribution", "boot_id", "unit", "invocation_id")},
        }
        if frame() != expected:
            raise ValueError("attachment identity mismatch")
        current = helper.control({"action": "observe", "unit": identity["unit"]})
        if any(current[key] != identity[key] for key in expected if key != "action"):
            raise ValueError("attachment identity changed")
        if current["state"] != "running":
            raise ValueError("attachment unit unavailable")
        print(json.dumps({"attached": True}), flush=True)
        attached = True

        threading.Thread(
            target=_relay_input,
            args=(sys.stdin.buffer, child.stdin, persistent,
                  stop_owned if owner_lifetime else None),
            daemon=True,
        ).start()
        for _ in count():
            raw = child.stdout.readline(MAX_RELAY_FRAME + 1)
            if not raw:
                break
            if len(raw) > MAX_RELAY_FRAME or not raw.endswith(b"\n"):
                raise ValueError("invalid vendor frame")
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()
    except (ValueError, OSError, helper.ControlError, subprocess.SubprocessError):
        if not attached:
            print(json.dumps({"ok": False, "error": "unit stream unverified"}), flush=True)
    finally:
        stop_owned()
        if child is not None and child.poll() is None:
            child.terminate()
        # Only the local pipe client is terminated here. Full Linux cleanup is
        # still the Windows custody owner's separate observe/stop/readback duty.
