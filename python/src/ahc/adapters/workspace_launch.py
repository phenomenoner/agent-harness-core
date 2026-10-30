"""Bounded self-contained launch source for explicitly selected interpreters."""

import base64
import zlib

from ahc.domain.sessions import ContractError

from .workspace_kernel import KERNEL


def kernel_command(cwd, engine="python", *, startup_timeout=None):
    if engine == "python":
        source = KERNEL
    elif engine == "ipython":
        from .ipython_kernel import IPYTHON_KERNEL

        source = IPYTHON_KERNEL
    else:
        raise ContractError("workspace engine unsupported")
    payload = base64.b64encode(zlib.compress(source.encode())).decode()
    command = ("import os,base64,zlib;os.chdir(" + repr(cwd) + ");exec("
               "zlib.decompress(base64.b64decode(" + repr(payload) + ")).decode())")
    if startup_timeout is not None:
        if type(startup_timeout) is not int or not 1 <= startup_timeout <= 30:
            raise ContractError("workspace startup timeout invalid")
        # No generated user code runs before the first host frame. That frame
        # is sent only after durable full-identity custody and stream attachment.
        command = ("import select,sys;sys.exit(124) if not select.select([sys.stdin],[],[],"
                   + str(startup_timeout) + ")[0] else None;" + command)
    if len(command) > 4096 or any(ord(char) < 32 for char in command):
        raise ContractError("Python workspace kernel command exceeds unit bounds")
    return command
