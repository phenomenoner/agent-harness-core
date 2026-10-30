"""Protected, offline Codex composition over the custody-gated WSL stream.

Only packaged source and the packaged pin are sent to the root controller.
No AHC installation, host mount, source rewriting or remote import fallback.
The caller still owns workspace admission and the durable creation intent.
"""

import base64
import json
import zlib
from importlib.resources import files

from ahc.domain.sessions import ContractError

from .wsl_stream_runner import WslStreamRunner


class WslCodexStreamRunner(WslStreamRunner):
    def __init__(self, host_executable, distribution, *, launch_spec):
        super().__init__(host_executable, distribution)
        # Freeze caller data; later mutation cannot change a submitted plan.
        if type(launch_spec) is not dict or any(
            type(key) is not str or type(value) not in (str, int)
            for key, value in launch_spec.items()
        ):
            raise ContractError("invalid protected launch specification")
        self._launch_json = json.dumps(launch_spec, sort_keys=True)
        if len(self._launch_json) > 8192:
            raise ContractError("protected launch specification too large")

    def _controller_source(self):
        resources = files("ahc.adapters")
        payload = {
            "modules": {
                name: resources.joinpath(name + ".py").read_text(encoding="utf-8")
                for name in (
                    "linux_codex_tree",
                    "linux_codex_launch",
                    "linux_unit_metadata",
                    "linux_unit_helper",
                    "linux_unit_stream",
                )
            },
            "pin": json.loads(
                resources.joinpath("codex_linux_backend.json").read_text(encoding="utf-8")
            ),
            "spec": json.loads(self._launch_json),
        }
        # Compression keeps the complete source below Windows command-line limits.
        encoded = base64.b85encode(zlib.compress(json.dumps(payload).encode())).decode("ascii")
        return (
            "import base64,json,zlib,sys,types,functools\n"
            "p=json.loads(zlib.decompress(base64.b85decode(" + repr(encoded) + ")))\n"
            "m={}\n"
            "for name,source in p['modules'].items():\n"
            " module=types.ModuleType(name); sys.modules[name]=module; m[name]=module\n"
            " exec(compile(source,name+'.py','exec'),module.__dict__)\n"
            "verify=functools.partial(m['linux_codex_tree'].verify_linux_tree,pin=p['pin'])\n"
            "protect=functools.partial(m['linux_codex_launch'].protect_arguments,"
            "spec=p['spec'],verify_bundle=verify)\n"
            "m['linux_unit_stream'].serve(m['linux_unit_helper'],protect_arguments=protect)\n"
        )
