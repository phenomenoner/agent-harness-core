import ast
import base64
import json
import subprocess
import sys
import zlib
from importlib.resources import files

import pytest

from ahc.adapters.wsl_codex_stream_runner import WslCodexStreamRunner
from ahc.domain.sessions import ContractError


def test_controller_embeds_exact_packaged_source_and_frozen_spec():
    spec = {"home": "/initial"}
    runner = WslCodexStreamRunner("C:/Windows/System32/wsl.exe", "AHC", launch_spec=spec)
    spec["home"] = "/changed"
    source = runner._controller_source()
    tree = ast.parse(source)
    encoded = next(
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "b85decode"
    )
    payload = json.loads(zlib.decompress(base64.b85decode(encoded)))
    assert payload["spec"] == {"home": "/initial"}
    for name, text in payload["modules"].items():
        assert text == files("ahc.adapters").joinpath(name + ".py").read_text(encoding="utf-8")
    assert payload["pin"] == json.loads(
        files("ahc.adapters").joinpath("codex_linux_backend.json").read_text(encoding="utf-8")
    )
    assert (
        len(subprocess.list2cmdline(["wsl.exe", "--exec", "/usr/bin/python3", "-I", "-c", source]))
        < 30000
    )
    # Exact isolated bootstrap imports no installed AHC and cannot use cwd imports.
    result = subprocess.run(
        [sys.executable, "-I", "-c", source],
        input=b'{"action":"create","unit":"invalid"}\n',
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"ok": False, "error": "unit stream unverified"}


@pytest.mark.parametrize("spec", [None, {"home": object()}, {"home": True}, {"home": "x" * 9000}])
def test_unserializable_or_unbounded_spec_rejected(spec):
    with pytest.raises(ContractError):
        WslCodexStreamRunner("C:/Windows/System32/wsl.exe", "AHC", launch_spec=spec)
