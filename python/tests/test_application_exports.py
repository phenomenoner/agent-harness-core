"""Selecting core exports does not load optional credential or plugin capabilities."""

import subprocess
import sys
from pathlib import Path


def test_static_lazy_exports_keep_core_import_optional_capabilities_unloaded():
    source = str(Path(__file__).parents[1] / "src")
    script = """
import sys
sys.path.insert(0, sys.argv[1])
import ahc.application as app
assert 'ahc.application.credential_facade' not in sys.modules
assert 'ahc.application.plugin_host' not in sys.modules
assert app.AgentBackend.__module__ == 'ahc.application.agent_backend_port'
assert app.AgentBackend is app.AgentBackend
assert 'ahc.application.credential_facade' not in sys.modules
assert 'ahc.application.plugin_host' not in sys.modules
try:
    app.unconfigured_capability
except AttributeError:
    pass
else:
    raise AssertionError('unknown export accepted')
assert 'ahc.application.credential_facade' not in sys.modules
assert 'ahc.application.plugin_host' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, source], capture_output=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
