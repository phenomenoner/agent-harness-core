"""Cold-interpreter proof that selecting the inert port does not load old hosts."""

import os
import subprocess
import sys
from pathlib import Path


def test_backend_import_does_not_load_superseded_infrastructure(tmp_path):
    source = Path(__file__).parents[1] / "src"
    env = {**os.environ, "PYTHONPATH": str(source), "PYTHONDONTWRITEBYTECODE": "1"}
    check = subprocess.run(
        [sys.executable, "-B", "-c", "\n".join([
            "import sys",
            "from ahc import AgentTurnRequest, AgentTurnObservation",
            "from ahc.application import AgentBackend",
            "from ahc.adapters.inert_agent_backend import InertAgentBackend",
            "assert not any(x.startswith('ahc.infrastructure') for x in sys.modules)",
            "assert 'ahc.application.plugin_host' not in sys.modules",
            "assert 'ahc.application.credential_facade' not in sys.modules",
        ])],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=15,
    )
    assert check.returncode == 0, check.stderr
