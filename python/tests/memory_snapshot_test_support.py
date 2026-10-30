"""Prepare an offline public-API fixture, separate from readonly recall effects."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def snapshot_fixture(root, names):
    provider = pytest.importorskip("openclaw_mem")
    packages = str(Path(provider.__file__).resolve().parent.parent)
    snapshot = root / "shared-memory.sqlite"
    configuration = root / "memory-openclaw.json"
    configuration.write_text("{}", encoding="utf-8")
    environment = {k: os.environ[k] for k in ("SystemRoot", "WINDIR") if k in os.environ}
    environment.update({"HOME": str(root), "USERPROFILE": str(root),
                        "OPENCLAW_HOME": str(root), "OPENCLAW_CONFIG_PATH": str(configuration)})
    prepare = """
import json,sys
sys.path.insert(0,sys.argv[1])
from openclaw_mem.core.api import connect,store_observation
connection=connect(sys.argv[2])
try:
    for name in json.loads(sys.argv[3]):
        store_observation(connection, {'kind':'note','summary':'orchid '+name,
                                       'detail':{'scope':name}})
    connection.commit()
finally:
    connection.close()
"""
    result = subprocess.run([sys.executable, "-I", "-B", "-c", prepare, packages, str(snapshot),
                             json.dumps(names)], cwd=root, env=environment,
                            capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    return snapshot, packages
