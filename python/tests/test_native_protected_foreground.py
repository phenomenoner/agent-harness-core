"""Real foreground composition with guarded Python; backend remains an inert peer."""

import hashlib
import shutil
import sys
from pathlib import Path
from textwrap import dedent

import pytest
import test_native_foreground as existing
from test_linux_native import native_owner as _native_owner

native_owner = _native_owner


def test_protected_foreground_python_uses_actual_boundary(native_owner, tmp_path, monkeypatch):
    isolation = shutil.which("bwrap")
    if isolation is None:
        pytest.fail("required pinned namespace isolation executable unavailable")
    bwrap = Path(isolation).resolve()
    workspace = tmp_path / "isolated-work"
    workspace.mkdir()
    sentinel = tmp_path / "runtime" / "authority-canary.txt"
    denied = dedent(f"""\
        from pathlib import Path
        p=Path({str(sentinel)!r})
        assert not p.exists()
        try:
            p.read_text()
        except OSError:
            pass
        else:
            raise AssertionError('protected read escaped')
        try:
            p.write_text('escape')
        except OSError:
            pass
        else:
            raise AssertionError('protected write escaped')
        print('protected-read-write-denied')
        count=globals().get('count',0)+1;print(count)
        """)
    peer = existing.PEER.replace("\"count=globals().get('count',0)+1;print(count)\"", repr(denied))
    peer = peer.replace(
        "assert result['disposition']=='observed',result",
        "assert result['disposition']=='observed',result\n"
        "        assert result['result']['ok'],result\n"
        "        assert 'protected-read-write-denied' in result['result']['output'],result",
    )
    monkeypatch.setattr(existing, "PEER", peer)
    original = existing.compose_foreground_document
    roots = sorted(
        {
            str(Path("/usr").resolve()),
            str(Path(sys.prefix).resolve()),
            str(Path(sys.executable).resolve().parent.parent),
        }
    )

    def compose(value):
        if not sentinel.exists():
            sentinel.write_text("unchanged-authority")
        value["workspace"].update(
            kind="linux-python",
            cwd=str(workspace),
            protection={
                "executable": str(bwrap),
                "sha256": hashlib.sha256(bwrap.read_bytes()).hexdigest(),
                "readonly_roots": roots,
                "protected_roots": [value["runtime_root"]],
                "resource_limits": {
                    "memory_bytes": 256 * 1024**2,
                    "pids": 32,
                    "cpu_quota_us": 100000,
                    "cpu_period_us": 100000,
                    "swap_bytes": 0,
                },
            },
        )
        return original(value)

    monkeypatch.setattr(existing, "compose_foreground_document", compose)
    existing.test_native_composition_sequential_child_checkpoint_restart(
        native_owner, tmp_path, monkeypatch
    )
    assert sentinel.read_text() == "unchanged-authority"
