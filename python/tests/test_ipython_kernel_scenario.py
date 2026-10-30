"""Real optional IPython process; no WSL or provider substitute claim."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from ahc.adapters.ipython_kernel import IPYTHON_KERNEL


def run_kernel(requests):
    ipython = pytest.importorskip("IPython")
    packages = str(Path(ipython.__file__).resolve().parent.parent)
    command = f"import sys;sys.path.insert(0, {packages!r});exec({IPYTHON_KERNEL!r})"
    result = subprocess.run(
        [sys._base_executable, "-I", "-u", "-c", command],
        input="".join(json.dumps(r) + "\n" for r in requests),
        text=True, capture_output=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert [r["id"] for r in rows] == [r["id"] for r in requests]
    return rows


def test_real_ipython_cells_persistence_error_and_new_process_restore():
    rows = run_kernel([
        {"id": "1", "operation": "execute", "code": "value = 40\nvalue + 1"},
        {"id": "2", "operation": "execute", "code": "%precision 3\nvalue += 2\nprint(value)"},
        {"id": "3", "operation": "execute", "code": "raise ValueError('expected')"},
        {"id": "4", "operation": "inspect", "names": ["value"]},
        {"id": "5", "operation": "checkpoint", "names": ["value"]},
    ])
    assert rows[0]["ok"] and "41" in rows[0]["output"]
    assert rows[1]["ok"] and "42" in rows[1]["output"]
    assert rows[2]["ok"] is False and rows[2]["error"] == "execution-error"
    assert rows[3]["variables"] == [{"name": "value", "kind": "integer", "size": None}]
    assert rows[4]["values"] == {"value": 42}
    restored = run_kernel([
        {"id": "6", "operation": "restore", "values": rows[4]["values"]},
        {"id": "7", "operation": "execute", "code": "value += 1\nprint(value)"},
        {"id": "8", "operation": "checkpoint", "names": ["value"]},
    ])
    assert restored[1]["ok"] and "43" in restored[1]["output"]
    assert restored[2]["values"] == {"value": 43}


def test_real_ipython_cell_magic_and_output_limit():
    rows = run_kernel([
        {"id": "1", "operation": "execute", "code": "%%capture captured\nprint('hello')"},
        {"id": "2", "operation": "execute", "code": "print(captured.stdout.strip())"},
        {"id": "3", "operation": "execute", "code": "print('x' * 5000)"},
    ])
    assert all(r["ok"] for r in rows)
    assert rows[0]["output"] == ""
    assert rows[1]["output"].strip() == "hello"
    assert len(rows[2]["output"]) == 4096
    assert rows[2]["output_truncated"] is True


def test_display_limit_and_update_are_explicit_and_reset_between_cells():
    rows = run_kernel([
        {"id": "1", "operation": "execute", "code":
         "from IPython.display import display, HTML\ndisplay(HTML('x'*17000))"},
        {"id": "2", "operation": "execute", "code":
         "for i in range(20): display(HTML('<b>ok</b>'))"},
        {"id": "3", "operation": "execute", "code": "display(HTML('x'), display_id='id')"},
        {"id": "4", "operation": "execute", "code": "print('next')"},
    ])
    assert all(row["ok"] for row in rows)
    assert rows[0]["displays"] == [] and rows[0]["display_truncated"]
    assert len(rows[1]["displays"]) == 16 and rows[1]["display_truncated"]
    assert rows[2]["displays"] == [] and rows[2]["display_truncated"]
    assert rows[3]["displays"] == [] and not rows[3]["display_truncated"]


def test_rich_final_expression_reaches_display_without_changing_plain_output():
    rows = run_kernel([
        {"id": "1", "operation": "execute", "code":
         "from IPython.display import HTML\nHTML('<b>expression</b>')"},
        {"id": "2", "operation": "execute", "code": "6 * 7"},
    ])
    assert rows[0]["ok"]
    assert rows[0]["displays"][0]["text/html"] == "<b>expression</b>"
    assert not rows[0]["display_truncated"]
    assert rows[1]["ok"] and "42" in rows[1]["output"]
    assert rows[1]["displays"] == []
