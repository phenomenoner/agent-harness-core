"""Compiler feedback is bounded; failed cells retain ordinary Python semantics."""

import json
import subprocess
import sys

from ahc.adapters.workspace_launch import kernel_command


def test_report_syntax_feedback_and_short_repair_preserve_namespace_without_secret_echo(tmp_path):
    codes = [
        "saved = 42",
        "from pathlib import Path\nPath('should-not-exist').write_text('PRIVATE')\n"
        "report = f(sum([0.1])/6:.12f)",
        "from pathlib import Path\nprint(saved); report = f'{sum([0.1])/6:.12f}'; "
        "Path('report.txt').write_text(report); print(Path('report.txt').read_text())",
        "saved += 1; raise RuntimeError('PRIVATE-EXCEPTION')",
        "print(saved)",
        "raise SyntaxError('PRIVATE-MESSAGE', ('PRIVATE-PATH', 1, 1, 'PRIVATE-SOURCE'))",
    ]
    frames = "".join(json.dumps({"id": i, "operation": "execute", "code": code}) + "\n"
                     for i, code in enumerate(codes, 1))
    command = kernel_command(str(tmp_path))
    assert len(command) <= 4096 and "\n" not in command
    process = subprocess.run([sys._base_executable, "-I", "-u", "-c", command],
        input=frames, capture_output=True, text=True, encoding="utf-8", cwd=tmp_path,
        timeout=10, check=True)
    replies = [json.loads(row) for row in process.stdout.splitlines()]
    assert len(replies) == len(codes) and not process.stderr
    failed = replies[1]
    assert failed["ok"] is False and failed["error"] == "execution-error"
    assert "SyntaxError at submitted line 3, column" in failed["output"]
    assert "PRIVATE" not in str(replies) and not (tmp_path / "should-not-exist").exists()
    assert replies[2]["ok"] is True and replies[2]["output"] == "42\n0.016666666667\n"
    assert (tmp_path / "report.txt").read_text() == "0.016666666667"
    assert replies[3]["ok"] is False and "RuntimeError" in replies[3]["output"]
    assert "Use the latest identity" in replies[3]["output"]
    assert replies[4]["output"] == "43\n"
    assert replies[5]["ok"] is False and replies[5]["output"] == ""


def test_real_kernel_accepts_code_above_former_cap_inside_the_actual_frame(tmp_path):
    code = "#" + "x"*20000 + "\nvalue=42; print(value)"
    payload = json.dumps({"id": 1, "operation": "execute", "code": code}) + "\n"
    assert len(code.encode()) > 16384 and len(payload.encode()) < 65536
    process = subprocess.run([sys._base_executable, "-I", "-u", "-c", kernel_command(str(tmp_path))],
        input=payload, capture_output=True, text=True, encoding="utf-8", timeout=10, check=True)
    reply = json.loads(process.stdout)
    assert reply["ok"] and reply["output"] == "42\n"
