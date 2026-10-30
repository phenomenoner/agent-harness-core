"""Static architecture gate for bounded M0 and ASI Python modules."""

from __future__ import annotations

import ast
from ast import literal_eval
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "ahc"
HARD_LINE_LIMIT = 300
GUARD_RELATIVE = Path("src/ahc/domain/skill_guard.py")
DOMAIN_OUTWARD = ("ahc.application", "ahc.cli", "ahc.infrastructure", "ahc.adapters")
RUNTIME_APPLICATION = {
    "custodied_backend.py", "foreground_runtime.py", "runtime_service.py",
    "trusted_codex_backend.py", "wsl_codex_backend.py",
    "channel_ingress.py", "channel_delivery.py", "memory_port.py", "memory_diagnostics.py",
    "child_dispatch.py",
    "workspace_artifact_tools.py",
}
ASI_IMPORT_PREFIXES = (
    "ahc.adapters",
    "ahc.memory",
    "ahc.providers",
    "ahc.channels",
    "ahc.infrastructure.process",
)
ASI_IMPORT_NAMES = ("subprocess", "socket", "win32com", "comtypes", "psutil")


def python_sources() -> list[Path]:
    return sorted(SOURCE.rglob("*.py")) if SOURCE.exists() else []


def import_names(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def guard_literals() -> tuple[str, ...]:
    path = ROOT / GUARD_RELATIVE
    if not path.is_file():
        return ()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(target, "id", None) == "PROTECTED_FAMILIES" for target in node.targets
        ):
            value = literal_eval(node.value)
            return tuple(item for family in value.values() for item in family)
    return ()


def is_asi(path: Path) -> bool:
    relative = path.relative_to(ROOT).as_posix()
    return (
        "/skill_" in f"/{relative}"
        or "/judgment" in f"/{relative}"
        or relative.endswith("/cli/main.py")
    ) and "/domain/models.py" not in f"/{relative}"


def violations() -> list[str]:
    findings: list[str] = []
    guard_path = ROOT / GUARD_RELATIVE
    literals = guard_literals()
    for path in python_sources():
        relative = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        line_count = len(text.splitlines())
        if line_count >= HARD_LINE_LIMIT:
            findings.append(f"{relative}: {line_count} lines must be below {HARD_LINE_LIMIT}")
        names = import_names(path)
        if "/domain/" in f"/{relative}" and any(name.startswith(DOMAIN_OUTWARD) for name in names):
            findings.append(f"{relative}: domain imports outward dependency")
        if "/application/" in f"/{relative}" and path.name in RUNTIME_APPLICATION:
            if any(name.startswith(("ahc.cli", "ahc.adapters", "ahc.infrastructure"))
                   for name in names):
                findings.append(f"{relative}: runtime application imports concrete assembly")
        if is_asi(path):
            if path != guard_path:
                lowered = text.casefold()
                for literal in literals:
                    if literal.casefold() in lowered:
                        findings.append(f"{relative}: protected guard literal outside skill_guard")
                        break
            for name in names:
                if name in ASI_IMPORT_NAMES or any(
                    name.startswith(prefix) for prefix in ASI_IMPORT_PREFIXES
                ):
                    findings.append(f"{relative}: forbidden ASI import {name}")
    return findings


def main() -> int:
    findings = violations()
    if findings:
        print("\n".join(findings))
        return 1
    print(
        f"architecture gate passed: {len(python_sources())} source modules; "
        f"all modules <{HARD_LINE_LIMIT} lines"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
