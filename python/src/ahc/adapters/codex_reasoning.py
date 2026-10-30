"""Optional model-advertised effort and its binding identity."""

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


def validate_effort(value):
    if value is not None and (type(value) is not str or not value or len(value) > 64
                              or not value.isascii() or not value.replace("-", "").isalnum()):
        raise ContractError("Codex reasoning effort invalid")
    return value


def effort_options(raw):
    if "reasoning_effort" not in raw:
        return {}
    if raw["reasoning_effort"] is None:
        raise ContractError("backend.reasoning_effort must be explicit text")
    return {"reasoning_effort": validate_effort(raw["reasoning_effort"])}


def client_configuration(cwd, model, target, executable_sha256, effort):
    base = digest_json({"cwd": cwd, "model": model, "protocol": "codex-app-server",
                        "wire_policy": "read-only-approval-never", "target": target,
                        "executable_sha256": executable_sha256})
    return base if effort is None else digest_json({"base": base, "reasoning_effort": effort})
