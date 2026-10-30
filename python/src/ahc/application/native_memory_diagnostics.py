"""Opt-in source identity readback; never import provider code or open its database."""

from pathlib import Path

from ahc.adapters.memory_provider_identity import provider_digest


def inspect_native_provider(binding):
    report = {"observation_scope": "provider-source-identity", "execution_verified": False,
              "database_inspected": False}
    try:
        root = Path(binding.provider_root)
        if not root.exists() and not root.is_symlink():
            return {**report, "status": "unavailable", "reason_code": "provider-directory-absent"}
        actual = provider_digest(root)
        if actual != binding.provider_digest:
            return {**report, "status": "blocked", "reason_code": "provider-content-mismatch"}
        return {**report, "status": "present-unverified", "reason_code": None}
    except ValueError:
        return {**report, "status": "unavailable", "reason_code": "provider-source-invalid"}
    except OSError:
        return {**report, "status": "indeterminate", "reason_code": "provider-source-unreadable"}
