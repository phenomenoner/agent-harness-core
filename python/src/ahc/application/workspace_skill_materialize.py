"""Bounded data transfer through existing workspace execution and custody.

No new worker protocol, subprocess, destination override or script execution.
An indeterminate workspace step uses its existing close/no-replay mechanism.
"""

import base64
import json
import re
import secrets
import time
from importlib.resources import files

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_bytes, digest_json


class SkillTransferIncomplete(RuntimeError):
    """A transfer may have retained a partial copy; never relabel it no-effect."""


def materialize_skill(workspace, repository, revision, *, clock=time.monotonic):
    root = getattr(getattr(workspace, "port", None), "skill_materialization_root", None)
    if type(root) is not str or not root.startswith("/"):
        raise ContractError("skill materialization requires a configured Linux workspace")
    contents = {"SKILL.md": revision.body.encode("utf-8")}
    contents.update({asset.path: repository.read_asset(revision.revision_digest, asset.path)
                     for asset in revision.assets})
    manifest = [{"path": path, "sha256": digest_bytes(raw), "size": len(raw)}
                for path, raw in sorted(contents.items())]
    total = sum(row["size"] for row in manifest)
    variable = "_ahc_skill_" + secrets.token_hex(12)
    helper = files("ahc.adapters").joinpath("linux_skill_bundle.py").read_text("utf-8")
    setup = (
        f"{variable}={{}}\nexec({helper!r},{variable})\n"
        f"{variable}={variable}['SkillBundleWriter']({root!r},"
        f"{revision.revision_digest!r},{len(manifest)},{total})"
    )
    if len(setup.encode("utf-8")) > 16384:
        raise ContractError("skill bundle helper exceeds execution frame")
    deadline, steps = clock() + 30, 0

    def execute(code):
        nonlocal steps
        steps += 1
        if steps > 9000 or clock() >= deadline:
            raise ContractError("skill bundle transfer budget exhausted; partial copy retained")
        result = workspace.execute(workspace.identity, code)
        if result.get("ok") is not True or result.get("output_truncated") is not False:
            raise ContractError("skill bundle transfer is incomplete")
        return result.get("output", "")

    try:
        execute(setup)
        for row in manifest:
            execute(f"{variable}.call('begin_file',{row['path']!r},{row['size']},"
                    f"{row['sha256']!r})")
            raw = contents[row["path"]]
            for offset in range(0, len(raw), 8192):
                encoded = base64.b64encode(raw[offset:offset + 8192]).decode("ascii")
                execute(f"{variable}.call('chunk',{offset},{encoded!r})")
            execute(f"{variable}.call('end_file')")
        output = execute(
            f"print(__import__('json').dumps({variable}.call('finish'))); del {variable}")
        result = json.loads(output)
    except Exception as exc:
        # Release only descriptors held by this generated helper. Partial files
        # remain. Never send cleanup into an already closed/unknown generation.
        if not workspace.closed:
            try:
                workspace.execute(workspace.identity,
                    f"if {variable!r} in globals():\n"
                    f"    if hasattr({variable},'close'): {variable}.close()\n"
                    f"    del {variable}")
            except Exception:
                pass  # RuntimeWorkspace retains indeterminate step/cleanup custody.
        raise SkillTransferIncomplete("skill bundle transfer incomplete") from exc
    if (
        type(result) is not dict
        or set(result) != {"root", "revision_digest", "manifest_digest", "file_count", "byte_count"}
        or result["revision_digest"] != revision.revision_digest
        or result["manifest_digest"] != digest_json(manifest)
        or type(result["file_count"]) is not int or type(result["byte_count"]) is not int
        or result["file_count"] != len(manifest) or result["byte_count"] != total
        or type(result["root"]) is not str
        or not re.fullmatch(re.escape(root.rstrip("/")) + r"/\.ahc-skill-"
                           + revision.revision_digest[:12] + r"-[0-9a-f]{24}", result["root"])
    ):
        raise SkillTransferIncomplete("skill bundle result binding invalid")
    resource_context = {key: result[key] for key in ("root", "revision_digest", "manifest_digest")}
    return {**result, "resource_context": resource_context,
            "identity": workspace.identity.to_dict(),
            "scripts_executed": False, "copy_is_mutable": True}
