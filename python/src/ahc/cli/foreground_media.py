"""Bounded native image input from the current foreground file import."""

import base64

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_execution_scope import workspace_request

# CodexStdioTransport caps one outbound JSONL frame at 1 MiB. The image URLs
# carry base64 and the turn also carries user text, context, and selected skills.
# Keep image source bytes below the transport cap with room for those fields.
MAX_NATIVE_IMAGE_BYTES = 600_000
MAX_NATIVE_IMAGE_ITEMS = 8

_SIGNATURES = {
    "image/png": lambda value: value.startswith(b"\x89PNG\r\n\x1a\n"),
    "image/jpeg": lambda value: value.startswith(b"\xff\xd8\xff"),
    "image/gif": lambda value: value.startswith((b"GIF87a", b"GIF89a")),
    "image/webp": lambda value: len(value) >= 12 and value[:4] == b"RIFF"
    and value[8:12] == b"WEBP",
}

_NOTICE_PREFIX = (
    "AHC native image input notice: omitted imported attachment(s) remain available through "
    "workspace.incoming_artifacts and workspace.read_artifact; "
)


def _same_request(active, request):
    return (
        type(active) is AgentTurnRequest
        and request_fingerprint(active) == request_fingerprint(request)
    )


def _same_workspace_owner(producer, identity):
    """Accept prior workspace generations within the exact current scope."""
    return (
        type(producer) is WorkspaceIdentity
        and producer.session == identity.session
        and producer.workspace_id == identity.workspace_id
        and producer.generation <= identity.generation
    )


def _project(request, identity, artifacts):
    if type(request) is not AgentTurnRequest:
        raise ContractError("foreground media request invalid")
    if (type(identity) is not WorkspaceIdentity
            or request.session != identity.session
            or request.workspace_scope != identity.workspace_id):
        raise ContractError("foreground media workspace binding mismatch")
    active = workspace_request(artifacts.store, identity.session, identity.workspace_id)
    if not _same_request(active, request):
        raise ContractError("foreground media operation binding mismatch")

    from ahc.infrastructure.channel_context_store import read_context

    # Validate the durable event context when present. The sidecar/import rows
    # below remain the source of selected artifact ids and bytes.
    read_context(artifacts.store, request)
    from ahc.infrastructure.channel_file_import import LegacyFileImport

    try:
        selections = artifacts.incoming(identity, require_provenance=True)
    except LegacyFileImport:
        return [{"type": "text", "text":
                 "AHC did not project these legacy attachments as images: their import lacks "
                 "selection provenance. Ask the user to upload again; do not claim to see them."}]
    projected = []
    image_bytes = 0
    unsupported = 0
    budget_skipped = 0
    for selection in selections:
        if type(selection) is not dict or set(selection) != {"artifact_id", "filename"}:
            raise ContractError("foreground media selection invalid")
        stored = artifacts.read(request.session, selection["artifact_id"])
        if stored is None:
            raise ContractError("foreground media artifact unavailable")
        manifest, content = stored
        if not _same_workspace_owner(manifest.producer, identity):
            raise ContractError("foreground media artifact owner mismatch")
        checker = _SIGNATURES.get(manifest.media_type)
        if checker is None or not checker(content):
            unsupported += 1
            continue
        if (len(projected) >= MAX_NATIVE_IMAGE_ITEMS
                or image_bytes + len(content) > MAX_NATIVE_IMAGE_BYTES):
            budget_skipped += 1
            continue
        encoded = base64.b64encode(content).decode("ascii")
        projected.append({
            "type": "image",
            "url": f"data:{manifest.media_type};base64,{encoded}",
        })
        image_bytes += len(content)

    skipped = unsupported + budget_skipped
    if skipped:
        reasons = []
        if unsupported:
            reasons.append(f"{unsupported} unsupported image or non-image byte set")
        if budget_skipped:
            reasons.append(f"{budget_skipped} over the partial image budget")
        projected.append({
            "type": "text",
            "text": _NOTICE_PREFIX + "; ".join(reasons) + ".",
        })
    return projected


def compose_foreground_media(store, tools):
    """Return a request-bound callback only for the foreground workspace owner."""

    if tools is None or getattr(tools, "artifacts", None) is None:
        return None
    workspace = getattr(tools, "workspace", None)
    artifacts = tools.artifacts
    if (getattr(workspace, "store", None) is not store
            or type(getattr(workspace, "identity", None)) is not WorkspaceIdentity
            or not isinstance(artifacts, ArtifactRepository)
            or artifacts.store is not store):
        raise ContractError("foreground media owner binding invalid")
    identity = workspace.identity
    return lambda request: _project(request, identity, artifacts)


__all__ = ["MAX_NATIVE_IMAGE_BYTES", "MAX_NATIVE_IMAGE_ITEMS", "compose_foreground_media"]
