"""Explicit local artifact publication and exact identity readback."""

import base64
from pathlib import Path

from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository

from .runtime import _json

COMMANDS = frozenset({"artifact-publish", "artifact-read"})


def register_artifact_commands(commands):
    for name in sorted(COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        if name == "artifact-publish":
            parser.add_argument("--producer-json", required=True)
            parser.add_argument("--content-file", type=Path, required=True)
            parser.add_argument("--media-type", required=True)
            parser.add_argument("--lineage", action="append", default=[])
        else:
            parser.add_argument("--session-json", required=True)
            parser.add_argument("--artifact-id", required=True)
            parser.add_argument("--include-content", action="store_true")


def run_artifact_command(args):
    artifacts = ArtifactRepository(RuntimeRepository(args.runtime_root))
    if args.command == "artifact-publish":
        producer = WorkspaceIdentity.from_dict(_json(args.producer_json))
        with args.content_file.open("rb") as stream:
            content = stream.read(1048577)
        manifest = artifacts.publish(
            producer, content, args.media_type, lineage=tuple(args.lineage)
        )
        return 0, {
            "command": args.command,
            "artifact_id": manifest.artifact_id,
            "manifest": manifest.to_dict(),
        }
    session = VirtualSessionIdentity.from_dict(_json(args.session_json))
    value = artifacts.read(session, args.artifact_id)
    if value is None:
        return 0, {"command": args.command, "artifact": None}
    manifest, content = value
    return 0, {
        "command": args.command,
        "artifact_id": manifest.artifact_id,
        "manifest": manifest.to_dict(),
        **(
            {"content_base64": base64.b64encode(content).decode("ascii")}
            if args.include_content
            else {}
        ),
    }
