"""Validate selected bytes and enqueue attachments in the terminal transaction."""

from ahc.domain.result_artifacts import ResultArtifact, result_artifacts
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .artifact_repository import ArtifactRepository


def validate_selected(connection, session, result):
    raw = result.get("artifacts", [])
    if type(raw) is not list:
        raise ContractError("result artifact list invalid")
    if raw and result.get("accepted") is not True:
        raise ContractError("unaccepted result selects artifacts")
    selections = tuple(ResultArtifact.from_dict(item) for item in raw)
    result_artifacts(selections)
    for selection in selections:
        row = connection.execute(
            "SELECT * FROM runtime_artifacts WHERE artifact_id=?", (selection.artifact_id,)
        ).fetchone()
        if row is None:
            raise ContractError("selected result artifact unavailable")
        manifest, content = ArtifactRepository._decode(row)
        if selection.delivery_mode == "photo":
            from ahc.domain.channel_photo import validate_photo

            validate_photo(content, manifest.media_type)
        if manifest.producer.session != session:
            raise ContractError("selected result artifact owner mismatch")
    return selections


def enqueue_attachments(connection, operation_id, route, result):
    for ordinal, selection in enumerate(validate_selected(connection, route.session, result)):
        connection.execute(
            "INSERT INTO runtime_channel_attachments "
            "(operation_id,ordinal,route_json,selection_json,state) VALUES(?,?,?,?,'pending')",
            (operation_id, ordinal, canonical_json(route.to_dict()).decode(),
             canonical_json(selection.to_dict()).decode()),
        )
