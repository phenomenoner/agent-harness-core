"""Export an immutable primitive snapshot in chunks that fit the real pipe frame."""

import base64
from hashlib import sha256
from io import BytesIO

from ahc.domain.sessions import ContractError
from ahc.domain.workspace_inspection import inspection_names

from .docker_workspace import WorkspaceUnavailable

# 32 KiB raw fits the 64 KiB JSON frame after base64 and the control envelope.
CHUNK_SIZE = 32_768


def export_artifact(workspace, expected, name):
    inspection_names((name,))
    with workspace._lock:
        workspace._require(expected)
        request = {"operation": "export_artifact", "name": name}
        offset, export_id, total, digest = 0, None, None, None
        hasher = sha256()
        with BytesIO() as content:
            while True:
                reply = workspace._reply(request)
                if (type(reply) is dict and reply.get("ok") is False
                        and reply.get("error") == "unsupported-value" and export_id is None):
                    raise ContractError("workspace artifact value is unsupported")
                try:
                    if type(reply) is not dict or reply.get("ok") is not True:
                        raise ValueError()
                    observed_id, observed_total = reply["export_id"], reply["byte_count"]
                    observed_digest = reply["content_digest"]
                    if (type(observed_id) is not int or observed_id < 1
                            or type(observed_total) is not int or observed_total < 0
                            or type(reply["offset"]) is not int or reply["offset"] != offset
                            or type(observed_digest) is not str or len(observed_digest) != 64
                            or any(char not in "0123456789abcdef" for char in observed_digest)):
                        raise ValueError()
                    if export_id is None:
                        if observed_id != reply.get("id"):
                            raise ValueError()
                        export_id, total, digest = observed_id, observed_total, observed_digest
                    elif (observed_id, observed_total, observed_digest) != (export_id, total, digest):
                        raise ValueError()
                    data = reply["data"]
                    if type(data) is not str or not data.isascii():
                        raise ValueError()
                    chunk = base64.b64decode(data.encode("ascii"), validate=True)
                    if (len(chunk) != min(CHUNK_SIZE, total - offset)
                            or base64.b64encode(chunk).decode("ascii") != data):
                        raise ValueError()
                    hasher.update(chunk)
                    content.write(chunk)
                    offset += len(chunk)
                    if offset == total:
                        if hasher.hexdigest() != digest:
                            raise ValueError()
                        return content.getvalue()
                except (KeyError, TypeError, ValueError, UnicodeError):
                    workspace._terminate_indeterminate()
                    raise WorkspaceUnavailable("Python workspace artifact reply is invalid") from None
                request = {"operation": "export_artifact", "export_id": export_id, "offset": offset}
