"""Bounded provider context, always data rather than command authority."""

import json

from .sessions import ContractError
from .skill_values import canonical_json


def message_context(value):
    if value is None:
        return None
    limits = {"message_id": 128, "reply_to_id": 128, "quote": 3000,
              "forwarded_text": 3000, "media_group_id": 128, "topic_id": 128,
              "notice": 500}
    allowed = set(limits) | {"forwarded", "quote_status", "attachments", "album_members"}
    if type(value) is not dict or set(value) - allowed:
        raise ContractError("channel message context invalid")
    for key, item in value.items():
        if key in limits:
            if (type(item) is not str or not 1 <= len(item) <= limits[key]
                    or any(0xD800 <= ord(c) <= 0xDFFF for c in item)):
                raise ContractError("channel message context text invalid")
        elif key == "album_members":
            if type(item) is not list or not 1 <= len(item) <= 10:
                raise ContractError("channel album context invalid")
            seen = set()
            for member in item:
                if (type(member) is not dict
                        or not {"message_id", "attachment_indexes"} <= set(member)
                        or set(member) - {"message_id", "attachment_indexes", "reply_to_id"}):
                    raise ContractError("channel album member invalid")
                for field in ("message_id", "reply_to_id"):
                    if field not in member:
                        continue
                    identifier = member[field]
                    if (type(identifier) is not str or not 1 <= len(identifier) <= 128
                            or not identifier.isascii() or not identifier.isdigit()
                            or not 0 < int(identifier)):
                        raise ContractError("channel album message invalid")
                indexes = member["attachment_indexes"]
                if (type(indexes) is not list or len(indexes) > 8
                        or any(type(index) is not int or not 0 <= index < 8 for index in indexes)
                        or len(set(indexes)) != len(indexes)
                        or member["message_id"] in seen):
                    raise ContractError("channel album attachment indexes invalid")
                seen.add(member["message_id"])
        elif key == "forwarded" and type(item) is not bool:
            raise ContractError("channel forward context invalid")
        elif key == "quote_status" and item not in ("available", "unavailable", "truncated"):
            raise ContractError("channel quote status invalid")
        elif key == "attachments":
            if type(item) is not list or len(item) > 8:
                raise ContractError("channel attachment context invalid")
            for entry in item:
                if (type(entry) is not dict
                        or set(entry) != {"filename", "media_type", "status"}
                        or any(type(v) is not str or not 1 <= len(v) <= 128
                               or any(0xD800 <= ord(c) <= 0xDFFF for c in v)
                               for v in entry.values())):
                    raise ContractError("channel attachment context invalid")
    encoded = canonical_json(value)
    if len(encoded.decode()) > 8000:
        raise ContractError("channel message context exceeds limit")
    return json.loads(encoded)


def needs_queued_context(value):
    return bool(value and (value.get("forwarded") is True or
                set(value) & {"reply_to_id", "quote", "forwarded_text", "attachments"}))
