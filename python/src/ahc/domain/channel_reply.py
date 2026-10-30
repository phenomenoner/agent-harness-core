"""A same-channel provider message identifier; never a destination selector."""

import re

from .sessions import ContractError


def reply_message_id(value):
    if type(value) is not str or not re.fullmatch(r"[1-9][0-9]{0,19}", value):
        raise ContractError("reply message identifier invalid")
    return value
