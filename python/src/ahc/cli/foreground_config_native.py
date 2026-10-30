"""Explicit Linux foreground configuration; never infer a platform from the host."""

from ahc.domain.sessions import ContractError
from .foreground_config_v2 import _parse_trusted, parse_foreground_config_v2

FOREGROUND_CONFIG_NATIVE_SCHEMA_VERSION = "ahc.foreground.config.v3"


def parse_foreground_config_native(value):
    return _parse_trusted(value, native=True)


def parse_trusted_foreground_config(value):
    if type(value) is not dict:
        raise ContractError("foreground configuration must be an object")
    if value.get("schema_version") == FOREGROUND_CONFIG_NATIVE_SCHEMA_VERSION:
        return parse_foreground_config_native(value)
    return parse_foreground_config_v2(value)
