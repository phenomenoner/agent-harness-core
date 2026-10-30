"""Presence diagnostics disclose neither token values nor false authentication success."""

import json
from types import SimpleNamespace

import pytest

from ahc.cli.channel_diagnostics import inspect_channel_credentials


@pytest.mark.parametrize("token", [None, "", "private-token"])
def test_delivery_and_ingress_credentials_are_separate_and_sanitized(token):
    document = {"channels": [
        {"route": {"platform": "telegram"}, "token_env": "DELIVERY", "enabled": True}],
        "telegram_polling": {"enabled": True, "token_env": "INGRESS"},
        "discord_gateway": {"enabled": False, "token_env": "DISABLED"}}
    report = inspect_channel_credentials(SimpleNamespace(to_dict=lambda: document),
        environment={"DELIVERY": token, "INGRESS": "private-ingress"})
    rows = report["bindings"]
    assert rows[0]["status"] == ("configured-unverified" if token else "blocked")
    assert rows[1]["credential"] == "present-unverified"
    assert rows[2]["credential"] == "not-inspected"
    assert all(row["authentication_verified"] is False for row in rows)
    assert "private-" not in json.dumps(report)
    assert report["live_probe"] is False


def test_unconfigured_and_disabled_do_not_read_environment():
    class Forbidden(dict):
        def get(self, *args):
            pytest.fail("disabled binding inspected environment")

    config = SimpleNamespace(to_dict=lambda: {})
    report = inspect_channel_credentials(config, environment=Forbidden())
    assert report["status"] == "not-configured"
    config.to_dict = lambda: {"discord_gateway": {"enabled": False, "token_env": "SECRET"}}
    assert inspect_channel_credentials(config, environment=Forbidden())["bindings"][0][
        "status"] == "disabled"
