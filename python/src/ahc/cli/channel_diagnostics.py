"""Configured channel credential presence; no authentication or delivery probe."""

import os


def inspect_channel_credentials(config, *, environment=None):
    environment = os.environ if environment is None else environment
    document = config.to_dict()
    entries = []

    def append(platform, direction, row):
        enabled = row["enabled"]
        present = bool(environment.get(row["token_env"])) if enabled else None
        entries.append({"platform": platform, "direction": direction,
            "status": ("disabled" if not enabled else
                       "configured-unverified" if present else "blocked"),
            "credential": ("not-inspected" if not enabled else
                           "present-unverified" if present else "missing-or-empty"),
            "authentication_verified": False})

    for row in document.get("channels", []):
        append(row["route"]["platform"], "delivery", row)
    for key, platform in (("telegram_polling", "telegram"), ("discord_gateway", "discord")):
        if key in document:
            append(platform, "ingress", document[key])
    return {"observation_scope": "configured-environment-presence", "live_probe": False,
            "status": ("not-configured" if not entries else
                       "blocked" if any(row["status"] == "blocked" for row in entries)
                       else "observed"), "bindings": entries}
