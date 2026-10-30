"""Opt-in idle import: one queued event per step, failures retained without same-run retry."""

import os

from ahc.adapters.channel_file_download import BotFileDownload
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_file_import import ChannelFileImport, FileImportCancelled
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.working_repository import WorkingWriterBusy


def compose_files(config, store):
    document, bindings = config.to_dict(), {}
    for key, platform in (("telegram_polling", "telegram"), ("discord_gateway", "discord")):
        value = document.get(key, {})
        if not value.get("enabled") or not value.get("include_files", False):
            continue
        token = os.environ.get(value["token_env"])
        if token is None:
            raise ContractError("inbound file credential unavailable")
        for row in document.get("channels", []):
            route = ChannelRoute.from_dict(row["route"])
            if route.platform == platform and route.account_id == value["account_id"]:
                bindings[route] = BotFileDownload(route, token)
    if not bindings:
        return None
    failed = set()

    def process(workspace, *, cancelled):
        if cancelled():
            return
        if workspace is None or workspace.closed or workspace.store is not store:
            raise ContractError("inbound import workspace unavailable")
        for route, downloader in bindings.items():
            for event, goal in ChannelFileInbox(store).pending(route):
                if event.operation_id in failed:
                    continue
                if cancelled():
                    return
                try:
                    importer = ChannelFileImport(store)
                    importer.run(event, goal, workspace.identity, downloader, cancelled=cancelled)
                    if not cancelled():
                        importer.submit(event, goal)
                except FileImportCancelled:
                    return
                except WorkingWriterBusy:
                    return  # No effectful replay: downloads are reads; admission stayed atomic.
                except Exception:
                    from ahc.infrastructure.channel_file_failure import record_failure

                    record_failure(store, event)
                    failed.add(event.operation_id)
                return

    process.status = lambda: {"state": "degraded" if failed else "ready",
                              "failed_events": len(failed), "same_run_retry": False}
    return process
