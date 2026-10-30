"""Single background channel receiver keeps operator controls responsive during turns."""

import os
import threading

from ahc.adapters.channel_control_ui import BotControlTransport
from ahc.adapters.channel_text import BotTextTransport
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.working_repository import WorkingWriterBusy


class ChannelControlPump:
    workspace_independent = True
    def __init__(self, controls, telegram, discord):
        self.controls, self.telegram, self.discord = controls, telegram, discord
        self.stop = threading.Event()
        self.thread = None
        self.failed = False
        self.paused = False
        self.failure_trace = None
        self._contention_failures = 0
        self.transports = {}
        for row in controls.document.get("channels", []):
            if row["enabled"]:
                route = ChannelRoute.from_dict(row["route"])
                transport = (BotControlTransport if controls.options.get("native_ui", False)
                             else BotTextTransport)
                self.transports[route] = transport(route, os.environ.get(row["token_env"]))

    def start(self):
        if self.thread is not None:
            raise ContractError("channel control pump already started")
        if self.discord is not None:
            self.discord.start()
        self.thread = threading.Thread(target=self._run, name="ahc-channel-controls", daemon=True)
        self.thread.start()

    def _run(self):
        try:
            while not self.stop.is_set():
                try:
                    self.controls.deliver_one(self.transports)
                    if self.telegram is not None:
                        self.telegram(cancelled=self.stop.is_set)
                except WorkingWriterBusy:
                    # One attempt per normal pump cycle. Durable sending rows
                    # fence uncertain deliveries; contention is not a permanent
                    # receiver failure and never authorizes replay of a send.
                    self._contention_failures = 1
                else:
                    self._contention_failures = 0
                self.stop.wait(0.1)
        except Exception as error:
            from ahc.application.failure_trace import failure_trace

            self.failure_trace = failure_trace(error)
            self.failed = True
            self.stop.set()

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(15)
            if self.thread.is_alive():
                raise ContractError("channel control pump cleanup unverified")
        if self.discord is not None:
            self.discord.close()

    def status(self):
        state = "new"
        if self.thread:
            state = "running"
        if self.paused:
            state = "paused"
        if self.failed:
            state = "failed"
        return {"state": state,
                "paused": self.paused,
                "telegram": None if self.telegram is None else self.telegram.status(),
                "discord": None if self.discord is None else self.discord.status()}
