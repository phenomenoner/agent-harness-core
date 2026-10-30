"""Kernel callback transport for an explicitly composed read-only batch."""

import json
import time
from ahc.domain.adaptive_time_budget import deadline_value

from ahc.application.tool_batch import BatchCall


def receive_reply(workspace, request, deadline):
    callbacks = 0
    lease = getattr(workspace, "_job_lease", None)
    parent_cancelled = getattr(workspace, "_cancelled", lambda: False)

    def cancelled():
        return parent_cancelled() or (lease is not None and lease.cancel_requested)

    def active_deadline():
        cap = deadline_value(deadline)
        if lease is not None and getattr(workspace, "_turn_budget", None) is None:
            cap = min(cap, lease.hard_deadline, lease.quiet_deadline)
        if cancelled() or time.monotonic() >= cap:
            raise ValueError("workspace callback expired")
        return cap

    while True:
        reply = workspace._receive(active_deadline if callable(deadline) else active_deadline())
        active_deadline()
        if type(reply) is not dict or "batch" not in reply:
            return reply
        batch = getattr(workspace, "_batch", None)
        if (
            set(reply) != {"id", "batch", "concurrency"}
            or type(reply["id"]) is not int
            or reply["id"] != workspace._sequence
            or request["operation"] != "execute"
            or batch is None
            or batch.session != workspace.identity.session
            or callbacks >= 4
            or type(reply["batch"]) is not list
            or not 1 <= len(reply["batch"]) <= 16
        ):
            raise ValueError("invalid workspace callback")
        callbacks += 1
        calls = []
        for row in reply["batch"]:
            if type(row) is not dict or set(row) != {
                "call_id", "capability_id", "resource_id", "arguments"
            }:
                raise ValueError("invalid workspace batch call")
            calls.append(BatchCall(row["call_id"], row["capability_id"], row["resource_id"],
                                   json.dumps(row["arguments"], allow_nan=False)))
        result = batch.run(batch.session, tuple(calls), deadline=active_deadline(),
                           cancelled=cancelled, concurrency=reply["concurrency"])
        payload = {"id": workspace._sequence, "batch_result": {
            **result.summary(), "values": result.values,
        }}
        if len(json.dumps(payload, allow_nan=False).encode()) + 1 > 65536:
            payload = {"id": workspace._sequence,
                       "batch_result": {"error": "batch-result-too-large"}}
        send = getattr(workspace, "_send", None)
        if send is None:
            workspace._transport.send(payload, deadline=active_deadline())
        else:
            send(payload, active_deadline())
