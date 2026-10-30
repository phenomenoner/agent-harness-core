"""Application seam for admitting an observed channel event into RuntimeInbox."""

from __future__ import annotations

from typing import Any

from ahc.domain.channel_ingress import (
    ChannelInboundEvent,
    ChannelRoute,
    channel_operation_id,
)
from ahc.domain.sessions import ContractError


class ChannelIngress:
    """Validate a trusted route before creating a durable local submission."""

    def __init__(self, inbox: Any, trusted_route: ChannelRoute) -> None:
        if type(trusted_route) is not ChannelRoute:
            raise ContractError("trusted channel route must be a ChannelRoute")
        if not callable(getattr(inbox, "submit", None)):
            raise ContractError("channel ingress requires an inbox submit method")
        self.inbox = inbox
        self.trusted_route = trusted_route

    def submit(self, event: ChannelInboundEvent, goal_digest: str) -> dict[str, str]:
        """Admit one route-bound event using the caller-provided trusted goal digest."""

        if type(event) is not ChannelInboundEvent:
            raise ContractError("channel event must be a ChannelInboundEvent")
        if event.route != self.trusted_route:
            raise ContractError("channel event route does not match trusted route")
        operation_id = channel_operation_id(self.trusted_route, event.event_id)
        return self.inbox.submit(
            self.trusted_route.session,
            operation_id,
            goal_digest,
            event.input_text,
            channel_event=event,
        )


__all__ = ["ChannelIngress"]
