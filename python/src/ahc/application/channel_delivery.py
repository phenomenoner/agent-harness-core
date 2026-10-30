"""One-shot delivery of an already durable, route-bound local outbox entry."""

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_presentation import typed_payload
from ahc.domain.sessions import ContractError


class ChannelDelivery:
    def __init__(self, outbox, trusted_route, transport):
        if type(trusted_route) is not ChannelRoute:
            raise ContractError("trusted channel route invalid")
        if (
            not callable(getattr(outbox, "claim", None))
            or not callable(getattr(outbox, "delivery_scope", None))
            or not callable(getattr(transport, "send", None))
        ):
            raise ContractError("channel delivery dependencies invalid")
        self.outbox, self.trusted_route, self.transport = outbox, trusted_route, transport

    def deliver(self, operation_id, token):
        scoped = getattr(self.outbox, "route_delivery_scope", None)
        with scoped(self.trusted_route) if callable(scoped) else self.outbox.delivery_scope():
            record = self.outbox.claim(self.trusted_route, operation_id, token)
            try:
                payload = record["payload"]
                if "message_type" in record:
                    payload = typed_payload(payload, record["message_type"])
                receipt = self.transport.send(self.trusted_route, payload, operation_id)
                return self.outbox.acknowledge(self.trusted_route, operation_id, token, receipt)
            except Exception:
                return self.outbox.mark_unknown(self.trusted_route, operation_id, token)
