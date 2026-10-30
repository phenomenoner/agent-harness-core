"""Claim before upload and retain ambiguous outcomes without automatic replay."""

from ahc.domain.skill_values import canonical_json

from .channel_artifact_upload import prepare_artifact_upload


class ChannelAttachmentDelivery:
    def __init__(self, outbox, route, transport):
        self.outbox, self.route, self.transport = outbox, route, transport

    def deliver(self, operation_id, ordinal, token):
        scoped = getattr(self.outbox, "route_delivery_scope", None)
        with scoped(self.route) if callable(scoped) else self.outbox.delivery_scope():
            record = self.outbox.claim(self.route, operation_id, ordinal, token)
            try:
                selected = record["selection"]
                upload = prepare_artifact_upload(
                    self.outbox.store, self.route, selected["artifact_id"], selected["filename"],
                    delivery_mode=selected.get("delivery_mode", "document"),
                    caption=selected.get("caption", ""),
                    reply_to_message_id=record.get("reply_to_message_id"))
                receipt = self.transport.send(upload)
                return self.outbox.acknowledge(self.route, operation_id, ordinal, token,
                                               canonical_json(receipt).decode())
            except Exception:
                return self.outbox.mark_unknown(self.route, operation_id, ordinal, token)
