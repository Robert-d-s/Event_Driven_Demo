"""
The consume loop.

This is where the "at least once" guarantee comes from. A message is only
removed from the queue when the consumer *acknowledges* it (basic_ack). If the
consumer crashes before acking, the broker gives the message to someone else.

In this stage the handler either succeeds (we ack) or raises (we nack). Stage 2
replaces the bare nack with a retry/dead-letter topology; stage 3 makes the
handler safe to run twice. For now: ack on success, nack-without-requeue on
failure so a broken message doesn't spin forever.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable

from .connection import Channel


@dataclass
class Message:
    """What a handler receives. The raw AMQP details stay out of the handler."""

    routing_key: str
    body: dict
    event_id: str
    redelivered: bool  # True if the broker has handed us this message before


Handler = Callable[[Message], None]


def consume(
    channel: Channel,
    *,
    queue: str,
    handler: Handler,
    prefetch: int = 1,
) -> None:
    """
    Consume `queue` forever, calling `handler` for each message.

    prefetch=1 (basic_qos) means the broker will not send this consumer a second
    message until the first is acked. That is what makes a work queue with
    several replicas distribute fairly — a slow consumer simply gets fewer
    messages, instead of having a pile dumped on it up front. Try setting it to
    50 and watch one replica hog everything.
    """
    channel.basic_qos(prefetch_count=prefetch)

    def _on_message(ch, method, properties, raw: bytes) -> None:
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            # A message that will never parse. Requeuing it achieves nothing.
            # Stage 2 routes these to a dead-letter queue; for now, drop it.
            print(f"[consume:{queue}] poison message, dropping: {raw!r}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        msg = Message(
            routing_key=method.routing_key,
            body=body,
            event_id=str(body.get("event_id", properties.message_id or "")),
            redelivered=bool(method.redelivered),
        )
        try:
            handler(msg)
        except Exception as err:  # noqa: BLE001 — handler decides what's fatal
            print(
                f"[consume:{queue}] handler raised on {msg.routing_key} "
                f"(event_id={msg.event_id}): {err!r}",
                flush=True,
            )
            # requeue=False so a permanently-failing message doesn't loop.
            # Stage 2 makes this a real retry with backoff.
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue, on_message_callback=_on_message)
    print(f"[consume:{queue}] waiting for messages (prefetch={prefetch})", flush=True)
    channel.start_consuming()
