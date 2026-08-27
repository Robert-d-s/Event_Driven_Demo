"""
observer's consumer thread.

Binds observer.q to "#" so it receives a copy of every message published to the
orders exchange, and forwards each one to the dashboard via api.broadcast().

Note it acks immediately and never fails — observer must not slow the bus or
build a backlog. If the dashboard is down, messages are simply not shown; they're
not held up.
"""

from datetime import datetime, timezone

from pyevents import connect, consume, Message

from .api import broadcast

QUEUE = "observer.q"
EXCHANGE = "orders"


def run_consumer() -> None:
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="#")

    def handler(msg: Message) -> None:
        broadcast(
            {
                "seen_at": datetime.now(timezone.utc).isoformat(),
                "routing_key": msg.routing_key,
                "event_id": msg.event_id,
                "redelivered": msg.redelivered,
                "order_id": msg.body.get("order_id"),
                "body": msg.body,
            }
        )

    consume(ch, queue=QUEUE, handler=handler)
