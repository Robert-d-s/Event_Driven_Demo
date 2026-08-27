"""
inventory-service — consumer only.

Listens for PaymentCaptured, reserves stock, emits StockReserved.

Single replica, its own queue (inventory.q). One thread, blocking consume loop.
"""

import os
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, Message

QUEUE = "inventory.q"
EXCHANGE = "orders"

# Stage 2 uses this to demonstrate a slow consumer and prefetch/backpressure.
SLOW_MS = int(os.environ.get("SLOW_MS", "0"))


def main() -> None:
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="payment.captured")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        print(f"[inventory-service] reserving stock for order {order_id}", flush=True)

        if SLOW_MS:
            import time

            time.sleep(SLOW_MS / 1000)

        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="stock.reserved",
            body={
                "event_id": str(uuid.uuid4()),
                "order_id": order_id,
                "reserved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
