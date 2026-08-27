"""
shipping-service — consumer only.

Listens for StockReserved, dispatches the order, emits OrderShipped.

OrderShipped is the event order-service listens for to close the loop — that's
the only feedback the original producer gets, and it arrives seconds later on a
different queue.
"""

import os
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, Message

QUEUE = "shipping.q"
EXCHANGE = "orders"

# Stage 2 / stage 5 flip this to demonstrate failure + compensation.
FAIL_SHIPPING = os.environ.get("FAIL_SHIPPING", "false").lower() == "true"


def main() -> None:
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="stock.reserved")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        print(f"[shipping-service] dispatching order {order_id}", flush=True)

        if FAIL_SHIPPING:
            raise RuntimeError(f"no courier available for order {order_id} (simulated)")

        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="order.shipped",
            body={
                "event_id": str(uuid.uuid4()),
                "order_id": order_id,
                "tracking_code": f"TRK-{order_id}-{uuid.uuid4().hex[:6].upper()}",
                "shipped_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
