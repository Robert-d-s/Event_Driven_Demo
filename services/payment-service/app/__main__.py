"""
payment-service — consumer only.

Listens for OrderPlaced, "charges" the customer, emits PaymentCaptured.

This service runs as 3 replicas (see docker-compose.yml). All three consume the
SAME queue, payment.q. RabbitMQ hands each message to exactly one of them. That
is a "work queue" / "competing consumers" — the way you scale throughput.

Contrast with notification-service, which has its own queue and therefore sees
*every* message. Same broker, same exchange; the only difference is whether the
consumers share a queue.

Single thread, one connection, blocking consume loop. No HTTP, so none of
order-service's threading dance is needed.
"""

import os
import socket
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, Message

QUEUE = "payment.q"
EXCHANGE = "orders"

# Identifies which replica handled a message, so the dashboard can show work
# being split across the three.
WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

# Stage 2 flips this from the dashboard to demonstrate retries. For stage 1 it
# stays off and every payment succeeds.
FAIL_PAYMENTS = os.environ.get("FAIL_PAYMENTS", "false").lower() == "true"


def main() -> None:
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="order.placed")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        amount = msg.body["total_cents"]
        print(
            f"[payment-service/{WORKER_ID}] charging order {order_id} "
            f"({amount} cents)",
            flush=True,
        )

        if FAIL_PAYMENTS:
            raise RuntimeError(f"payment declined for order {order_id} (simulated)")

        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="payment.captured",
            body={
                "event_id": str(uuid.uuid4()),
                "order_id": order_id,
                "amount_cents": amount,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
