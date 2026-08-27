"""
payment-service — consumer only.

Listens for OrderPlaced, "charges" the customer, emits PaymentCaptured.

Runs as 3 replicas (docker-compose.yml). All three consume the SAME queue,
payment.q; RabbitMQ hands each message to exactly one of them — a "work queue" /
"competing consumers". Contrast notification-service, which has its own queue and
sees every message.

--- Stage 2 ---------------------------------------------------------------------

`fail` is a runtime toggle flipped from the dashboard via the control channel
(pyevents.listen_for_commands). When on, the handler raises — and you watch the
retry/DLQ machinery in pyevents.consume kick in: 3 attempts spaced by the retry
queue's 5s TTL, then the message lands in payment.q.dlq.

The control listener runs on its own thread with its own connection; the consume
loop is blocking on the main thread. They share one module-level dict — a single
bool write/read needs no lock in CPython.
"""

import os
import socket
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, listen_for_commands, Message

QUEUE = "payment.q"
EXCHANGE = "orders"

WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

# Runtime-toggloable behaviour. Starts from env (compose default), then the
# dashboard can flip it live.
_state = {
    "fail": os.environ.get("FAIL_PAYMENTS", "false").lower() == "true",
}


def _on_command(command: dict) -> None:
    if command.get("target") != "payment":
        return
    if command.get("action") == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[payment-service/{WORKER_ID}] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="payment")

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
            f"({amount} cents) [attempt {msg.attempt}]",
            flush=True,
        )

        if _state["fail"]:
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
