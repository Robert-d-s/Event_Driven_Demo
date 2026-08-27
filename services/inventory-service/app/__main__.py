"""
inventory-service — consumer only.

Listens for PaymentCaptured, reserves stock, emits StockReserved.
Single replica, its own queue (inventory.q).

--- Stage 2 ---------------------------------------------------------------------

Two runtime toggles from the dashboard:
  - `slow_ms`  — sleep before processing. With prefetch=1 this means the broker
                 stops handing inventory new messages until the current one is
                 done, so `inventory.q` visibly backs up. That backlog IS
                 backpressure — the queue absorbing a rate mismatch.
  - `fail`     — raise, to send messages down the retry/DLQ path.
"""

import os
import time
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, listen_for_commands, Message

QUEUE = "inventory.q"
EXCHANGE = "orders"

_state = {
    "slow_ms": int(os.environ.get("SLOW_MS", "0")),
    "fail": os.environ.get("FAIL_INVENTORY", "false").lower() == "true",
}


def _on_command(command: dict) -> None:
    if command.get("target") != "inventory":
        return
    action = command.get("action")
    if action == "slow_ms":
        _state["slow_ms"] = int(command.get("value") or 0)
        print(f"[inventory-service] slow_ms -> {_state['slow_ms']}", flush=True)
    elif action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[inventory-service] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="inventory")

    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="payment.captured")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        print(
            f"[inventory-service] reserving stock for order {order_id} "
            f"[attempt {msg.attempt}]",
            flush=True,
        )

        if _state["slow_ms"]:
            time.sleep(_state["slow_ms"] / 1000)

        if _state["fail"]:
            raise RuntimeError(f"stock system unavailable for order {order_id} (simulated)")

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
