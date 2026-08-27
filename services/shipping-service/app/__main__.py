"""
shipping-service — consumer only.

Listens for StockReserved, dispatches the order, emits OrderShipped.
OrderShipped is what order-service listens for to close the loop — the only
feedback the original producer gets, arriving seconds later on a different queue.

--- Stage 2 ---------------------------------------------------------------------

`fail` toggle from the dashboard. Turn it on and shipping's messages go through
3 retries then to shipping.q.dlq. (Stage 5 reuses this same failure to
demonstrate compensation — an order that was already charged + reserved and now
can't ship has to be unwound.)
"""

import os
import uuid
from datetime import datetime, timezone

from pyevents import connect, consume, publish, listen_for_commands, Message

QUEUE = "shipping.q"
EXCHANGE = "orders"

_state = {
    "fail": os.environ.get("FAIL_SHIPPING", "false").lower() == "true",
}


def _on_command(command: dict) -> None:
    if command.get("target") != "shipping":
        return
    if command.get("action") == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[shipping-service] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="shipping")

    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="stock.reserved")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        print(
            f"[shipping-service] dispatching order {order_id} [attempt {msg.attempt}]",
            flush=True,
        )

        if _state["fail"]:
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
