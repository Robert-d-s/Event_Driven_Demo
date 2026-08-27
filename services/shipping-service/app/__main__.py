"""
shipping-service — consumer only.

Listens for StockReserved, dispatches the order, emits OrderShipped.
OrderShipped is what order-service listens for to close the loop.

--- Stage 2 -------------------------------------------------------------------

`fail` toggle from the dashboard. On → 3 retries then shipping.q.dlq. (Stage 5
reuses this to demonstrate compensation.)

--- Stage 3: idempotency ----------------------------------------------------

`process_once` guards the shipment. A double-processed StockReserved would
otherwise create a second OrderShipped, and order-service would mark the order
shipped twice. The tracking code is derived from the order id so a re-publish on
a duplicate carries the same OrderShipped (deterministic event_id "ship-<id>").
"""

import os
import pathlib
from datetime import datetime, timezone

from pyevents import (
    connect,
    connect_db,
    consume,
    listen_for_commands,
    process_once,
    publish,
    run_script,
    set_duplicate_mode,
    Message,
)

QUEUE = "shipping.q"
EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

_state = {"fail": os.environ.get("FAIL_SHIPPING", "false").lower() == "true"}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":  # global
        set_duplicate_mode(bool(command.get("value")))
        return
    if command.get("target") != "shipping":
        return
    if action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[shipping-service] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="shipping")

    db = connect_db()
    run_script(db, SCHEMA)

    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="stock.reserved")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        tracking = f"TRK-{order_id}"

        if _state["fail"]:
            raise RuntimeError(f"no courier available for order {order_id} (simulated)")

        with process_once(db, msg.event_id) as first_time:
            if first_time:
                with db.cursor() as cur:
                    cur.execute(
                        "INSERT INTO shipments (order_id, tracking_code) "
                        "VALUES (%s, %s) ON CONFLICT (order_id) DO NOTHING",
                        (order_id, tracking),
                    )
                print(
                    f"[shipping-service] dispatched order {order_id} ({tracking}) "
                    f"[attempt {msg.attempt}]",
                    flush=True,
                )
            else:
                print(
                    f"[shipping-service] duplicate StockReserved for order "
                    f"{order_id} (event_id={msg.event_id}) — not shipping again",
                    flush=True,
                )

        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="order.shipped",
            body={
                "event_id": f"ship-{order_id}",
                "order_id": order_id,
                "tracking_code": tracking,
                "shipped_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
