"""
inventory-service — consumer only.

Listens for PaymentCaptured, reserves stock, emits StockReserved.
Single replica, its own queue (inventory.q).

--- Stage 2 -------------------------------------------------------------------

Two runtime toggles from the dashboard:
  - `slow_ms`  — sleep before processing. With prefetch=1 the broker stops
                 handing inventory new messages until the current one is done, so
                 `inventory.q` visibly backs up: backpressure.
  - `fail`     — raise, to send messages down the retry/DLQ path.

--- Stage 3: idempotency ----------------------------------------------------

`process_once` guards the stock decrement. A double-processed PaymentCaptured
would reserve stock twice — `stock.qty` on the dashboard drops by 2 instead of 1.
With the guard, the second delivery is a no-op.

The emitted StockReserved uses a deterministic event_id ("stock-<order_id>") so
shipping-service dedupes it too, and it's re-published on a duplicate so the
order can't stall.
"""

import os
import pathlib
import time
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

QUEUE = "inventory.q"
EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

_state = {
    "slow_ms": int(os.environ.get("SLOW_MS", "0")),
    "fail": os.environ.get("FAIL_INVENTORY", "false").lower() == "true",
}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":  # global
        set_duplicate_mode(bool(command.get("value")))
        return
    if command.get("target") != "inventory":
        return
    if action == "slow_ms":
        _state["slow_ms"] = int(command.get("value") or 0)
        print(f"[inventory-service] slow_ms -> {_state['slow_ms']}", flush=True)
    elif action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[inventory-service] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="inventory")

    db = connect_db()
    run_script(db, SCHEMA)

    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="payment.captured")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        sku, qty = "WIDGET-1", 1

        if _state["slow_ms"]:
            time.sleep(_state["slow_ms"] / 1000)

        if _state["fail"]:
            raise RuntimeError(f"stock system unavailable for order {order_id} (simulated)")

        with process_once(db, msg.event_id) as first_time:
            if first_time:
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE stock SET qty = qty - %s WHERE sku = %s",
                        (qty, sku),
                    )
                    cur.execute(
                        "INSERT INTO reservations (order_id, sku, qty) "
                        "VALUES (%s, %s, %s) ON CONFLICT (order_id) DO NOTHING",
                        (order_id, sku, qty),
                    )
                print(
                    f"[inventory-service] reserved {qty}x {sku} for order {order_id} "
                    f"[attempt {msg.attempt}]",
                    flush=True,
                )
            else:
                print(
                    f"[inventory-service] duplicate PaymentCaptured for order "
                    f"{order_id} (event_id={msg.event_id}) — not reserving again",
                    flush=True,
                )

        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="stock.reserved",
            body={
                "event_id": f"stock-{order_id}",
                "order_id": order_id,
                "reserved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
