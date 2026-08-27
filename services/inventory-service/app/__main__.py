"""
inventory-service — consumer only.

Listens for PaymentCaptured, reserves stock, emits StockReserved.
Single replica, its own queue (inventory.q).

--- Stage 2 -------------------------------------------------------------------

Toggles: `slow_ms` (backpressure demo) and `fail` (retry/DLQ demo).

--- Stage 3: idempotency ----------------------------------------------------

Guarded stock decrement — a double-processed PaymentCaptured is a no-op.

--- Stage 4: the outbox ---------------------------------------------------

StockReserved is now written to the `outbox` table inside the same transaction
as the stock decrement (`handle_once` yields the cursor). One COMMIT:
processed_events + stock + reservations + outbox row. A relay thread drains the
outbox. No publish() call, no re-publish-on-duplicate hack.
"""

import os
import pathlib
import threading
import time
from datetime import datetime, timezone

from pyevents import (
    connect,
    connect_db,
    consume,
    handle_once,
    listen_for_commands,
    relay_loop,
    run_script,
    set_duplicate_mode,
    set_relay_paused,
    stage_event,
    DB_URL,
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
    if action == "duplicate":
        set_duplicate_mode(bool(command.get("value")))
        return
    if action == "pause_relay":
        set_relay_paused(bool(command.get("value")))
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

    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": EXCHANGE},
        name="outbox-relay", daemon=True,
    ).start()

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

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                print(
                    f"[inventory-service] duplicate PaymentCaptured for order "
                    f"{order_id} — not reserving again",
                    flush=True,
                )
                return

            u.cur.execute(
                "UPDATE stock SET qty = qty - %s WHERE sku = %s", (qty, sku)
            )
            u.cur.execute(
                "INSERT INTO reservations (order_id, sku, qty) VALUES (%s, %s, %s) "
                "ON CONFLICT (order_id) DO NOTHING",
                (order_id, sku, qty),
            )
            stage_event(
                u.cur,
                event_id=f"stock-{order_id}",
                routing_key="stock.reserved",
                body={
                    "event_id": f"stock-{order_id}",
                    "order_id": order_id,
                    "reserved_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            print(
                f"[inventory-service] reserved {qty}x {sku} for order {order_id} "
                f"+ staged StockReserved [attempt {msg.attempt}]",
                flush=True,
            )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
