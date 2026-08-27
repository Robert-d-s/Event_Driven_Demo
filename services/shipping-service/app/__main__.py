"""
shipping-service — consumer only.

Listens for StockReserved, dispatches the order, emits OrderShipped.
OrderShipped is what order-service listens for to close the loop.

--- Stage 2 -------------------------------------------------------------------

`fail` toggle → 3 retries then shipping.q.dlq. (Stage 5 reuses this for
compensation.)

--- Stage 3: idempotency ----------------------------------------------------

Guarded shipment — a double-processed StockReserved is a no-op.

--- Stage 4: the outbox ---------------------------------------------------

OrderShipped is written to the `outbox` table in the same transaction as the
shipment insert. One COMMIT; a relay thread drains it. No publish() call.
"""

import os
import pathlib
import threading
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

QUEUE = "shipping.q"
EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

_state = {"fail": os.environ.get("FAIL_SHIPPING", "false").lower() == "true"}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":
        set_duplicate_mode(bool(command.get("value")))
        return
    if action == "pause_relay":
        set_relay_paused(bool(command.get("value")))
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

    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": EXCHANGE},
        name="outbox-relay", daemon=True,
    ).start()

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

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                print(
                    f"[shipping-service] duplicate StockReserved for order "
                    f"{order_id} — not shipping again",
                    flush=True,
                )
                return

            u.cur.execute(
                "INSERT INTO shipments (order_id, tracking_code) VALUES (%s, %s) "
                "ON CONFLICT (order_id) DO NOTHING",
                (order_id, tracking),
            )
            stage_event(
                u.cur,
                event_id=f"ship-{order_id}",
                routing_key="order.shipped",
                body={
                    "event_id": f"ship-{order_id}",
                    "order_id": order_id,
                    "tracking_code": tracking,
                    "shipped_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            print(
                f"[shipping-service] dispatched order {order_id} ({tracking}) "
                f"+ staged OrderShipped [attempt {msg.attempt}]",
                flush=True,
            )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
