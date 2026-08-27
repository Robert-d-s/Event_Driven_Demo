"""
order-service's consumer thread.

Listens for the orchestrator's TERMINAL events and updates the order's status:
  order.shipped   -> SHIPPED
  order.cancelled -> CANCELLED

order-service still owns the order row and stages `order.placed` (see api.py);
the orchestrator owns everything in between. These two events are how the
workflow's outcome gets back to the order.

Runs on its OWN pika + psycopg connections (created here, inside the thread).
Also hosts order-service's control-channel listener for the global toggles.
"""

import pathlib

from pyevents import (
    connect,
    connect_db,
    consume,
    listen_for_commands,
    process_once,
    run_script,
    set_duplicate_mode,
    set_relay_paused,
    Message,
)

QUEUE = "order.q"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":
        set_duplicate_mode(bool(command.get("value")))
    elif action == "pause_relay":
        set_relay_paused(bool(command.get("value")))


def run_consumer() -> None:
    listen_for_commands(_on_command, service="order")

    db = connect_db()  # consumer-thread-local
    run_script(db, SCHEMA)  # idempotent; api.py may or may not have run yet

    conn = connect()
    ch = conn.channel()

    # order.q (+ its .retry / .dlq) is declared by infra/topology.py before this
    # service starts.
    _STATUS = {"order.shipped": "SHIPPED", "order.cancelled": "CANCELLED"}

    def handler(msg: Message) -> None:
        status = _STATUS.get(msg.routing_key)
        if status is None:
            return
        order_id = msg.body["order_id"]
        with process_once(db, msg.event_id) as first_time:
            if not first_time:
                return
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET status = %s, updated_at = now() "
                    "WHERE order_id = %s",
                    (status, order_id),
                )
            reason = msg.body.get("reason")
            extra = f" ({reason})" if reason else ""
            print(f"[order-service] order {order_id} -> {status}{extra}", flush=True)

    consume(ch, queue=QUEUE, handler=handler)
