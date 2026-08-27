"""
order-service's consumer thread.

Listens for OrderShipped and marks the order SHIPPED in Postgres. That's the only
event order-service consumes — it's mostly a producer.

Runs on its OWN pika connection and its OWN psycopg connection (both created here,
inside the thread) so nothing is shared with the HTTP side.

Also hosts order-service's control-channel listener, for the global "duplicate
everything" toggle — order-service's Publisher needs to honour it too.
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
    def handler(msg: Message) -> None:
        if msg.routing_key != "order.shipped":
            return
        order_id = msg.body["order_id"]
        with process_once(db, msg.event_id) as first_time:
            if not first_time:
                print(
                    f"[order-service] duplicate OrderShipped for order {order_id} "
                    f"(event_id={msg.event_id}) — ignoring",
                    flush=True,
                )
                return
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET status = 'SHIPPED', updated_at = now() "
                    "WHERE order_id = %s",
                    (order_id,),
                )
            print(f"[order-service] order {order_id} -> SHIPPED", flush=True)

    consume(ch, queue=QUEUE, handler=handler)
