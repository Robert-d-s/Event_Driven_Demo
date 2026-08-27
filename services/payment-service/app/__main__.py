"""
payment-service — consumer only.

Listens for OrderPlaced, "charges" the customer, emits PaymentCaptured.

Runs as 3 replicas (docker-compose.yml). All three consume the SAME queue,
payment.q — a "work queue" / "competing consumers".

--- Stage 2 -------------------------------------------------------------------

`fail` toggle from the dashboard → the handler raises → retry/DLQ machinery.

--- Stage 3: idempotency ----------------------------------------------------

Redelivery means this handler runs twice for the same OrderPlaced. The
idempotency check makes the second run a no-op.

--- Stage 4: the outbox ---------------------------------------------------

Before: handler did `UPDATE payments` (COMMIT), then `publish(PaymentCaptured)`.
Two systems, one gap. A crash between them = charged but nobody downstream hears.
Stage 3 covered it with a "re-publish on duplicate" hack in every handler.

Now: the handler writes PaymentCaptured to the `outbox` TABLE inside the same
transaction as the charge (`handle_once` yields the cursor for exactly this).
One COMMIT: processed_events + payments + outbox row, atomically. A relay thread
(pyevents.relay_loop) drains the outbox to RabbitMQ.

No publish() call in this file any more. No re-publish hack. The gap is gone —
the event is committed with the work or not at all.

Each replica runs its own relay against the shared payment_db. They don't collide
because the relay claims rows with `FOR UPDATE SKIP LOCKED` (see pyevents.outbox).
"""

import os
import pathlib
import socket
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

QUEUE = "payment.q"
EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

_state = {"fail": os.environ.get("FAIL_PAYMENTS", "false").lower() == "true"}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":
        set_duplicate_mode(bool(command.get("value")))
        return
    if action == "pause_relay":  # global — every service's outbox relay
        set_relay_paused(bool(command.get("value")))
        return
    if command.get("target") != "payment":
        return
    if action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[payment-service/{WORKER_ID}] fail -> {_state['fail']}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="payment")

    db = connect_db()
    run_script(db, SCHEMA)

    # Relay: drains payment_db.outbox -> RabbitMQ. Own thread, own connections.
    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": EXCHANGE},
        name="outbox-relay", daemon=True,
    ).start()

    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="order.placed")

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        amount = msg.body["total_cents"]

        if _state["fail"]:
            raise RuntimeError(f"payment declined for order {order_id} (simulated)")

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                print(
                    f"[payment-service/{WORKER_ID}] duplicate OrderPlaced for "
                    f"order {order_id} — skipping",
                    flush=True,
                )
                return

            u.cur.execute(
                "INSERT INTO payments (order_id, charged_cents) VALUES (%s, %s) "
                "ON CONFLICT (order_id) DO NOTHING",
                (order_id, amount),
            )
            u.cur.execute(
                "UPDATE ledger SET total_charged_cents = total_charged_cents + %s "
                "WHERE id = 1",
                (amount,),
            )
            stage_event(
                u.cur,
                event_id=f"pay-{order_id}",
                routing_key="payment.captured",
                body={
                    "event_id": f"pay-{order_id}",
                    "order_id": order_id,
                    "amount_cents": amount,
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            print(
                f"[payment-service/{WORKER_ID}] charged order {order_id} "
                f"({amount} cents) + staged PaymentCaptured [attempt {msg.attempt}]",
                flush=True,
            )
        # ONE commit above: processed_events + payments + ledger + outbox row.
        # No publish() here. The relay thread drains the outbox — and if it's
        # paused or this process is killed right now, the PaymentCaptured row is
        # already durably on disk and goes out when the relay next runs.

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
