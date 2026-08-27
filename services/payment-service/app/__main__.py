"""
payment-service — consumer only.

Listens for OrderPlaced, "charges" the customer, emits PaymentCaptured.

Runs as 3 replicas (docker-compose.yml). All three consume the SAME queue,
payment.q; RabbitMQ hands each message to exactly one of them — a "work queue" /
"competing consumers". Contrast notification-service, which has its own queue and
sees every message.

--- Stage 2 -------------------------------------------------------------------

`fail` is a runtime toggle flipped from the dashboard via the control channel.
When on, the handler raises — retry/DLQ machinery in pyevents.consume kicks in.

--- Stage 3: idempotency ----------------------------------------------------

Stage 2's redelivery means this handler WILL be handed the same OrderPlaced
twice — after a crash, a retry, or if the publisher duplicates it (the dashboard
can force that). Charging twice is a bug.

`pyevents.process_once` makes the second run a no-op: it INSERTs the event_id
into processed_events and does the charge in the SAME transaction. If the id is
already there, it's a duplicate — roll back, skip, ack. The dashboard's
`total charged` number makes this visible: idempotent → matches the sum of order
totals; not → drifts upward on every duplicate.

Two subtleties this handler has to get right:

1. The emitted PaymentCaptured uses a DETERMINISTIC event_id ("pay-<order_id>"),
   not a random one. So if this handler runs twice, downstream (inventory) sees
   the same id both times and dedupes it too. A fresh uuid each time would defeat
   the whole chain.

2. On a duplicate we still RE-PUBLISH PaymentCaptured. Why: the first run might
   have charged the card and then crashed before publishing. The redelivery hits
   "already processed", skips the charge (correct) — but must still emit the
   event or the order stalls forever. Re-publishing is safe because of (1).
   (Stage 4's outbox removes this hazard properly.)
"""

import os
import pathlib
import socket
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

QUEUE = "payment.q"
EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

_state = {"fail": os.environ.get("FAIL_PAYMENTS", "false").lower() == "true"}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":  # global — every service's publisher
        set_duplicate_mode(bool(command.get("value")))
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

        with process_once(db, msg.event_id) as first_time:
            if first_time:
                with db.cursor() as cur:
                    cur.execute(
                        "INSERT INTO payments (order_id, charged_cents) "
                        "VALUES (%s, %s) ON CONFLICT (order_id) DO NOTHING",
                        (order_id, amount),
                    )
                    cur.execute(
                        "UPDATE ledger SET total_charged_cents = "
                        "total_charged_cents + %s WHERE id = 1",
                        (amount,),
                    )
                print(
                    f"[payment-service/{WORKER_ID}] charged order {order_id} "
                    f"({amount} cents) [attempt {msg.attempt}]",
                    flush=True,
                )
            else:
                print(
                    f"[payment-service/{WORKER_ID}] duplicate OrderPlaced for "
                    f"order {order_id} (event_id={msg.event_id}) — not charging again",
                    flush=True,
                )
        # process_once has committed (charge + id together) or rolled back.

        # Published every time, with a deterministic id so inventory dedupes it.
        publish(
            ch,
            exchange=EXCHANGE,
            routing_key="payment.captured",
            body={
                "event_id": f"pay-{order_id}",
                "order_id": order_id,
                "amount_cents": amount,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
