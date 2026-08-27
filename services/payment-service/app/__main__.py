"""
payment-service — command handler.

--- Stages 1-4: choreography ------------------------------------------------

Consumed OrderPlaced off the "orders" topic exchange and emitted PaymentCaptured.
Nobody was in charge — the flow was the sum of every service's bindings.

--- Stage 5: orchestration -------------------------------------------------

Now consumes explicit COMMANDS from payment.cmd.q and sends back a REPLY:

  cmd.payment.charge  → charge, then  reply.payment.charged  (or reply.payment.failed)
  cmd.payment.refund  → refund, then  reply.payment.refunded    (compensation)

The orchestrator decides what happens next. payment-service just does its one
job and reports the outcome.

Everything from the earlier stages still holds: 3 replicas on one command queue
(competing consumers), handle_once for idempotency (a redelivered command must
not double-charge), and the outbox — the reply is staged in the same transaction
as the charge, so "charged but reply lost" can't happen.
"""

import os
import pathlib
import socket
import threading

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

QUEUE = "payment.cmd.q"
COMMANDS = "commands"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

_state = {"fail": os.environ.get("FAIL_PAYMENTS", "false").lower() == "true"}


def _on_command(command: dict) -> None:
    action = command.get("action")
    if action == "duplicate":
        set_duplicate_mode(bool(command.get("value")))
        return
    if action == "pause_relay":
        set_relay_paused(bool(command.get("value")))
        return
    if command.get("target") != "payment":
        return
    if action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[payment-service/{WORKER_ID}] fail -> {_state['fail']}", flush=True)


def _reply(cur, order_id: int, routing_key: str, **extra) -> None:
    eid = f"{routing_key}:{order_id}"
    stage_event(
        cur,
        event_id=eid,
        routing_key=routing_key,
        body={"event_id": eid, "order_id": order_id, **extra},
    )


def main() -> None:
    listen_for_commands(_on_command, service="payment")

    db = connect_db()
    run_script(db, SCHEMA)

    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": COMMANDS},
        name="outbox-relay", daemon=True,
    ).start()

    conn = connect()
    ch = conn.channel()

    def handler(msg: Message) -> None:
        order_id = msg.body["order_id"]
        amount = msg.body.get("amount_cents", 0)
        cmd = msg.routing_key

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                return

            if cmd == "cmd.payment.charge":
                if _state["fail"]:
                    _reply(u.cur, order_id, "reply.payment.failed", reason="declined")
                    print(f"[payment/{WORKER_ID}] order {order_id}: charge FAILED", flush=True)
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
                _reply(u.cur, order_id, "reply.payment.charged", amount_cents=amount)
                print(f"[payment/{WORKER_ID}] order {order_id}: charged {amount}c", flush=True)

            elif cmd == "cmd.payment.refund":
                # compensation — undo the charge
                u.cur.execute("DELETE FROM payments WHERE order_id = %s", (order_id,))
                u.cur.execute(
                    "UPDATE ledger SET total_charged_cents = total_charged_cents - %s "
                    "WHERE id = 1",
                    (amount,),
                )
                _reply(u.cur, order_id, "reply.payment.refunded", amount_cents=amount)
                print(f"[payment/{WORKER_ID}] order {order_id}: refunded {amount}c", flush=True)

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
