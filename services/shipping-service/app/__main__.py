"""
shipping-service — command handler (Stage 5).

  cmd.shipping.dispatch  → dispatch, then  reply.shipping.dispatched
                                       (or reply.shipping.failed)

Shipping has no compensation — it's the last forward step. If it fails, the
orchestrator undoes everything *before* it (release stock, refund payment).

`fail` toggle (Stage 2) is how the demo triggers the interesting case: an order
that's already been charged and reserved, that now can't ship.
"""

import os
import pathlib
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

QUEUE = "shipping.cmd.q"
COMMANDS = "commands"
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
        print(f"[shipping] fail -> {_state['fail']}", flush=True)


def _reply(cur, order_id: int, routing_key: str, **extra) -> None:
    eid = f"{routing_key}:{order_id}"
    stage_event(
        cur,
        event_id=eid,
        routing_key=routing_key,
        body={"event_id": eid, "order_id": order_id, **extra},
    )


def main() -> None:
    listen_for_commands(_on_command, service="shipping")

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
        if msg.routing_key != "cmd.shipping.dispatch":
            return
        tracking = f"TRK-{order_id}"

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                return

            if _state["fail"]:
                _reply(u.cur, order_id, "reply.shipping.failed", reason="no courier")
                print(f"[shipping] order {order_id}: dispatch FAILED", flush=True)
                return

            u.cur.execute(
                "INSERT INTO shipments (order_id, tracking_code) VALUES (%s, %s) "
                "ON CONFLICT (order_id) DO NOTHING",
                (order_id, tracking),
            )
            _reply(u.cur, order_id, "reply.shipping.dispatched", tracking_code=tracking)
            print(f"[shipping] order {order_id}: dispatched ({tracking})", flush=True)

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
