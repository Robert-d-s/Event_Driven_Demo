"""
inventory-service — command handler (Stage 5).

  cmd.inventory.reserve  → reserve stock, then  reply.inventory.reserved
                                            (or reply.inventory.failed if OOS)
  cmd.inventory.release   → put the stock back, then  reply.inventory.released  (compensation)

Toggles from Stage 2 still work: `fail` (→ reply.inventory.failed) and `slow_ms`
(delay before processing, for the timeout demo — a slow-enough inventory makes
the orchestrator's watchdog fire).
"""

import os
import pathlib
import threading
import time

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

QUEUE = "inventory.cmd.q"
COMMANDS = "commands"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

_state = {
    "slow_ms": int(os.environ.get("SLOW_MS", "0")),
    "fail": os.environ.get("FAIL_INVENTORY", "false").lower() == "true",
    # "silent": do the reserve (hold the stock) but DON'T send the reply. That's
    # a genuinely unresponsive service — the orchestrator's watchdog has to time
    # it out, and the stock really is held, so the compensating release matters.
    "silent": False,
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
        print(f"[inventory] slow_ms -> {_state['slow_ms']}", flush=True)
    elif action == "fail":
        _state["fail"] = bool(command.get("value"))
        print(f"[inventory] fail -> {_state['fail']}", flush=True)
    elif action == "silent":
        _state["silent"] = bool(command.get("value"))
        print(f"[inventory] silent -> {_state['silent']}", flush=True)


def _reply(cur, order_id: int, routing_key: str, **extra) -> None:
    eid = f"{routing_key}:{order_id}"
    stage_event(
        cur,
        event_id=eid,
        routing_key=routing_key,
        body={"event_id": eid, "order_id": order_id, **extra},
    )


def main() -> None:
    listen_for_commands(_on_command, service="inventory")

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
        cmd = msg.routing_key
        sku, qty = "WIDGET-1", 1

        # The slow toggle only delays the FORWARD step (reserve). A compensating
        # release must stay fast — it's unwinding a doomed order, and if it were
        # also slow the orchestrator could never converge on a timed-out saga.
        if _state["slow_ms"] and cmd == "cmd.inventory.reserve":
            time.sleep(_state["slow_ms"] / 1000)

        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                return

            if cmd == "cmd.inventory.reserve":
                if _state["fail"]:
                    _reply(u.cur, order_id, "reply.inventory.failed", reason="out of stock")
                    print(f"[inventory] order {order_id}: reserve FAILED", flush=True)
                    return
                u.cur.execute(
                    "UPDATE stock SET qty = qty - %s WHERE sku = %s", (qty, sku)
                )
                u.cur.execute(
                    "INSERT INTO reservations (order_id, sku, qty) VALUES (%s, %s, %s) "
                    "ON CONFLICT (order_id) DO NOTHING",
                    (order_id, sku, qty),
                )
                if _state["silent"]:
                    # stock is held, but we send NO reply — the orchestrator will
                    # time this step out, and the compensating release undoes it.
                    print(
                        f"[inventory] order {order_id}: reserved {qty}x {sku} "
                        f"— NOT replying (silent mode)",
                        flush=True,
                    )
                else:
                    _reply(u.cur, order_id, "reply.inventory.reserved")
                    print(f"[inventory] order {order_id}: reserved {qty}x {sku}", flush=True)

            elif cmd == "cmd.inventory.release":
                # Compensation — put the stock back. Idempotent and safe even if
                # nothing was reserved: DELETE removes the row (or 0 rows), and
                # we add back exactly what we removed (or 0). So a release for an
                # order that was never reserved is a genuine no-op — which is
                # what lets the orchestrator fire release defensively on timeout.
                u.cur.execute(
                    "DELETE FROM reservations WHERE order_id = %s RETURNING qty",
                    (order_id,),
                )
                row = u.cur.fetchone()
                released = row[0] if row else 0
                if released:
                    u.cur.execute(
                        "UPDATE stock SET qty = qty + %s WHERE sku = %s",
                        (released, sku),
                    )
                _reply(u.cur, order_id, "reply.inventory.released", qty=released)
                print(f"[inventory] order {order_id}: released {released}x {sku}", flush=True)

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
