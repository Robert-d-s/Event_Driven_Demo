"""
orchestrator — owns each order's saga (state machine).

Consumes:
  - order.placed        (on the "orders" exchange) — the trigger
  - reply.#             (on the "commands" exchange) — every service's reply

Emits (all via the outbox, on the "commands" exchange):
  - cmd.payment.charge / cmd.inventory.reserve / cmd.shipping.dispatch  (forward)
  - cmd.payment.refund / cmd.inventory.release                          (compensating)
  - order.shipped / order.cancelled  (terminal — order-service closes the order)

Also runs a **timeout watchdog** thread: if a step has been awaiting a reply for
longer than STEP_TIMEOUT_S, it's treated as a failure and compensation begins.
That's what stops a silently-dead service from hanging an order forever.

Everything is idempotent (replies get redelivered) and transactional (the state
change + the next command's outbox row commit together).
"""

import os
import pathlib
import threading
import time
import uuid
from datetime import datetime, timezone

from pyevents import (
    connect,
    connect_db,
    consume,
    handle_once,
    listen_for_commands,
    relay_loop,
    run_script,
    set_relay_paused,
    stage_event,
    DB_URL,
    Message,
)
from pyevents import saga
from pyevents.saga import (
    STEPS,
    STEP_COMMAND,
    STEP_OK,
    STEP_FAIL,
    STEP_COMPENSATE,
    COMP_OK,
)

COMMANDS = "commands"
EVENTS = "orders"
QUEUE = "orchestrator.q"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

STEP_TIMEOUT_S = int(os.environ.get("STEP_TIMEOUT_S", "12"))


def _on_command(command: dict) -> None:
    if command.get("action") == "pause_relay":
        set_relay_paused(bool(command.get("value")))


# ---------------------------------------------------------------------------
# Emitting commands / terminal events. All go through the outbox on the cursor
# the caller already holds, so they commit with the state change.
# ---------------------------------------------------------------------------

def _emit(cur, order_id: int, routing_key: str, body: dict, *, suffix: str = "") -> None:
    # event_id is deterministic so services dedupe — but a defensive re-release
    # needs a DISTINCT id or the receiving service would dedupe it as the first
    # release. `suffix` gives it one.
    eid = f"{routing_key}:{order_id}{suffix}"
    stage_event(
        cur,
        event_id=eid,
        routing_key=routing_key,
        body={"event_id": eid, "order_id": order_id, **body},
    )
    saga.log(cur, order_id, "command", routing_key + (f" ({suffix.lstrip('.')})" if suffix else ""))


def _send_step(cur, order_id: int, step: str, total_cents: int) -> None:
    """Send the forward command for `step` and mark the saga awaiting its reply."""
    _emit(cur, order_id, STEP_COMMAND[step], {"amount_cents": total_cents})
    state = {"charge": "STARTED", "reserve": "CHARGED", "dispatch": "RESERVED"}[step]
    saga.set_state(cur, order_id, state, awaiting=step)


def _begin_compensation(
    cur,
    order_id: int,
    failed_step: str,
    total_cents: int,
    *,
    include_failed_step: bool,
) -> None:
    """
    A step failed or timed out. Send compensating commands for the completed
    forward steps, in reverse order.

    `include_failed_step`:
      - False (a real reply.*.failed): the step definitely did NOT happen — the
        service told us so. Compensate only the steps *before* it.
      - True (a timeout): "no reply" does NOT mean "didn't happen" — the service
        might just be slow, and could still apply the change and reply late. So
        compensate the failed step too. Every compensating handler is a safe
        no-op if the forward action never happened (e.g. inventory.release
        deletes 0 rows and adds back 0 stock).
    """
    up_to = STEPS.index(failed_step) + (1 if include_failed_step else 0)
    to_undo = STEPS[:up_to]
    comps = [STEP_COMPENSATE[s] for s in reversed(to_undo) if s in STEP_COMPENSATE]

    if not comps:
        saga.set_state(cur, order_id, "CANCELLING", awaiting=None, comp_pending=0)
        _finish_cancelled(cur, order_id, reason=f"{failed_step} failed")
        return

    saga.set_state(
        cur, order_id, "COMPENSATING", awaiting=None, comp_pending=len(comps)
    )
    saga.log(cur, order_id, "state", f"compensating: {comps}")
    for rk in comps:
        _emit(cur, order_id, rk, {"amount_cents": total_cents})


def _finish_completed(cur, order_id: int) -> None:
    saga.set_state(cur, order_id, "COMPLETED", awaiting=None)
    _emit(cur, order_id, "order.shipped", {"final": True})


def _finish_cancelled(cur, order_id: int, *, reason: str) -> None:
    saga.set_state(cur, order_id, "CANCELLED", awaiting=None)
    _emit(cur, order_id, "order.cancelled", {"reason": reason})


# ---------------------------------------------------------------------------
# Handling incoming messages
# ---------------------------------------------------------------------------

def _handle_order_placed(cur, msg: Message) -> None:
    order_id = msg.body["order_id"]
    total = msg.body["total_cents"]
    if saga.load(cur, order_id) is not None:
        return  # already running (redelivered trigger)
    saga.create(cur, order_id, total)
    saga.log(cur, order_id, "state", "saga started")
    _send_step(cur, order_id, "charge", total)


def _handle_reply(cur, msg: Message) -> None:
    order_id = msg.body["order_id"]
    rk = msg.routing_key
    s = saga.load(cur, order_id)
    if s is None:
        return
    saga.log(cur, order_id, "reply", rk)

    # --- compensation replies ------------------------------------------
    # Count them down; when the last compensating reply arrives, cancel.
    if s.state == "COMPENSATING":
        if rk in COMP_OK.values():
            remaining = saga.dec_comp_pending(cur, order_id)
            if remaining <= 0:
                _finish_cancelled(cur, order_id, reason="compensated")
        elif rk == "reply.inventory.reserved":
            # A slow inventory reserved stock AFTER we timed it out and started
            # compensating. Release it again with a distinct event_id so
            # inventory doesn't dedupe it as the first release.
            saga.log(cur, order_id, "state", "late reserve during compensation — re-releasing")
            _emit(
                cur, order_id, "cmd.inventory.release",
                {"amount_cents": s.total_cents}, suffix=".late",
            )
        return

    if s.state in ("CANCELLED", "COMPLETED", "CANCELLING"):
        if rk == "reply.inventory.reserved":
            saga.log(cur, order_id, "state", "late reserve after cancel — re-releasing")
            _emit(
                cur, order_id, "cmd.inventory.release",
                {"amount_cents": s.total_cents}, suffix=".late",
            )
        return

    # --- forward replies ---------------------------------------------
    if s.awaiting is None:
        return
    step = s.awaiting
    if rk == STEP_OK[step]:
        idx = STEPS.index(step)
        if idx + 1 < len(STEPS):
            _send_step(cur, order_id, STEPS[idx + 1], s.total_cents)
        else:
            _finish_completed(cur, order_id)
    elif rk == STEP_FAIL[step]:
        # a real "failed" reply — the service confirmed the step did not happen
        _begin_compensation(
            cur, order_id, step, s.total_cents, include_failed_step=False
        )


def _timeout_watchdog(dsn: str) -> None:
    """Periodically: any saga awaiting a reply for > STEP_TIMEOUT_S → treat as failed."""
    db = connect_db()
    while True:
        time.sleep(3)
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT order_id, awaiting, total_cents FROM sagas "
                    "WHERE awaiting IS NOT NULL "
                    "AND step_started < now() - make_interval(secs => %s) "
                    "FOR UPDATE SKIP LOCKED",
                    (STEP_TIMEOUT_S,),
                )
                stuck = cur.fetchall()
                for order_id, step, total in stuck:
                    saga.log(cur, order_id, "timeout", f"{step} timed out after {STEP_TIMEOUT_S}s")
                    # a timeout is "unknown", not "didn't happen" — compensate the
                    # timed-out step too, in case the slow service applies it later
                    _begin_compensation(
                        cur, order_id, step, total, include_failed_step=True
                    )
            db.commit()
        except Exception as err:  # noqa: BLE001
            db.rollback()
            print(f"[orchestrator] watchdog error: {err!r}", flush=True)


def main() -> None:
    listen_for_commands(_on_command, service="orchestrator")

    db = connect_db()
    run_script(db, saga.SAGA_DDL)

    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": COMMANDS},
        name="outbox-relay", daemon=True,
    ).start()
    threading.Thread(
        target=_timeout_watchdog, args=(DB_URL,), name="watchdog", daemon=True,
    ).start()

    conn = connect()
    ch = conn.channel()

    def handler(msg: Message) -> None:
        with handle_once(db, msg.event_id) as u:
            if not u.first_time:
                return
            if msg.routing_key == "order.placed":
                _handle_order_placed(u.cur, msg)
            elif msg.routing_key.startswith("reply."):
                _handle_reply(u.cur, msg)

    consume(ch, queue=QUEUE, handler=handler)


if __name__ == "__main__":
    main()
