"""
Saga state — the orchestrator's memory of where each order is.

--- Why an orchestrator at all --------------------------------------------

Stages 1-4 were *choreography*: order-service emits OrderPlaced, payment reacts
and emits PaymentCaptured, inventory reacts... nobody is in charge. Beautifully
decoupled, and it has a cost you should feel before anyone hands you a fix: when
an order is stuck, no single service can tell you where. The flow exists only as
the sum of everyone's queue bindings.

And when a *later* step fails — shipping can't dispatch an order that's already
been charged and had stock reserved — there is no transaction to roll back across
three services. You have to **undo forward**: refund the payment, release the
stock, with explicit compensating actions in reverse order. The moment you're
writing that, the missing owner is obvious.

Stage 5 adds one: the `orchestrator`. It holds each order's state machine, sends
explicit COMMANDS ("cmd.payment.charge") instead of hoping someone reacts, waits
for a REPLY, advances or compensates. This reintroduces a bit of central coupling
— that's the actual trade, and why the earlier stages are choreographed first.

--- The state machine ---------------------------------------------------

    STARTED ──charge ok──▶ CHARGED ──reserve ok──▶ RESERVED ──dispatch ok──▶ COMPLETED
       │                     │                        │
       │ charge fails        │ reserve fails          │ dispatch fails / times out
       ▼                     ▼                        ▼
    CANCELLED          COMPENSATING ────────────▶ COMPENSATING
                       (refund)                   (release stock, then refund)
                            │                        │
                            ▼                        ▼
                        CANCELLED                CANCELLED

Every state transition and every command/reply is written to `saga_log` — the
audit trail the dashboard renders and a human reads when something is wrong.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterator

import psycopg

SAGA_DDL = """
CREATE TABLE IF NOT EXISTS sagas (
    order_id     BIGINT PRIMARY KEY,
    state        TEXT NOT NULL,
    total_cents  BIGINT NOT NULL,
    -- which forward step we're waiting on a reply for; NULL when not waiting
    awaiting     TEXT,
    -- how many compensating replies we're still waiting for (COMPENSATING state)
    comp_pending INTEGER NOT NULL DEFAULT 0,
    -- when the current step was started, for the timeout watchdog
    step_started TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS saga_log (
    id        BIGSERIAL PRIMARY KEY,
    order_id  BIGINT NOT NULL,
    at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind      TEXT NOT NULL,   -- 'state' | 'command' | 'reply' | 'timeout'
    detail    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS saga_log_order ON saga_log (order_id, id);

-- idempotency for the orchestrator itself (it's a consumer too)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
""" + """
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    body         JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
"""

# Forward steps, in order. Each maps to a command routing key and the reply
# routing keys that mean success / failure.
STEPS = ["charge", "reserve", "dispatch"]

STEP_COMMAND = {
    "charge": "cmd.payment.charge",
    "reserve": "cmd.inventory.reserve",
    "dispatch": "cmd.shipping.dispatch",
}
STEP_OK = {
    "charge": "reply.payment.charged",
    "reserve": "reply.inventory.reserved",
    "dispatch": "reply.shipping.dispatched",
}
STEP_FAIL = {
    "charge": "reply.payment.failed",
    "reserve": "reply.inventory.failed",
    "dispatch": "reply.shipping.failed",
}

# Compensating command for each forward step that might need undoing.
STEP_COMPENSATE = {
    "charge": "cmd.payment.refund",
    "reserve": "cmd.inventory.release",
    # dispatch has no compensation — if it failed, nothing to undo for it
}

# The reply that confirms a compensating command finished.
COMP_OK = {
    "cmd.payment.refund": "reply.payment.refunded",
    "cmd.inventory.release": "reply.inventory.released",
}


@dataclass
class Saga:
    order_id: int
    state: str
    total_cents: int
    awaiting: str | None
    comp_pending: int
    step_started: object  # datetime | None


def log(cur: psycopg.Cursor, order_id: int, kind: str, detail: str) -> None:
    cur.execute(
        "INSERT INTO saga_log (order_id, kind, detail) VALUES (%s, %s, %s)",
        (order_id, kind, detail),
    )


def load(cur: psycopg.Cursor, order_id: int) -> Saga | None:
    cur.execute(
        "SELECT order_id, state, total_cents, awaiting, comp_pending, step_started "
        "FROM sagas WHERE order_id = %s",
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return Saga(row[0], row[1], row[2], row[3], row[4], row[5])


def create(cur: psycopg.Cursor, order_id: int, total_cents: int) -> None:
    cur.execute(
        "INSERT INTO sagas (order_id, state, total_cents) VALUES (%s, 'STARTED', %s) "
        "ON CONFLICT (order_id) DO NOTHING",
        (order_id, total_cents),
    )


def set_state(
    cur: psycopg.Cursor,
    order_id: int,
    state: str,
    *,
    awaiting: str | None = None,
    comp_pending: int | None = None,
) -> None:
    cur.execute(
        "UPDATE sagas SET state = %s, awaiting = %s::text, "
        "step_started = CASE WHEN %s::text IS NULL THEN NULL ELSE now() END, "
        "comp_pending = COALESCE(%s::int, comp_pending), "
        "updated_at = now() WHERE order_id = %s",
        (state, awaiting, awaiting, comp_pending, order_id),
    )
    log(cur, order_id, "state", f"-> {state}" + (f" (awaiting {awaiting})" if awaiting else ""))


def dec_comp_pending(cur: psycopg.Cursor, order_id: int) -> int:
    """Decrement the compensating-reply counter; return the new value."""
    cur.execute(
        "UPDATE sagas SET comp_pending = comp_pending - 1, updated_at = now() "
        "WHERE order_id = %s RETURNING comp_pending",
        (order_id,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0
