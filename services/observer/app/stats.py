"""
Cross-service consistency snapshot for the dashboard.

observer is a monitoring tool, so it's allowed to peek into every service's
database read-only. The dashboard renders these numbers side by side; when
idempotency is working they stay consistent no matter how many duplicates the
"duplicate everything" toggle injects, and when it's broken they visibly drift:

    orders.total_cents   should equal   payment.total_charged_cents
    orders count         should equal   payment rows == inventory reservations
                                         == shipping shipments
    100000 - inventory.stock_qty         should equal   the reservation count
"""

from __future__ import annotations

import os
from typing import LiteralString

import psycopg

# One read-only DSN per service DB. Same postgres server, different databases.
_PG = os.environ.get("PG_HOST", "postgres")
DSNS = {
    "order": f"postgresql://demo:demo@{_PG}:5432/order_db",
    "payment": f"postgresql://demo:demo@{_PG}:5432/payment_db",
    "inventory": f"postgresql://demo:demo@{_PG}:5432/inventory_db",
    "shipping": f"postgresql://demo:demo@{_PG}:5432/shipping_db",
}
_ORCH_DSN = f"postgresql://demo:demo@{_PG}:5432/orchestrator_db"


def _one(dsn: str, sql: LiteralString) -> tuple | None:
    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()
    except Exception:  # noqa: BLE001 — a service may not have created tables yet
        return None


def _all(dsn: str, sql: LiteralString) -> list[tuple]:
    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
    except Exception:  # noqa: BLE001
        return []


def saga_snapshot() -> dict:
    """Stage 5: the orchestrator's view — per-order state + the audit log."""
    states = _all(
        _ORCH_DSN,
        "SELECT state, count(*) FROM sagas GROUP BY state ORDER BY state",
    )
    recent = _all(
        _ORCH_DSN,
        "SELECT order_id, at, kind, detail FROM saga_log "
        "ORDER BY id DESC LIMIT 60",
    )
    return {
        "by_state": {s: int(c) for s, c in states},
        "log": [
            {"order_id": o, "at": a.isoformat(), "kind": k, "detail": d}
            for o, a, k, d in recent
        ],
    }


def snapshot() -> dict:
    orders = _one(
        DSNS["order"],
        "SELECT count(*), coalesce(sum(total_cents),0), "
        "count(*) FILTER (WHERE status='SHIPPED'), "
        "count(*) FILTER (WHERE status='CANCELLED'), "
        "coalesce(sum(total_cents) FILTER (WHERE status='SHIPPED'), 0) "
        "FROM orders",
    )
    payment = _one(
        DSNS["payment"],
        "SELECT count(*), (SELECT total_charged_cents FROM ledger WHERE id=1) FROM payments",
    )
    inventory = _one(
        DSNS["inventory"],
        "SELECT count(*), (SELECT qty FROM stock WHERE sku='WIDGET-1') FROM reservations",
    )
    shipping = _one(DSNS["shipping"], "SELECT count(*) FROM shipments")
    dupes = {
        svc: (_one(DSNS[svc], "SELECT count(*) FROM processed_events") or [0])[0]
        for svc in DSNS
    }
    # Stage 4: outbox rows still waiting for the relay. Non-zero briefly under
    # load, or persistently if a relay is down / a service is stalled.
    outbox_pending = {
        svc: int(
            (_one(DSNS[svc], "SELECT count(*) FROM outbox WHERE published_at IS NULL") or [0])[0]
        )
        for svc in DSNS
    }

    # psycopg returns SUM() as Decimal and COUNT() as int — normalise to int so
    # the JSON is clean and comparisons are unambiguous.
    order_count = int(orders[0]) if orders else 0
    order_total = int(orders[1]) if orders else 0
    shipped = int(orders[2]) if orders else 0
    cancelled = int(orders[3]) if orders else 0
    shipped_total = int(orders[4]) if orders else 0
    pay_rows = int(payment[0]) if payment else 0
    pay_total = int(payment[1]) if payment and payment[1] is not None else 0
    resv = int(inventory[0]) if inventory else 0
    stock_qty = int(inventory[1]) if inventory and inventory[1] is not None else 0
    ship_rows = int(shipping[0]) if shipping else 0

    return {
        "orders": order_count,
        "orders_total_cents": order_total,
        "shipped": shipped,
        "cancelled": cancelled,
        "shipped_total_cents": shipped_total,
        "payment_rows": pay_rows,
        "payment_total_cents": pay_total,
        "reservations": resv,
        "stock_consumed": 100000 - stock_qty if stock_qty else 0,
        "shipments": ship_rows,
        "processed_events": dupes,
        "outbox_pending": outbox_pending,
        # Stage 5: a CANCELLED order legitimately has NO payment / reservation /
        # shipment — the compensation removed them. So the invariant is now:
        # the footprint of COMPLETED (SHIPPED) orders lines up, and CANCELLED
        # orders leave nothing behind.
        "consistent": (
            shipped == pay_rows == resv == ship_rows
            and shipped_total == pay_total
        ),
    }
