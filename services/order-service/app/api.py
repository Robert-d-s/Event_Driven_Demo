"""
The HTTP surface of order-service.

POST /orders accepts an order, assigns it an id, and publishes OrderPlaced. That
is *all* it does. It does not call payment-service. It does not wait for the
order to be fulfilled. It publishes an event and returns 202 Accepted.

The response deliberately says "accepted", not "confirmed" — because from here on
the outcome is asynchronous and this service genuinely does not know it yet.

--- Stage 3 -----------------------------------------------------------------

Orders now live in Postgres (order_db), not an in-memory dict. The consumer that
handles OrderShipped uses process_once so a duplicate OrderShipped doesn't
re-run the status update (harmless here, but the pattern is uniform across
services). GET /stats exposes the numbers the dashboard compares to prove
idempotency: order count, sum of order totals, statuses.
"""

import pathlib
import threading
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field

from pyevents import Publisher, connect_db, run_script

EXCHANGE = "orders"
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

app = FastAPI(title="order-service")

# One psycopg connection for the HTTP side. FastAPI's sync endpoints run on a
# threadpool, so guard it with a lock (demo scale — a real service would pool).
_db = connect_db()
run_script(_db, SCHEMA)
_db_lock = threading.Lock()

# Self-healing publisher for the HTTP thread — survives RabbitMQ's idle-connection
# heartbeat timeout. Its own connection, never shared with the consumer thread.
_publisher = Publisher(exchange=EXCHANGE)


class OrderItem(BaseModel):
    sku: str
    qty: int = Field(ge=1)


class PlaceOrder(BaseModel):
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    total_cents: int = Field(ge=0)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/orders/{order_id}")
def get_order(order_id: int) -> dict:
    with _db_lock, _db.cursor() as cur:
        cur.execute(
            "SELECT order_id, customer_id, total_cents, status "
            "FROM orders WHERE order_id = %s",
            (order_id,),
        )
        row = cur.fetchone()
    _db.commit()
    if row is None:
        return {"error": "not found"}
    return {
        "order_id": row[0],
        "customer_id": row[1],
        "total_cents": row[2],
        "status": row[3],
    }


@app.post("/orders", status_code=202)
def place_order(req: PlaceOrder) -> dict:
    with _db_lock:
        with _db.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (customer_id, total_cents) VALUES (%s, %s) "
                "RETURNING order_id",
                (req.customer_id, req.total_cents),
            )
            inserted = cur.fetchone()
        _db.commit()
    assert inserted is not None  # RETURNING always yields a row
    order_id = inserted[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": req.customer_id,
        "items": [item.model_dump() for item in req.items],
        "total_cents": req.total_cents,
        "placed_at": datetime.now(timezone.utc).isoformat(),
    }
    _publisher.publish(routing_key="order.placed", body=event)
    return {"order_id": order_id, "status": "PENDING", "accepted": True}


