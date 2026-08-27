"""
The HTTP surface of order-service.

POST /orders accepts an order, writes it to Postgres, and returns 202. It does
not call payment-service, does not wait for fulfilment. The response says
"accepted", not "confirmed" — the outcome is asynchronous.

--- Stage 3 -----------------------------------------------------------------

Orders live in Postgres (order_db). The OrderShipped consumer uses process_once
so a duplicate doesn't re-run the status update.

--- Stage 4: the outbox ---------------------------------------------------

`POST /orders` used to do: INSERT order (COMMIT), then _publisher.publish(
OrderPlaced). Two systems, one gap — crash between them and the order exists but
no OrderPlaced ever goes out. It just sits at PENDING forever.

Now the request handler does ONE transaction:

    INSERT INTO orders ...
    INSERT INTO outbox (routing_key='order.placed', body=...)
    COMMIT

and returns. RabbitMQ is not touched in the request path at all. A relay thread
(started in __main__.py) drains order_db.outbox to the broker. If the relay is
down when an order comes in, the order is still safely recorded and the event
goes out when the relay recovers.
"""

import pathlib
import threading
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field

from pyevents import connect_db, run_script, stage_event

SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()

app = FastAPI(title="order-service")

# One psycopg connection for the HTTP side. FastAPI's sync endpoints run on a
# threadpool, so guard it with a lock (demo scale — a real service would pool).
_db = connect_db()
run_script(_db, SCHEMA)
_db_lock = threading.Lock()


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
    event_id = str(uuid.uuid4())
    with _db_lock:
        with _db.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (customer_id, total_cents) VALUES (%s, %s) "
                "RETURNING order_id",
                (req.customer_id, req.total_cents),
            )
            inserted = cur.fetchone()
            assert inserted is not None  # RETURNING always yields a row
            order_id = inserted[0]

            stage_event(
                cur,
                event_id=event_id,
                routing_key="order.placed",
                body={
                    "event_id": event_id,
                    "order_id": order_id,
                    "customer_id": req.customer_id,
                    "items": [item.model_dump() for item in req.items],
                    "total_cents": req.total_cents,
                    "placed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        _db.commit()  # order row + outbox row, atomically

    return {"order_id": order_id, "status": "PENDING", "accepted": True}
