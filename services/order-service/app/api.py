"""
The HTTP surface of order-service.

POST /orders accepts an order, assigns it an id, and publishes OrderPlaced. That
is *all* it does. It does not call payment-service. It does not wait for the
order to be fulfilled. It publishes an event and returns 202 Accepted.

The response deliberately says "accepted", not "confirmed" — because from here on
the outcome is asynchronous and this service genuinely does not know it yet.
"""

import itertools
import threading
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field

from pyevents import Publisher

EXCHANGE = "orders"

app = FastAPI(title="order-service")

# In-memory order store for stages 0-2. Stage 3 moves this to Postgres.
_orders: dict[int, dict] = {}
_id_seq = itertools.count(1001)
_lock = threading.Lock()

# Self-healing publisher for the HTTP thread. Between orders this connection sits
# idle; pika doesn't heartbeat an idle BlockingConnection, so RabbitMQ closes it
# after ~60s. Publisher detects the dead channel and reconnects on the next
# publish, instead of throwing ChannelWrongStateError. (Its own connection, never
# shared with the consumer thread — see __main__.py.)
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
    return {"status": "ok", "orders": len(_orders)}


@app.get("/orders/{order_id}")
def get_order(order_id: int) -> dict:
    return _orders.get(order_id, {"error": "not found"})


@app.post("/orders", status_code=202)
def place_order(req: PlaceOrder) -> dict:
    with _lock:
        order_id = next(_id_seq)
        _orders[order_id] = {
            "order_id": order_id,
            "customer_id": req.customer_id,
            "status": "PENDING",
        }

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


def mark_shipped(order_id: int) -> None:
    """Called by the consumer thread when OrderShipped arrives."""
    with _lock:
        if order_id in _orders:
            _orders[order_id]["status"] = "SHIPPED"
            print(f"[order-service] order {order_id} -> SHIPPED", flush=True)
