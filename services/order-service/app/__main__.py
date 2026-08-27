"""
order-service entrypoint.

Three things run concurrently:

  main thread        -> uvicorn (FastAPI). POST /orders writes the order + an
                        OrderPlaced outbox row in one transaction. Never touches
                        RabbitMQ.
  consumer thread    -> consume loop for OrderShipped. Own pika + psycopg conns.
  relay thread       -> drains order_db.outbox to RabbitMQ. Own conns.

pika is blocking and channels aren't thread-safe, so each thread that talks to
the broker owns its own connection. The HTTP thread doesn't talk to the broker
at all any more (Stage 4) — that's the outbox's whole point.
"""

import threading

import uvicorn

from pyevents import relay_loop, DB_URL

from .consumer import run_consumer

if __name__ == "__main__":
    threading.Thread(
        target=run_consumer, name="order-consumer", daemon=True
    ).start()
    threading.Thread(
        target=relay_loop, args=(DB_URL,), kwargs={"exchange": "orders"},
        name="outbox-relay", daemon=True,
    ).start()

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, log_level="info")
