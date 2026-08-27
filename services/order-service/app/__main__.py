"""
order-service entrypoint.

This is the one Python service that has to do two things at once:
  - serve HTTP  (POST /orders  — a customer places an order)
  - consume events  (order.shipped -> mark the order complete)

pika is blocking and single-threaded, and a pika channel can't be shared across
threads. So the design is:

  main thread        -> uvicorn (FastAPI). Owns its OWN connection+channel,
                        used only to PUBLISH when an order comes in.
  background thread  -> the consume loop. Owns a SEPARATE connection+channel.

They never touch each other's channel. This is the pattern any service needs
when it mixes a request/response API with a message consumer.
"""

import threading

import uvicorn

from .consumer import run_consumer

if __name__ == "__main__":
    # Consumer on its own thread with its own broker connection.
    t = threading.Thread(target=run_consumer, name="order-consumer", daemon=True)
    t.start()

    # HTTP server on the main thread.
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, log_level="info")
