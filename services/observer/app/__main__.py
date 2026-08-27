"""
observer — turns the invisible message bus into something you can watch.

Two jobs:
  1. Bind a queue to "#" (every routing key) on the orders exchange, and
     broadcast each message to any connected dashboard over a WebSocket.
  2. Poll RabbitMQ's management HTTP API for queue depths and expose them at
     GET /queues, so the dashboard can show backlogs building up.

observer is a pure spectator. It never publishes to the orders exchange and no
business logic depends on it — you can kill it and the pipeline runs fine, you
just lose the view. That's deliberate: a monitoring tool must not be load-bearing.

Threading is the same pattern as order-service: consumer loop on a background
thread with its own pika connection, HTTP server on the main thread.
"""

import threading

import uvicorn

from .consumer import run_consumer

if __name__ == "__main__":
    t = threading.Thread(target=run_consumer, name="observer-consumer", daemon=True)
    t.start()
    uvicorn.run("app.api:app", host="0.0.0.0", port=8001, log_level="warning")
