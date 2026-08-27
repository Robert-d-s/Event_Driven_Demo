"""
Opening a connection to RabbitMQ.

pika is *synchronous* and *blocking*. There is no event loop here. A
BlockingConnection is a real TCP socket; a channel is a lightweight virtual
connection multiplexed over it.

The one rule that matters and that the rest of this project obeys everywhere:

    one connection per thread, and never share a channel between threads.

pika channels are not thread-safe. Consumer-only services (payment, inventory,
shipping) are a single thread doing `start_consuming()` forever, so this is a
non-issue for them. order-service and observer also serve HTTP, so they run the
consumer on its own thread with its own connection — see their main.py.
"""

import os
import time

import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel

# A pika channel, for type hints across the package. `import pika` alone does not
# expose the submodule this lives in, so it's imported explicitly here and
# re-exported for the other modules to use.
Channel = BlockingChannel

# In compose the hostname is the service name "rabbitmq". Locally it's localhost.
BROKER_URL = os.environ.get(
    "BROKER_URL", "amqp://guest:guest@localhost:5672/%2F"
)


def connect(*, retries: int = 30, delay: float = 2.0) -> pika.BlockingConnection:
    """
    Connect to the broker, retrying while it boots.

    Compose starts rabbitmq and our services at roughly the same time, and the
    broker takes a few seconds to accept connections. Without this retry loop
    every service would crash on startup in a fresh `docker compose up`.
    """
    params = pika.URLParameters(BROKER_URL)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(params)
            print(f"[pyevents] connected to broker on attempt {attempt}", flush=True)
            return conn
        except pika.exceptions.AMQPConnectionError as err:
            last_err = err
            print(
                f"[pyevents] broker not ready (attempt {attempt}/{retries}), "
                f"retrying in {delay}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"could not connect to broker after {retries} attempts") from last_err
