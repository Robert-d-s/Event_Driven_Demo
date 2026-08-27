"""
Declares the RabbitMQ topology.

STAGE 1 gave us one topic exchange and one queue per consumer.

STAGE 2 adds a retry + dead-letter path for each *consumer* queue.

--- The shape (payment shown; inventory/shipping/order are identical) ----------

    orders ──"order.placed"──▶ payment.q
    (topic)                      │
                                 │  handler raises
                                 ▼
              consume() bumps x-retry-count and publishes to
                                 │
                       orders.dlx ──"payment.q.retry"──▶ payment.q.retry
                       (direct)                            │  x-message-ttl = 5000ms
                                                           │  (message just sits here)
                                                           ▼  TTL expires
                                        dead-lettered via x-dead-letter-exchange
                                        with x-dead-letter-routing-key = payment.q
                                                           │
                       orders.dlx ──"payment.q"────────────┘  ◀── back for another go

    After MAX_ATTEMPTS, consume() publishes to "payment.q.dead" instead:

                       orders.dlx ──"payment.q.dead"──▶ payment.q.dlq
                                                          (no consumer — terminal)

--- Why this design ----------------------------------------------------------

* **A dedicated direct exchange (`orders.dlx`), not the topic `orders`.**
  If retries went back through `orders` with routing key `order.placed`, every
  retry would ALSO re-hit notification.q and observer.q — the customer would get
  "order received!" three times. Routing retries by *queue name* on a direct
  exchange keeps a retry private to the one consumer that failed.

* **A retry queue with a TTL, not `basic_nack(requeue=True)`.**
  A requeued message is redelivered immediately. A handler that always fails
  (downstream down, bad data) then spins in a tight loop. RabbitMQ has no native
  "redeliver in 5s", so we park the message in a queue with a per-message TTL and
  let it expire — an expired message is dead-lettered, and we point that back at
  the work queue.

* **Attempt counting via an `x-retry-count` header that consume() manages.**
  The broker's own `x-death` count doesn't survive a manual re-publish cleanly
  (each failure re-publishes the message), so consume() owns and increments its
  own header instead. See libs/pyevents/pyevents/consumer.py.

--- Changing this file / moving between stages -------------------------------

Queue arguments (x-message-ttl, x-dead-letter-exchange, …) are fixed when the
queue is declared. Re-declaring a queue with *different* args fails with
PRECONDITION_FAILED. So between stages:  `make down`  then  `make up`.
Re-running topology with unchanged args is still a safe no-op.
"""

from pyevents import Channel, connect

EXCHANGE = "orders"        # topic — the event bus
DLX = "orders.dlx"         # direct — retry + dead-letter routing, keyed by queue name

RETRY_DELAY_MS = 5000
MAX_ATTEMPTS = 3

# consumer queue -> binding patterns on the "orders" topic exchange.
# Each also gets "<queue>.retry" and "<queue>.dlq".
CONSUMER_QUEUES: dict[str, list[str]] = {
    "payment.q": ["order.placed"],
    "inventory.q": ["payment.captured"],
    "shipping.q": ["stock.reserved"],
    "order.q": ["order.shipped"],
}

# Observer queues: no retry. If notification/observer fails on a message,
# dropping it is fine — nothing downstream depends on them.
OBSERVER_QUEUES: dict[str, list[str]] = {
    "notification.q": ["order.placed", "payment.captured", "stock.reserved", "order.shipped"],
    "observer.q": ["#"],
}


def declare(channel: Channel) -> None:
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)

    for queue, patterns in CONSUMER_QUEUES.items():
        retry_q = f"{queue}.retry"
        dlq = f"{queue}.dlq"

        # Work queue. Bound to the topic exchange for normal delivery, and to the
        # DLX under its own name so retried messages come back to it.
        channel.queue_declare(queue=queue, durable=True)
        for pattern in patterns:
            channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=pattern)
        channel.queue_bind(queue=queue, exchange=DLX, routing_key=queue)

        # Retry holding pen. Message waits RETRY_DELAY_MS, expires, and is
        # dead-lettered via DLX with routing key = <queue>, landing back in the
        # work queue above.
        channel.queue_declare(
            queue=retry_q,
            durable=True,
            arguments={
                "x-message-ttl": RETRY_DELAY_MS,
                "x-dead-letter-exchange": DLX,
                "x-dead-letter-routing-key": queue,
            },
        )
        channel.queue_bind(queue=retry_q, exchange=DLX, routing_key=f"{queue}.retry")

        # Terminal dead-letter queue. No consumer, no TTL.
        channel.queue_declare(queue=dlq, durable=True)
        channel.queue_bind(queue=dlq, exchange=DLX, routing_key=f"{queue}.dead")

        print(
            f"  {queue:<12} + {retry_q} (ttl {RETRY_DELAY_MS}ms, "
            f"max {MAX_ATTEMPTS} attempts) + {dlq}",
            flush=True,
        )

    for queue, patterns in OBSERVER_QUEUES.items():
        channel.queue_declare(queue=queue, durable=True)
        for pattern in patterns:
            channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=pattern)
        print(f"  {queue:<12} (observer, no retry)", flush=True)


def main() -> None:
    conn = connect()
    ch = conn.channel()
    print(f"declaring topology: '{EXCHANGE}' (topic) + '{DLX}' (direct)", flush=True)
    declare(ch)
    conn.close()
    print("topology ready", flush=True)


if __name__ == "__main__":
    main()
