"""
Declares the RabbitMQ topology: one exchange, and one queue per consumer bound
to the routing keys it cares about.

Run this once before starting the services (the Makefile / compose does it for
you). It is idempotent — declaring an exchange or queue that already exists with
the same settings is a no-op, so re-running it is safe.

Why declare topology in one place instead of each service declaring its own?
Mostly so you can *read the whole system in one file*. In a larger setup each
service would own its queue declaration; here, seeing every binding at once is
worth more.

--- The mental model this file is meant to teach --------------------------------

  Publisher --> [ exchange "orders" ] --> queue --> consumer
                       (topic)

The publisher picks a ROUTING KEY ("order.placed"). The exchange matches that key
against each queue's BINDING PATTERN and copies the message into every queue that
matches. "#" matches anything.

Different consumers = different queues = each gets its own copy.
Replicas of ONE consumer = ONE shared queue = messages split between them.

That second case is why payment-service scales to 3 replicas in compose but they
all read `payment.q`.
"""

from pyevents import Channel, connect

EXCHANGE = "orders"

# queue name -> list of binding patterns
QUEUES: dict[str, list[str]] = {
    "payment.q": ["order.placed"],
    "inventory.q": ["payment.captured"],
    "shipping.q": ["stock.reserved"],
    "order.q": ["order.shipped"],
    "notification.q": ["order.placed", "payment.captured", "stock.reserved", "order.shipped"],
    # observer sees everything — it feeds the dashboard
    "observer.q": ["#"],
}


def declare(channel: Channel) -> None:
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True,  # survives a broker restart
    )
    for queue, patterns in QUEUES.items():
        channel.queue_declare(queue=queue, durable=True)
        for pattern in patterns:
            channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=pattern)
            print(f"  bound {queue:<16} <- {pattern}", flush=True)


def main() -> None:
    conn = connect()
    ch = conn.channel()
    print(f"declaring topology on exchange '{EXCHANGE}'", flush=True)
    declare(ch)
    conn.close()
    print("topology ready", flush=True)


if __name__ == "__main__":
    main()
