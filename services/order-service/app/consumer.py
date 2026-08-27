"""
order-service's consumer thread.

It listens for OrderShipped and updates the local order status. That's the only
event order-service consumes — it's mostly a producer.

Runs on its own connection (created here, inside the thread) so it never shares a
channel with the HTTP publisher.
"""

from pyevents import connect, consume, Message

from .api import mark_shipped

QUEUE = "order.q"


def run_consumer() -> None:
    conn = connect()
    ch = conn.channel()

    # order.q (+ its .retry / .dlq) is declared by infra/topology.py, which the
    # compose file runs to completion before this service starts. We just consume.
    def handler(msg: Message) -> None:
        if msg.routing_key == "order.shipped":
            mark_shipped(msg.body["order_id"])

    consume(ch, queue=QUEUE, handler=handler)
