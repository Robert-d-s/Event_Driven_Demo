"""
Publishing an event.

Notice what a publisher does NOT do: it never names a queue. It publishes to an
*exchange* with a *routing key* (like "order.placed"). The exchange decides which
queues get a copy, based on bindings that the *consumers* set up.

That indirection is the whole point of the architecture. order-service has no
idea that payment-service exists. You can add inventory-service later and
order-service never changes.

--- Two ways to publish -------------------------------------------------------

`publish(channel, ...)` is the low-level call: you hand it a live channel.
Consumers already have one (their consume loop), so they use this directly to
emit follow-on events.

`Publisher` is for code that publishes from *outside* a consume loop — an HTTP
handler, the outbox relay. A pika BlockingConnection that sits idle between
requests stops sending heartbeats, so RabbitMQ closes it after ~60s and the next
publish throws ChannelWrongStateError. `Publisher` hides that: it checks the
channel is open before each publish and transparently reconnects if not.
"""

import json
from datetime import datetime, timezone

import pika
import pika.exceptions

from .connection import Channel, connect


def publish(
    channel: Channel,
    *,
    exchange: str,
    routing_key: str,
    body: dict,
) -> None:
    """
    Publish one event to `exchange` with `routing_key`.

    `delivery_mode=2` marks the message persistent, so it survives a broker
    restart (as long as the queue it lands in is also durable — see topology.py).

    We set a `message_id` and `timestamp` in the AMQP properties. The dashboard's
    observer reads these; later stages lean on `message_id` for idempotency.
    """
    payload = json.dumps(body).encode("utf-8")
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=payload,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # persistent
            message_id=str(body.get("event_id", "")),
            timestamp=int(datetime.now(timezone.utc).timestamp()),
            type=routing_key,
        ),
    )
    print(f"[publish] {routing_key} -> exchange '{exchange}': {body}", flush=True)


class Publisher:
    """
    A self-healing publisher for request-driven code.

    Owns its own connection + channel. Before every publish it verifies the
    channel is open; if the broker has dropped the idle connection, it rebuilds
    it and retries once. Declares the exchange on each (re)connect so it's always
    safe to publish to.

    Thread note: like any pika object, a single Publisher instance is NOT
    thread-safe. FastAPI serves requests on a threadpool, so publishing is
    guarded by an internal lock — fine at demo scale. A high-throughput service
    would use a Publisher per worker instead.
    """

    def __init__(self, *, exchange: str, exchange_type: str = "topic") -> None:
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._conn: pika.BlockingConnection | None = None
        self._channel: Channel | None = None
        import threading

        self._lock = threading.Lock()
        self._open()

    def _open(self) -> None:
        self._conn = connect()
        self._channel = self._conn.channel()
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type=self._exchange_type,
            durable=True,
        )
        print(f"[Publisher] connected, exchange '{self._exchange}' ready", flush=True)

    def _healthy(self) -> bool:
        return (
            self._conn is not None
            and self._conn.is_open
            and self._channel is not None
            and self._channel.is_open
        )

    def publish(self, *, routing_key: str, body: dict) -> None:
        with self._lock:
            if not self._healthy():
                print("[Publisher] channel dead, reconnecting", flush=True)
                self._open()
            try:
                assert self._channel is not None
                publish(
                    self._channel,
                    exchange=self._exchange,
                    routing_key=routing_key,
                    body=body,
                )
            except (
                pika.exceptions.ChannelWrongStateError,
                pika.exceptions.ConnectionWrongStateError,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionError,
            ):
                # Broker dropped us between the health check and the publish.
                # Rebuild once and retry; if this throws, let it propagate.
                print("[Publisher] publish failed, reconnecting once", flush=True)
                self._open()
                assert self._channel is not None
                publish(
                    self._channel,
                    exchange=self._exchange,
                    routing_key=routing_key,
                    body=body,
                )
