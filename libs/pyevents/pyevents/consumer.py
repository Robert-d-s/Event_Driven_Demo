"""
The consume loop.

--- "at least once" -----------------------------------------------------------

A message is removed from a queue only when the consumer *acknowledges* it
(`basic_ack`). Crash before the ack and the broker redelivers it — to this
consumer or another replica. That's why the guarantee is "at least once", never
"exactly once". Stage 3 makes handlers safe under that.

--- Stage 2: retry with backoff, then dead-letter ----------------------------

When a handler raises, we do NOT `basic_nack(requeue=True)` — that redelivers
immediately and a persistently-failing message would spin. Instead:

  1. Read our own `x-retry-count` header (0 on first delivery).
  2. If count < max_attempts:  bump the count, publish the message to
     `<queue>.retry` on the DLX, then `basic_ack` the original. It waits out the
     retry queue's TTL, expires, and is dead-lettered back to `<queue>` for
     another attempt.
  3. If count >= max_attempts:  publish to `<queue>.dead` on the DLX
     (→ `<queue>.dlq`), then `basic_ack`. A human looks at the DLQ.

Why an `x-retry-count` header we manage ourselves, rather than reading the
broker's `x-death` count? Because we re-publish the message on each failure
(basic_publish), and from the broker's point of view that's a brand-new message —
its `x-death` bookkeeping doesn't carry across a manual re-publish reliably. A
header we own and increment is unambiguous.

The original routing key is stashed in `x-original-routing-key` on the first
retry, so redelivered messages still report where they came from
(`order.placed`), not the DLX plumbing key.

A message whose body won't parse (a "poison" message) skips straight to the DLQ.

The retry/DLQ topology is declared in infra/topology.py.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable

import pika

from .connection import Channel

DLX = "orders.dlx"
DEFAULT_MAX_ATTEMPTS = 3

_RETRY_COUNT = "x-retry-count"
_ORIGINAL_RK = "x-original-routing-key"


@dataclass
class Message:
    """What a handler receives. Raw AMQP details stay out of the handler."""

    routing_key: str
    body: dict
    event_id: str
    redelivered: bool  # broker has delivered this message before
    attempt: int  # 1 on first delivery, 2 on the first retry, …


Handler = Callable[[Message], None]


def _int_header(headers: dict | None, key: str) -> int:
    if not headers:
        return 0
    value = headers.get(key, 0)
    return int(value) if isinstance(value, (int, float, str)) else 0


def consume(
    channel: Channel,
    *,
    queue: str,
    handler: Handler,
    prefetch: int = 1,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry: bool = True,
) -> None:
    """
    Consume `queue` forever, calling `handler(Message)` per message.

    prefetch=1 (basic_qos): the broker won't send a second message until the
    current one is acked. This is what makes a shared work queue distribute
    fairly across replicas — a slow consumer just gets fewer messages instead of
    a pile up front. Set it to 50 and watch one replica hog everything.

    retry=True (default): failures go through the retry/DLQ topology (the queue
    must have a matching `<queue>.retry` / `<queue>.dlq` — see topology.py).
    retry=False: for observer-style consumers (notification, observer) with no
    retry queues — a failed message is simply dropped, which is the right call
    when nothing downstream depends on that consumer.
    """
    channel.basic_qos(prefetch_count=prefetch)

    def _republish(
        target_key: str,
        raw: bytes,
        props: pika.BasicProperties,
        *,
        new_headers: dict,
    ) -> None:
        channel.basic_publish(
            exchange=DLX,
            routing_key=target_key,
            body=raw,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=props.message_id,
                type=props.type,
                headers=new_headers,
            ),
        )

    def _on_message(ch, method, properties, raw: bytes) -> None:
        headers = dict(properties.headers or {})

        # --- poison: unparseable body -------------------------------------
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            dest = f"{queue}.dlq" if retry else "(dropped)"
            print(
                f"[consume:{queue}] poison message (won't parse) -> {dest}: {raw!r}",
                flush=True,
            )
            if retry:
                _republish(f"{queue}.dead", raw, properties, new_headers=headers)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        retry_count = _int_header(headers, _RETRY_COUNT)
        original_rk = headers.get(_ORIGINAL_RK) or method.routing_key

        msg = Message(
            routing_key=str(original_rk),
            body=body,
            event_id=str(body.get("event_id", properties.message_id or "")),
            redelivered=bool(method.redelivered),
            attempt=retry_count + 1,
        )

        try:
            handler(msg)
        except Exception as err:  # noqa: BLE001 — handler decides what's fatal
            if not retry:
                print(
                    f"[consume:{queue}] handler failed on {msg.routing_key} "
                    f"(event_id={msg.event_id}), dropping (retry disabled): {err!r}",
                    flush=True,
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            headers[_RETRY_COUNT] = retry_count + 1
            headers[_ORIGINAL_RK] = str(original_rk)

            if retry_count + 1 >= max_attempts:
                print(
                    f"[consume:{queue}] attempt {msg.attempt}/{max_attempts} failed on "
                    f"{msg.routing_key} (event_id={msg.event_id}) -> {queue}.dlq: {err!r}",
                    flush=True,
                )
                _republish(f"{queue}.dead", raw, properties, new_headers=headers)
            else:
                print(
                    f"[consume:{queue}] attempt {msg.attempt}/{max_attempts} failed on "
                    f"{msg.routing_key} (event_id={msg.event_id}), retrying in a few s: {err!r}",
                    flush=True,
                )
                _republish(f"{queue}.retry", raw, properties, new_headers=headers)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue, on_message_callback=_on_message)
    print(
        f"[consume:{queue}] waiting for messages "
        f"(prefetch={prefetch}, max_attempts={max_attempts})",
        flush=True,
    )
    channel.start_consuming()
