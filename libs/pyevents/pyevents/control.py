"""
The "control" channel — how the dashboard breaks things on purpose.

Stage 2 needs to flip behaviour at runtime ("start failing payments", "slow
inventory down") without restarting containers or giving each service an HTTP
admin endpoint. So: a `control` **fanout** exchange. The dashboard publishes a
small JSON command; every service binds its own exclusive, auto-delete queue and
gets a copy.

    dashboard ──▶ observer  POST /control  ──▶ [ control exchange (fanout) ]
                                                 │        │         │
                                          payment    inventory   shipping
                                          (own       (own        (own
                                           exclusive  exclusive    exclusive
                                           queue)     queue)       queue)

Commands are advisory and in-memory: a service restart resets everything to the
compose defaults. That's fine — this is a failure lab, not configuration
management.

Command shape:   {"target": "payment", "action": "fail", "value": true}
                 {"target": "inventory", "action": "slow_ms", "value": 8000}
"""

from __future__ import annotations

import json
import threading
from typing import Callable

from .connection import connect

CONTROL_EXCHANGE = "control"

CommandHandler = Callable[[dict], None]


def publish_command(command: dict) -> None:
    """One-shot: open a connection, fanout the command, close. Used by observer."""
    conn = connect(retries=3, delay=1)
    ch = conn.channel()
    ch.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type="fanout", durable=True)
    ch.basic_publish(
        exchange=CONTROL_EXCHANGE,
        routing_key="",
        body=json.dumps(command).encode(),
    )
    conn.close()


def listen_for_commands(handler: CommandHandler, *, service: str) -> threading.Thread:
    """
    Start a background thread that binds an exclusive queue to the control
    exchange and calls `handler(command)` for each command received.

    `service` is only used for log lines. The queue is server-named + exclusive +
    auto-delete: the broker picks a unique name (e.g. "amq.gen-Xyz..."), so every
    replica gets its OWN queue and its OWN copy of each fanout command — all 3
    payment replicas react to one toggle. A named queue would make the replicas
    collide on the same exclusive name.
    """

    def _run() -> None:
        conn = connect()
        ch = conn.channel()
        ch.exchange_declare(
            exchange=CONTROL_EXCHANGE, exchange_type="fanout", durable=True
        )
        # queue="" -> broker assigns a unique name. exclusive -> gone when this
        # connection closes.
        result = ch.queue_declare(queue="", exclusive=True)
        qname = result.method.queue
        ch.queue_bind(queue=qname, exchange=CONTROL_EXCHANGE)
        print(f"[control:{service}] queue {qname}", flush=True)

        def _on_msg(_ch, _method, _props, raw: bytes) -> None:
            try:
                command = json.loads(raw)
            except json.JSONDecodeError:
                return
            try:
                handler(command)
            except Exception as err:  # noqa: BLE001
                print(f"[control:{service}] handler error: {err!r}", flush=True)

        ch.basic_consume(queue=qname, on_message_callback=_on_msg, auto_ack=True)
        print(f"[control:{service}] listening for commands", flush=True)
        ch.start_consuming()

    t = threading.Thread(target=_run, name=f"control-{service}", daemon=True)
    t.start()
    return t
