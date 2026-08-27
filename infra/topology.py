"""
Declares the RabbitMQ topology.

--- Stages 1-4: choreography ------------------------------------------------

One topic exchange "orders". Each service binds a queue to the routing keys it
cares about and reacts. Nobody is in charge; the workflow is the sum of everyone's
bindings. Retry + dead-letter path per consumer queue (Stage 2).

--- Stage 5: orchestration -------------------------------------------------

A new "commands" topic exchange and an `orchestrator` service that owns each
order's state machine. Instead of reacting to whatever the previous service
emitted, services now receive explicit COMMANDS and send back REPLIES:

    orchestrator ──cmd.payment.charge──▶ payment.cmd.q ──▶ payment-service
    payment-service ──reply.payment.charged / reply.payment.failed──▶ orchestrator.q

On a failure partway through (shipping can't dispatch an order already charged
and reserved), the orchestrator sends COMPENSATING commands in reverse:

    cmd.inventory.release,  cmd.payment.refund

`order.placed` still flows on the "orders" exchange — it's the trigger the
orchestrator listens for. Everything after it is commands + replies.

Retry/DLQ still applies to the command queues (a command that keeps failing
dead-letters, and the orchestrator's per-step timeout catches a silent service).

--- Between stages --------------------------------------------------------

Queue arguments are fixed at declare time, and Stage 5 adds new queues. Always
`make down` then `make up` when switching stages.
"""

from pyevents import Channel, connect

EVENTS = "orders"          # topic — the Stage 1-4 event bus (still carries order.placed)
DLX = "orders.dlx"         # direct — retry + dead-letter routing, keyed by queue name
COMMANDS = "commands"      # topic — Stage 5 command + reply bus

RETRY_DELAY_MS = 5000
MAX_ATTEMPTS = 3

# --- Stage 1-4 event consumers (choreography) -----------------------------
# order.q still matters: order-service marks the order SHIPPED / CANCELLED from
# the terminal events the orchestrator emits.
EVENT_QUEUES: dict[str, list[str]] = {
    "order.q": ["order.shipped", "order.cancelled"],
}

OBSERVER_QUEUES: dict[str, list[str]] = {
    "notification.q": ["order.placed", "order.shipped", "order.cancelled"],
    "observer.q": ["#"],  # bound to BOTH exchanges below
}

# --- Stage 5 command queues (orchestration) -------------------------------
# service queue -> the command routing keys it handles (forward + compensating).
COMMAND_QUEUES: dict[str, list[str]] = {
    "payment.cmd.q": ["cmd.payment.charge", "cmd.payment.refund"],
    "inventory.cmd.q": ["cmd.inventory.reserve", "cmd.inventory.release"],
    "shipping.cmd.q": ["cmd.shipping.dispatch"],
}

# The orchestrator's queue: every reply, plus the order.placed trigger.
ORCH_REPLY_BINDINGS = ["reply.#"]
ORCH_EVENT_BINDINGS = ["order.placed"]


def _declare_with_retry(
    channel: Channel, queue: str, bindings: list[str], exchange: str
) -> None:
    """A work queue plus its retry queue and terminal DLQ (Stage 2 machinery)."""
    retry_q, dlq = f"{queue}.retry", f"{queue}.dlq"

    channel.queue_declare(queue=queue, durable=True)
    for rk in bindings:
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=rk)
    channel.queue_bind(queue=queue, exchange=DLX, routing_key=queue)

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

    channel.queue_declare(queue=dlq, durable=True)
    channel.queue_bind(queue=dlq, exchange=DLX, routing_key=f"{queue}.dead")
    print(f"  {queue:<18} <- {bindings}", flush=True)


def declare(channel: Channel) -> None:
    channel.exchange_declare(exchange=EVENTS, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=COMMANDS, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)

    for queue, bindings in EVENT_QUEUES.items():
        _declare_with_retry(channel, queue, bindings, EVENTS)
        # The orchestrator emits order.shipped / order.cancelled through its
        # outbox relay, which publishes on the COMMANDS exchange. Bind order.q
        # there too so order-service sees those terminal events.
        for rk in bindings:
            channel.queue_bind(queue=queue, exchange=COMMANDS, routing_key=rk)

    for queue, bindings in COMMAND_QUEUES.items():
        _declare_with_retry(channel, queue, bindings, COMMANDS)

    # orchestrator: replies (on COMMANDS) + the order.placed trigger (on EVENTS).
    _declare_with_retry(channel, "orchestrator.q", ORCH_REPLY_BINDINGS, COMMANDS)
    for rk in ORCH_EVENT_BINDINGS:
        channel.queue_bind(queue="orchestrator.q", exchange=EVENTS, routing_key=rk)
    print(f"  orchestrator.q     <- {ORCH_EVENT_BINDINGS} (on '{EVENTS}')", flush=True)

    for queue, patterns in OBSERVER_QUEUES.items():
        channel.queue_declare(queue=queue, durable=True)
        for pattern in patterns:
            channel.queue_bind(queue=queue, exchange=EVENTS, routing_key=pattern)
        if queue == "observer.q":
            channel.queue_bind(queue=queue, exchange=COMMANDS, routing_key="#")
        print(f"  {queue:<18} (observer/notification, no retry)", flush=True)


def main() -> None:
    conn = connect()
    ch = conn.channel()
    print(
        f"declaring topology: '{EVENTS}' + '{COMMANDS}' (topic) + '{DLX}' (direct)",
        flush=True,
    )
    declare(ch)
    conn.close()
    print("topology ready", flush=True)


if __name__ == "__main__":
    main()
