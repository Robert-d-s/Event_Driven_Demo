# Stage 2 — Failure: acknowledgements, retries, dead letters

Stage 1 got messages flowing. This stage is about what happens when a consumer
**can't** process one — and it's the stage that changes how you think about
queues, because it forces the "at least once" guarantee into the open.

## Part 1 — acknowledgements and "at least once"

A message is removed from a queue only when the consumer **acknowledges** it
(`basic_ack`). Until then the broker considers it *unacknowledged* — handed out
but not confirmed done.

If the consumer dies before acking, the broker puts the message back and
redelivers it — to the same consumer when it restarts, or to another replica.

**This is why the guarantee is "at least once", never "exactly once".** A crash
in the window between "did the work" and "sent the ack" means the work runs
again on redelivery. You cannot close that window — you can only make the work
safe to repeat (that's Stage 3).

### See it

```bash
make up
# make one consumer slow so there's a window to kill it in
curl -XPOST localhost:8001/control -H 'content-type: application/json' \
  -d '{"target":"inventory","action":"slow_ms","value":8000}'

make demo-stage-0          # place one order
# within 8s, while inventory is "reserving stock":
docker compose kill --signal=SIGKILL inventory-service
docker compose up -d inventory-service

docker compose logs inventory-service | grep "reserving stock"
#   -> "reserving stock for order N"  appears TWICE
#      once before the kill, once after restart — the unacked message came back
```

The order still completes. The broker's redelivery healed the crash — at the
cost of running the handler twice.

Reset: `curl -XPOST localhost:8001/control -d '{"target":"inventory","action":"slow_ms","value":0}' -H 'content-type: application/json'`

## Part 2 — retries with backoff, and the dead-letter queue

A crash is one failure mode. The other is a handler that **keeps** failing —
payment gateway down, a bug, malformed data.

`basic_nack(requeue=True)` redelivers *immediately*, so a message that always
fails spins in a tight loop and pins the consumer. We need "try again, but in a
few seconds, and give up eventually".

RabbitMQ has no native "redeliver later", so we build it out of two primitives:

- **per-message TTL** (`x-message-ttl`) — a message expires from a queue after N ms
- **dead-lettering** (`x-dead-letter-exchange`) — an expired (or rejected) message
  is republished to another exchange instead of being dropped

### The topology

Every consumer queue (`payment.q`, `inventory.q`, `shipping.q`, `order.q`) gets
two companions, declared in [infra/topology.py](../infra/topology.py):

```
                        handler raises
  payment.q ───────────────────────────────┐
     ▲                                      ▼
     │                       consume() republishes to
     │                                      │
     │     orders.dlx ──"payment.q.retry"──▶ payment.q.retry
     │     (direct)                           │  x-message-ttl = 5000ms
     │                                        │  (just sits here)
     │                                        ▼  expires
     │                       dead-lettered via x-dead-letter-exchange,
     │                       x-dead-letter-routing-key = "payment.q"
     └────── orders.dlx ──"payment.q"─────────┘   back for another attempt

  After 3 attempts, consume() republishes to "payment.q.dead" instead:

           orders.dlx ──"payment.q.dead"──▶ payment.q.dlq   (no consumer — terminal)
```

Two design decisions worth pausing on:

**Why a separate `orders.dlx` (direct) exchange, not the `orders` topic exchange?**
If retries went back through `orders` with key `order.placed`, every retry would
*also* re-hit `notification.q` and `observer.q` — the customer gets "order
received!" three times. Routing retries by **queue name** on a private direct
exchange keeps a retry invisible to everyone except the consumer that failed.

**Why does `consume()` count attempts with its own `x-retry-count` header?**
The broker keeps an `x-death` header with a count, but we *re-publish* the
message on each failure (`basic_publish`), and to the broker that's a brand-new
message — the `x-death` chain doesn't survive a manual re-publish cleanly. A
header we own and increment ourselves is unambiguous. See
[libs/pyevents/pyevents/consumer.py](../libs/pyevents/pyevents/consumer.py).

### See it

```bash
make demo-stage-2
```

That turns on "fail payments" and places 3 orders. Watch the dashboard **Queues**
panel (retry queues are amber, DLQs are red):

| time | what you see |
|---|---|
| 0s | `payment.q.retry` jumps to 3 |
| ~5s | drains to 0, then back to 3 (attempt 2 — expired, re-tried, failed again) |
| ~10s | drains, back to 3 (attempt 3) |
| ~15s | `payment.q.retry` → 0, **`payment.q.dlq` → 3** |

And in the logs, one message's journey:

```
docker compose logs payment-service | grep "event_id=<pick one>"
  attempt 1/3 failed on order.placed …, retrying in a few s
  attempt 2/3 failed on order.placed …, retrying in a few s
  attempt 3/3 failed on order.placed … -> payment.q.dlq
```

Note the routing key stays `order.placed` the whole way (preserved in
`x-original-routing-key`), and different replicas pick up different attempts —
because the message genuinely re-enters `payment.q` and gets load-balanced again.

Turn it back off: the "fail payments" button, or `make demo-stage-2` prints the
curl command.

## Part 3 — poison messages

A message whose body isn't even valid JSON can never succeed. Retrying it is
pointless — it goes straight to the DLQ.

```bash
make chaos-poison
#   publishes 'this is not json {{{' with routing key order.placed
#   -> payment.q.dlq gets 1 message
#   -> payment.q.retry stays empty (no retry cycle for structurally-broken input)
```

## Part 4 — backpressure

Turn on the inventory slow toggle and flood orders:

```bash
curl -XPOST localhost:8001/control -H 'content-type: application/json' \
  -d '{"target":"inventory","action":"slow_ms","value":8000}'
./scripts/place_orders.sh 20
```

`inventory.q` climbs to ~20 and drains one message every 8s. That backlog **is**
backpressure — the queue absorbing the mismatch between how fast orders arrive
and how fast inventory can handle them. Nothing is lost; it's just slow. With
`prefetch=1`, the broker never hands inventory a second message early, so the
depth number is an honest measure of how far behind it is.

## What's in the DLQ is your problem now

The DLQ has no consumer by design — it's where messages go to wait for a human.
In a real system you'd alert on `*.dlq` depth > 0, inspect the messages in the
RabbitMQ UI (Queues → `payment.q.dlq` → Get messages), fix the cause, and either
re-publish them or write them off.

Try it: after `make demo-stage-2`, open http://localhost:15672 → Queues →
`payment.q.dlq` → Get Message(s), and read the `x-retry-count` header and the
body.

## Checklist — stage 2 is working when

- [ ] killing a slow consumer mid-message → the message is redelivered, handler
      runs twice, order still completes
- [ ] `make demo-stage-2` → `payment.q.retry` cycles 3× at 5s intervals, then 3
      messages land in `payment.q.dlq`
- [ ] a single message's logs show `attempt 1/3 → 2/3 → 3/3 → dlq`
- [ ] `make chaos-poison` → 1 in `payment.q.dlq`, `payment.q.retry` untouched
- [ ] slow inventory + 20 orders → `inventory.q` backs up and drains steadily

## Next

Stage 3: every failure mode above (crash redelivery, retry, poison re-publish)
can hand a consumer the **same message twice**. Right now `payment-service` would
charge the card twice. Stage 3 makes handlers idempotent — safe to run again.
