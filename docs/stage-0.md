# Stage 0 — Walking skeleton

## What this stage is

The smallest thing that proves a message travels end to end and that you can
**see** it. No failure handling, no databases, no retries. Just: an HTTP request
comes in, an event goes out, another service receives it, and the dashboard shows
it happen.

Everything after this stage is about failure. This stage is about having
something on screen to watch failure happen *to*.

## What's running

| Piece | Role |
|---|---|
| `rabbitmq` | the broker |
| `postgres` | created now, unused until stage 3 |
| `topology` | one-shot container: declares the exchange + queues, then exits |
| `order-service` | `POST /orders` → publishes `OrderPlaced` |
| `payment-service` ×3 | consumes `OrderPlaced` → publishes `PaymentCaptured` |
| `inventory-service` | consumes `PaymentCaptured` → publishes `StockReserved` |
| `shipping-service` | consumes `StockReserved` → publishes `OrderShipped` |
| `notification-service` | (TypeScript) consumes everything, logs a message |
| `observer` | consumes everything, feeds the dashboard over WebSocket |
| `dashboard` | React page at :5173 |

## Run it

```bash
make up            # ~1-2 min first time (image builds)
make dashboard     # opens http://localhost:5173
make demo-stage-0  # places ONE order
```

You should see, within a second, four rows appear in the event stream:

```
OrderPlaced     #1001
PaymentCaptured #1001
StockReserved   #1001
OrderShipped    #1001
```

and the order tracker fills all four segments.

## What to notice

**`order-service` never mentions `payment-service`.** Open
[services/order-service/app/api.py](../services/order-service/app/api.py). The
`place_order` handler publishes to an *exchange* called `orders` with routing key
`order.placed` and returns `202 Accepted`. It does not know who, if anyone, is
listening. That decoupling is the entire point — stage 1 explains the mechanism.

**The response says `PENDING`, not `CONFIRMED`.** order-service genuinely doesn't
know the outcome yet. It finds out later, asynchronously, when `OrderShipped`
comes back on a *different* queue (`order.q`) — see
[app/consumer.py](../services/order-service/app/consumer.py).

**One service is not Python.** `notification-service` is TypeScript using
`amqplib`. It works for exactly one reason: the events are plain JSON on the wire,
described by [/contracts](../contracts/), not Python objects. If you ever find
yourself wanting to `import` an event class across services, that's the coupling
this constraint exists to prevent.

## The threading detail (worth understanding once)

`pika` is blocking and its channels are not thread-safe. Consumer-only services
(`payment`, `inventory`, `shipping`) are a single thread in a `start_consuming()`
loop — nothing to worry about.

`order-service` and `observer` also serve HTTP, so they run **two threads, two
connections**: uvicorn on the main thread (publishes), and the consume loop on a
background thread (consumes). They never share a channel. See
[order-service/app/__main__.py](../services/order-service/app/__main__.py).

## The idle-connection gotcha (a real one, hit during the build)

AMQP connections use **heartbeats** — pika's default is 60s, and RabbitMQ closes a
connection after ~2 missed beats (~3 minutes). A `BlockingConnection` only sends
heartbeats when your code hands control back to pika.

- The **consumer** connection is fine: `start_consuming()` loops inside pika, so
  heartbeats go out continuously.
- A **publisher** connection in an HTTP handler is *idle between requests*.
  Nothing calls into pika, no heartbeats go out, and after 3 minutes the broker
  drops it. The next `POST /orders` then throws `ChannelWrongStateError: Channel
  is closed` → HTTP 500, and nothing is published.

This is exactly why you can open the dashboard, leave it a few minutes, click
"Place order", and get silence.

The fix is [`pyevents.Publisher`](../libs/pyevents/pyevents/publisher.py): it owns
the connection, checks `channel.is_open` before each publish, and transparently
reconnects if the broker has dropped it. `order-service` uses it instead of a
bare channel. Watch it happen:

```bash
# place an order, then wait out the timeout, then place another
curl -XPOST localhost:8000/orders -H 'content-type: application/json' \
  -d '{"customer_id":"a","items":[{"sku":"X","qty":1}],"total_cents":500}'
sleep 200
curl -XPOST localhost:8000/orders -H 'content-type: application/json' \
  -d '{"customer_id":"b","items":[{"sku":"X","qty":1}],"total_cents":500}'
docker compose logs order-service | grep Publisher
#   -> "[Publisher] channel dead, reconnecting"  then the publish succeeds
```

Consumer-only services don't need this — but if you later add a service that
consumes *and* publishes on separate connections, the same rule applies to its
publish side.

## Next

Stage 1 makes the exchange/queue/binding mechanism explicit and shows the
difference between "3 services" and "3 replicas of 1 service".
