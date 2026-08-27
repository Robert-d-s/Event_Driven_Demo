# Stage 1 — Topology: exchanges, queues, bindings

## The one idea

A publisher never names a queue. It publishes to an **exchange** with a
**routing key**. Consumers create **queues** and **bind** them to the routing-key
patterns they care about. The exchange copies each message into every queue whose
binding matches.

```
  order-service ──"order.placed"──▶ ┌─ exchange "orders" (topic) ─┐
                                     │                             │
              binding "order.placed" │  ┌────────────┐             │
                        ────────────▶│  │ payment.q  │──▶ payment-service ×3
                                     │  └────────────┘             │
        binding "payment.captured"   │  ┌────────────┐             │
                        ────────────▶│  │ inventory.q│──▶ inventory-service
                                     │  └────────────┘             │
                 binding "#"         │  ┌────────────┐             │
                        ────────────▶│  │ observer.q │──▶ observer  │
                                     │  └────────────┘             │
                                     └─────────────────────────────┘
```

The full map is one file:
[infra/topology.py](../infra/topology.py). Read it top to bottom — every queue
and every binding in the system is in the `QUEUES` dict.

## Topic exchange + routing key patterns

The `orders` exchange is a **topic** exchange. Routing keys are dot-separated
words; binding patterns can use wildcards:

| Pattern | Matches |
|---|---|
| `order.placed` | exactly that key |
| `order.*` | `order.placed`, `order.shipped` — one word |
| `order.#` | `order.placed`, `order.anything.here` — zero or more words |
| `#` | everything (this is how `observer` sees the whole bus) |

## The distinction people get wrong

**Different services → different queues → each gets its own copy.**

`payment.q`, `inventory.q`, `notification.q` are separate queues. When
`OrderPlaced` is published, `payment.q` and `notification.q` each receive a full
copy. They're independent.

**Replicas of one service → one shared queue → messages split between them.**

`payment-service` runs as **3 replicas** (`deploy.replicas: 3` in
[docker-compose.yml](../docker-compose.yml)). All three open a consumer on the
**same** queue, `payment.q`. RabbitMQ hands each message to exactly **one** of
them. This is a *work queue* (a.k.a. *competing consumers*) — how you scale
throughput.

Same broker, same exchange. The only difference is how many queues exist.

## Run it

```bash
make up
make demo-stage-1     # places 20 orders
```

Then compare the two logs:

```bash
docker compose logs payment-service | grep charging
#   -> ~20 lines TOTAL, split across payment-service-1/-2/-3
#      (each line is tagged with the replica's hostname)

docker compose logs notification-service | grep "Order #"
#   -> 20 "Order received" lines — notification-service saw ALL of them
```

On the dashboard: the **Queues** panel shows `payment.q` with **3 consumers**
(`3c`), while `notification.q` shows `1c`.

## prefetch — why the split is fair

In [libs/pyevents/pyevents/consumer.py](../libs/pyevents/pyevents/consumer.py)
the consume loop calls `basic_qos(prefetch_count=1)`. That tells the broker:
*don't send this consumer another message until it acks the current one.*

Without it, RabbitMQ round-robins messages to consumers the instant they connect —
so if replica 1 is briefly slow, it still got handed its share up front and they
pile up behind it while replicas 2 and 3 sit idle. `prefetch=1` makes a slow
consumer simply receive fewer messages. Try changing it to `50`, redeploy, and
flood 200 orders — you'll see one replica hog the work.

## Acknowledgements (the setup for stage 2)

The consume loop acks a message only *after* the handler returns successfully:

```python
handler(msg)                       # do the work
ch.basic_ack(delivery_tag=...)     # only now is the message gone from the queue
```

If the handler raises, we `basic_nack(requeue=False)` — drop it. If the process
crashes before either call, the broker eventually redelivers the message to
another consumer. That "redelivered, maybe to someone else" behaviour is the
whole subject of stage 2.

## Checklist — stage 1 is working when

- [ ] `make demo-stage-1` puts 20 rows in the dashboard event stream
- [ ] `docker compose logs payment-service` shows ~20 `charging` lines split
      across three replica hostnames
- [ ] `docker compose logs notification-service` shows 20 lines (all of them)
- [ ] the Queues panel shows `payment.q` at `3c`, `notification.q` at `1c`
- [ ] in the RabbitMQ UI (:15672 → Exchanges → `orders`) you can see the
      bindings listed
