# Event-Driven Architecture — a staged learning project

An order-fulfilment system built across five services on RabbitMQ, in stages.
Each stage introduces **one** failure mode, lets you watch it happen on a live
dashboard, then fixes it.

Coming from Node/React: think of a queue as the opposite of an `EventEmitter`.
`emit` is fire-and-forget inside one process. Here every message is
acknowledged, can be redelivered, survives a restart, and may be handled twice.
Almost all the complexity below is downstream of that one difference.

## Quick start

```bash
make up              # build + start everything (~1-2 min first run)
make dashboard       # http://localhost:5173
make demo-stage-1    # place 20 orders and watch them flow
make demo-stage-2    # fail payments; watch retries then dead-letter queue
make demo-stage-3    # duplicate every event; watch consistency hold
make demo-stage-4    # pause outbox relays, kill a service, watch events survive
make demo-stage-5    # fail shipping on a charged order; watch it compensate
make down            # stop, wipe broker + db
```

RabbitMQ management UI: http://localhost:15672 (guest / guest).
Postgres: `docker compose exec postgres psql -U demo -d <service>_db`
(or publish a host port — see the `postgres` service in docker-compose.yml)

## Branches — one per stage

This repo is a staged reference. Each stage is a branch you can check out and run
independently; `main` always holds the latest.

| Branch | Stage |
|---|---|
| `stage-0-1` | skeleton + topology (exchanges/queues/bindings, work queues) |
| `stage-2` | acks, retries, backoff, dead-letter queues, failure toggles |
| `stage-3` | duplicate-safe (idempotent) consumers + per-service Postgres |
| `stage-4` | transactional outbox — events written with the work, a relay publishes |
| `stage-5` | orchestration — a saga owns the workflow, compensates on failure |

```bash
git checkout stage-5 && make down && make up   # switch stages
```

Queue definitions and DB schemas differ between stages, so always `make down`
before switching branches.

**You are on: `stage-5`.**

## The system

```
  POST /orders
       │
  order-service ──OrderPlaced──▶┌───────────────┐
  payment-service   ◀───────────│  exchange     │
       │  PaymentCaptured ─────▶│  "orders"     │
  inventory-service ◀───────────│  (topic)      │
       │  StockReserved ──────▶ │               │
  shipping-service  ◀───────────│               │
       │  OrderShipped ───────▶ └───────┬───┬───┘
       ▼                                ▼   ▼
  (order marked SHIPPED)      notification   observer ──▶ dashboard
                              (TypeScript)
```

Nothing shares a database. `notification-service` is deliberately TypeScript, to
keep the events an honest language-neutral contract (see [contracts/](contracts/)).

## Stages

| # | Topic | Doc | Status |
|---|---|---|---|
| 0 | Walking skeleton — one message, end to end, on screen | [docs/stage-0.md](docs/stage-0.md) | ✅ built |
| 1 | Exchanges, queues, bindings; work queues vs. fan-out | [docs/stage-1.md](docs/stage-1.md) | ✅ built |
| 2 | Acks, retries, backoff, dead-letter queues | [docs/stage-2.md](docs/stage-2.md) | ✅ built |
| 3 | Duplicate-safe (idempotent) consumers | [docs/stage-3.md](docs/stage-3.md) | ✅ built |
| 4 | The transactional outbox | [docs/stage-4.md](docs/stage-4.md) | ✅ built |
| 5 | Orchestration — a saga owns the workflow, compensates on failure | [docs/stage-5.md](docs/stage-5.md) | ✅ built |

That's the series. Not covered — Kafka, event sourcing, CQRS, schema registries,
distributed tracing, Kubernetes. All real, all build on the above, none a
prerequisite for using this architecture well.

## Layout

```
docker-compose.yml      rabbitmq, postgres, all services, dashboard
Makefile                up / down / logs / demo-stage-N
contracts/              JSON Schema per event — the wire contract
infra/
  topology.py           every exchange/queue/binding, in one file
  postgres/init.sql     one database per service
libs/pyevents/          shared TRANSPORT only (connect / publish / consume)
services/
  order-service/        Python — FastAPI; POST /orders → order row + outbox
  orchestrator/         Python — owns each order's saga (stage 5)
  payment-service/      Python — command handler, runs 3 replicas
  inventory-service/    Python — command handler
  shipping-service/     Python — command handler
  notification-service/ TypeScript — amqplib
  observer/             Python — binds "#", WebSocket + /stats + /saga to the dashboard
dashboard/              Vite + React + TypeScript
scripts/                place_orders.sh — used by the demo targets
```

## Tooling choices (and why)

| Choice | Why |
|---|---|
| `pika`, synchronous | lowest-level standard client — nothing hidden. You see the ack decision on the line it's made. |
| RabbitMQ | queue semantics (per-message ack, DLQ, TTL) are exactly what stages 2–5 are about. |
| Postgres, hand-written SQL | the outbox (stage 4) is about one transaction boundary; an ORM's session/flush behaviour blurs it. |
| DB per service | so "no shared database" is enforced, not just intended. |
