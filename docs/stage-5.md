# Stage 5 — Workflows that roll back (orchestration + compensation)

Stages 1–4 were **choreography**: order-service emits OrderPlaced, payment reacts
and emits PaymentCaptured, inventory reacts... nobody is in charge. The workflow
exists only as the sum of every service's queue bindings.

That's beautifully decoupled, and it has a cost you should feel before anyone
hands you a fix:

1. **When an order is stuck, no single service can tell you where it is.** You'd
   have to reconstruct the flow from logs across four services.
2. **When a *later* step fails, there's nothing to roll back.** Shipping can't
   dispatch an order that's already been charged and had stock reserved — and
   there is no transaction spanning three services and two databases. The charge
   really happened. The stock really moved.

You have to **undo forward**: issue a refund, release the stock, with explicit
compensating actions in reverse order. The moment you're writing that, the
missing owner is obvious.

## The orchestrator

Stage 5 adds one service — `orchestrator` — that owns each order's **saga** (its
state machine). Instead of services reacting to whatever the previous one emitted,
the orchestrator sends explicit **commands** and waits for **replies**:

```
order.placed ─▶ orchestrator
                    │  cmd.payment.charge ──▶ payment-service
                    │  ◀── reply.payment.charged  (or reply.payment.failed)
                    │  cmd.inventory.reserve ──▶ inventory-service
                    │  ◀── reply.inventory.reserved
                    │  cmd.shipping.dispatch ──▶ shipping-service
                    │  ◀── reply.shipping.dispatched
                    ▼
              order.shipped ──▶ order-service marks the order SHIPPED
```

### The state machine

```
STARTED ──charged──▶ CHARGED ──reserved──▶ RESERVED ──dispatched──▶ COMPLETED
   │                   │                      │
   │ charge fails      │ reserve fails        │ dispatch fails / times out
   ▼                   ▼                      ▼
CANCELLED         COMPENSATING ──────────▶ COMPENSATING
                  (refund)                 (release stock, then refund)
                      │                       │
                      ▼                       ▼
                  CANCELLED               CANCELLED
```

Every command, reply, state change and timeout is written to `saga_log` — the
audit trail the dashboard renders and a human reads. That's the answer to
problem #1: one query tells you exactly where an order is and how it got there.

### Compensation

When a step fails, the orchestrator sends the compensating command for **each
forward step that already succeeded, in reverse order**:

| failed at | compensations sent |
|---|---|
| charge | none — nothing succeeded yet → straight to CANCELLED |
| reserve | `cmd.payment.refund` |
| dispatch | `cmd.inventory.release`, then `cmd.payment.refund` |

Each service handles its compensating command (`cmd.payment.refund` deletes the
payment row and decrements the ledger; `cmd.inventory.release` puts the stock
back) and replies. When the last compensating reply lands, the saga goes
CANCELLED and emits `order.cancelled`.

### The timeout watchdog

A background thread in the orchestrator polls: any saga that has been *awaiting a
reply* for longer than `STEP_TIMEOUT_S` (30s) is treated as a failed step, and
compensation begins. That's what stops a silently-dead service from hanging an
order forever — problem #2's second half.

## What carried over

Every earlier stage's guarantee still holds, now on the command queues:

- **Retry + DLQ** (Stage 2): a command whose handler *raises* (bad data) retries
  then dead-letters. (A *business* failure — "card declined" — is a `reply.*`, not
  an exception; that drives compensation, not retry.)
- **Idempotency** (Stage 3): `handle_once` on every command and reply. A
  redelivered `cmd.payment.charge` must not double-charge.
- **The outbox** (Stage 4): every command and reply is staged in the same
  transaction as the state change. The orchestrator never `publish()`es directly;
  its relay drains `orchestrator_db.outbox` to the `commands` exchange.

## The trade

The orchestrator reintroduces central coupling — it knows about payment,
inventory and shipping, and the order of the steps. That's the actual trade, and
it's why the earlier stages are choreographed first: you should have felt the
decoupling before giving some of it back for a workflow you can see and control.

Choreography and orchestration aren't right/wrong — they're a choice per
workflow. Simple fan-out (notify these five services): choreography. A
multi-step transaction that must roll back as a unit: orchestration.

## See it

```bash
make up
make dashboard        # new "Saga" panel replaces the old Orders tracker
```

Then the dashboard Scenarios row, or `make`:

- **▶ Stage 5 — shipping fails → compensate** / `make demo-stage-5`
  Fails shipping, places 1 order. The saga panel shows: charge ✓ → reserve ✓ →
  dispatch ✗ → COMPENSATING → release + refund → CANCELLED. The consistency
  panel stays green: a cancelled order leaves no payment, no reservation, no
  shipment.

- **▶ Stage 5 — silent inventory → timeout**
  Makes inventory take 90s (effectively dead), places 1 order. Charge succeeds,
  then reserve never answers. After 30s the watchdog fires: `timeout` in the
  saga log → refund → CANCELLED. Note only the *charge* is compensated — reserve
  never completed, so there's nothing to release.

- **✗ orchestrator** kill button — SIGKILL the orchestrator mid-workflow. On
  restart it reloads every saga from `orchestrator_db.sagas` and the watchdog
  picks up anything that was left awaiting a reply. Nothing is lost — the state
  is in Postgres, not memory.

### Inspect a saga

```bash
docker compose exec postgres psql -U demo -d orchestrator_db \
  -c 'select order_id, kind, detail, at from saga_log order by id;'
```

## Checklist — stage 5 is working when

- [ ] happy path: `make demo-stage-1`-style flow → all sagas COMPLETED, consistent
- [ ] `make demo-stage-5` → saga log shows dispatch fail → release → refund →
      CANCELLED; consistency panel green
- [ ] silent-inventory scenario → `timeout` entry in the saga log → CANCELLED,
      only the charge compensated
- [ ] kill the orchestrator mid-workflow → it recovers from Postgres on restart
- [ ] Stage 3 regression: duplicate mode + 10 orders → all COMPLETED, consistent

## That's the series

Stages 0–5 cover the core of event-driven design: topology, delivery guarantees,
idempotency, the outbox, and the choreography ↔ orchestration choice. Natural
next steps if you want them — event sourcing (the log *is* the database), CQRS
(separate read models), a schema registry, Kafka (compare a partitioned log to a
queue) — build on everything here but aren't prerequisites for using this
architecture well.
