# Stage 3 — Duplicates (idempotent consumers)

Stage 2 handed you a guarantee you can't switch off: **at least once**. A crash
between "did the work" and "sent the ack", a retry, the dashboard's duplicate
toggle — any of these delivers the same message to a consumer twice.

If "charge the card" or "reserve stock" runs twice, that's a bug. This stage
makes handlers **idempotent**: running them again changes nothing.

## The idea

You can't prevent duplicates — there's no exactly-once delivery over a network.
Instead you make the *effect* happen once even though *delivery* happens more than
once:

```
BEGIN
  INSERT INTO processed_events (event_id) VALUES (%s)
  ON CONFLICT (event_id) DO NOTHING       -- 0 rows back  => already processed
                                          -- 1 row back   => first time
  -- if first time: do the real work now, in THIS SAME transaction
COMMIT
```

The id-insert and the work **commit together or not at all**. Three cases:

| crash point | what happens on redelivery |
|---|---|
| before COMMIT | nothing was written → redelivery does the work cleanly |
| after COMMIT | the id row is there → redelivery sees it, skips |
| never (happy path) | id + work committed once |

Delivery is at-least-once; the effect is exactly-once.

The helper is [`pyevents.process_once`](../libs/pyevents/pyevents/db.py) — a
context manager that yields `True` on the first delivery and `False` on a
duplicate, and commits or rolls back for you.

```python
with process_once(db, msg.event_id) as first_time:
    if not first_time:
        return                       # duplicate — with-block already rolled back
    cur.execute("UPDATE stock SET qty = qty - 1 WHERE sku = %s", (sku,))
# COMMIT happens here
```

## Now the services have real state

Stages 0–2 just printed. Now each consumer writes to **its own Postgres
database** (`payment_db`, `inventory_db`, `shipping_db`, `order_db` — nothing
shared), so a double-processed message has a *visibly wrong* result:

| service | state | what a duplicate would do |
|---|---|---|
| payment | `payments` row + `ledger.total_charged_cents` | charge twice → ledger drifts up |
| inventory | `stock.qty`, `reservations` | reserve twice → stock over-decrements |
| shipping | `shipments` row | ship twice → 2nd `OrderShipped` emitted |
| order | `orders.status` | (naturally idempotent, but guarded for uniformity) |

## Two subtleties every handler gets right

**1. Derived events use deterministic ids.** When payment emits `PaymentCaptured`
it uses `event_id = "pay-<order_id>"`, not a random uuid. So if payment's handler
runs twice, inventory sees the *same* id both times and dedupes it too. A fresh
uuid per run would break every downstream consumer's idempotency.

**2. Derived events are re-published even on a duplicate.** If payment charged the
card and then crashed *before* publishing `PaymentCaptured`, the redelivery hits
"already processed", skips the charge (correct) — but still must emit
`PaymentCaptured`, or the order stalls forever with nobody downstream hearing
about it. Re-publishing is safe because of (1).

> This re-publish-on-duplicate dance is a workaround. The clean fix — writing the
> outgoing event to the database in the *same* transaction as the work — is
> Stage 4 (the outbox). Each stage sets up the next.

## See it

```bash
make up          # now includes postgres + per-service DBs
make dashboard
make demo-stage-3
```

`demo-stage-3` turns on **"duplicate everything"** (every service publishes each
event twice) and places 10 orders — so 20 of every event hits the bus. Then it
prints the consistency snapshot. Every row is `✓`:

```
orders            = 10
payment_rows      = 10   ✓
reservations      = 10   ✓
shipments         = 10   ✓
Σ order totals    = $X
Σ charged         = $X   ✓   (no drift, despite every charge event arriving twice)
stock consumed    = 10   ✓
```

The dashboard's **Cross-service consistency** panel shows this live, and the
header badge flips to `INCONSISTENT` (red) the instant any row diverges.

### The kill test, revisited

Stage 2's "kill a slow consumer mid-message" — the one where the handler ran
twice — now has the right outcome:

```bash
curl -XPOST localhost:8001/control -H 'content-type: application/json' \
  -d '{"target":"inventory","action":"slow_ms","value":6000}'
# note the reservations count on the dashboard, then:
./scripts/place_orders.sh 1
sleep 2
docker compose kill --signal=SIGKILL inventory-service
docker compose up -d inventory-service
```

`inventory-service` logs `reserved 1x WIDGET-1 for order N` then, after restart,
`duplicate PaymentCaptured for order N — not reserving again`. The reservations
count goes up by exactly 1. Before Stage 3 it went up by 2.

Reset: `curl ... -d '{"target":"inventory","action":"slow_ms","value":0}'`

## What idempotency does NOT fix

`process_once` protects one service's own database. It does **not** make the
charge-then-publish sequence atomic — if the process dies between `COMMIT` and
the `PaymentCaptured` publish, the re-publish-on-duplicate hack covers it, but
only because we remembered to write that hack. Stage 4 removes the hack.

## Checklist — stage 3 is working when

- [ ] `make demo-stage-3` → consistency snapshot all `✓` with duplicate mode on
- [ ] logs show hundreds of `duplicate ... — not <verb>ing again` lines
- [ ] dashboard consistency panel stays green under duplication
- [ ] kill inventory mid-message → reservations goes up by 1, not 2
- [ ] `payment_db.ledger.total_charged_cents` equals the sum of order totals
      exactly (check in psql: `psql postgresql://demo:demo@localhost:55432/payment_db -c 'select * from ledger'`)

## Next

Stage 4 — the transactional outbox. The `INSERT INTO processed_events` +
`UPDATE stock` are atomic, but the `publish()` that follows is a separate system.
Crash in that gap and the effect happened but the event didn't. The outbox writes
the event to a table in the same transaction, and a relay publishes it after.
