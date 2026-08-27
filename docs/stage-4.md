# Stage 4 — The transactional outbox

Stage 3 made consumers safe to run twice. But look at what every handler still
does:

```python
with handle_once(db, event_id) as u:       # Postgres
    u.cur.execute("UPDATE ...")            # Postgres
# COMMIT
publish(ch, ...)                           # RabbitMQ  ← different system
```

The `COMMIT` and the `publish` are two separate systems. **There is no way to
make them atomic.** Crash in the gap — after the commit, before the publish — and
the state changed but nobody downstream ever hears. An order charged and never
shipped. Forever.

It needs a crash in a millisecond-wide window, which is exactly why this one
reaches production: it almost never fires, until it does, and then it's a silent
permanent inconsistency with no error in any log.

Stage 3 papered over it with "re-publish the event even on a duplicate" in every
handler. That works only as long as every handler remembers to do it.

## The fix: use one system, not two

Stop trying to be atomic across Postgres and RabbitMQ. Write the event **to a
Postgres table**, in the same transaction as the work:

```sql
BEGIN
  INSERT INTO processed_events (event_id) ...      -- idempotency (stage 3)
  UPDATE payments SET ...                          -- the work
  INSERT INTO outbox (event_id, routing_key, body) -- the event, as a ROW
COMMIT          -- all three, or none
```

Then a separate **relay** loop polls the `outbox` table and publishes each
unpublished row to RabbitMQ, marking it published afterward:

```
relay:  SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id
        FOR UPDATE SKIP LOCKED
        → publish each → UPDATE outbox SET published_at = now()
        → COMMIT
```

Now there is **no gap**. The event is committed with the work or not at all. If
the relay crashes mid-batch, the transaction rolls back and the rows are picked
up next run — and a re-publish is harmless because Stage 3 made consumers
idempotent. Each stage depends on the one before it.

## What changed in the code

`pyevents.handle_once` (Stage 4's `process_once`) now yields a **cursor**, so the
handler stages its outgoing events on the same transaction:

```python
with handle_once(db, msg.event_id) as u:
    if not u.first_time:
        return
    u.cur.execute("UPDATE stock SET qty = qty - 1 ...")
    stage_event(u.cur, event_id="stock-42", routing_key="stock.reserved", body={...})
# one COMMIT: processed_events + stock + reservations + outbox row
```

- **No `publish()` call anywhere in a handler.** The re-publish-on-duplicate hack
  is gone.
- Each service runs `pyevents.relay_loop` on a **background thread** (its own
  Postgres + RabbitMQ connections).
- `order-service`'s `POST /orders` no longer touches RabbitMQ at all — it does
  one transaction (INSERT order + INSERT outbox) and returns. The relay does the
  rest.
- Payment runs 3 replicas → 3 relays against the shared `payment_db.outbox`.
  `FOR UPDATE SKIP LOCKED` lets them share the work without publishing anything
  twice.

### One gotcha found during the build

`order-service` runs schema creation from three threads (HTTP init, consumer,
relay). `CREATE TABLE IF NOT EXISTS` is **not** safe under true concurrency —
two sessions pass the "not exists" check and one fails on the system catalog.
Fix: `pyevents.run_script` now takes a `pg_advisory_xact_lock` first, so
whichever thread gets there creates the tables and the rest are no-ops.

## See it

```bash
make up
make dashboard
make demo-stage-4
```

The demo:

1. **Pause every outbox relay** (dashboard: "pause outbox relays"). Events will
   be staged but not published.
2. Place 5 orders. Each `POST /orders` commits an order row + an OrderPlaced
   outbox row — but nothing flows. The dashboard's consistency panel shows
   `outbox → order: 5` in amber.
3. **`SIGKILL order-service`.** Its process dies with 5 events unpublished.
   *Before the outbox, those 5 OrderPlaced events lived only in the publisher's
   memory — gone. The orders would sit at PENDING forever.*
4. Un-pause the relays and restart order-service. Its relay reads the 5
   still-unpublished rows and drains them. All 5 orders flow through to SHIPPED.

Final: `orders=8 shipments=8 consistent=True outbox_pending all 0`.

### Play with it

- Pause relays, flood 50 orders, watch `outbox_pending` climb on the consistency
  panel, then un-pause and watch it drain.
- Pause only after some orders are mid-flight — see partial outbox depth per
  service (`payment`, `inventory`, `shipping` each stage their own events).
- Inspect the table directly:
  ```bash
  docker compose exec postgres psql -U demo -d order_db \
    -c 'select id, routing_key, published_at from outbox order by id;'
  ```

## What the outbox does NOT give you

- **Exactly-once publish.** A crash after `publish()` but before the
  `UPDATE ... published_at` re-sends that row. Fine — consumers dedupe.
- **Cross-service ordering.** Each relay preserves its own service's order (poll
  by `id`); there's no global order, and there doesn't need to be.
- **Low latency.** The relay polls every 0.5s, so an event can sit up to that
  long before publishing. Production setups use `LISTEN/NOTIFY` or logical
  decoding to push instead of poll.

## Checklist — stage 4 is working when

- [ ] normal flow: `outbox_pending` is 0 within a second of placing orders
- [ ] `make demo-stage-4` → 5 rows pile up in `order_db.outbox`, survive a
      SIGKILL, and drain on restart; final state consistent
- [ ] no `publish(` call left in any service handler (`grep -rn 'publish(' services/`)
- [ ] Stage 3 regression: duplicate mode + 10 orders still ends consistent
- [ ] `order_db.outbox` has `published_at` set on every row once idle

## Next

Stage 5 — workflows that roll back. Stages 1–4 are *choreography*: each service
reacts to the previous one's event, nobody's in charge. When shipping fails on an
order that's already been charged and reserved, there's no transaction to roll
back across three services — you have to *undo forward* with compensating events,
and that's when the missing orchestrator becomes obvious.
