"""
The transactional outbox.

--- The problem Stage 4 fixes ------------------------------------------------

Every handler up to now does two things:

    with process_once(db, event_id) as first_time:
        cur.execute("UPDATE ...")        # (1) change state in Postgres
    publish(ch, ...)                     # (2) emit the follow-on event

(1) and (2) are two different systems (Postgres, RabbitMQ) and there is no way
to make them atomic. Crash in the gap between COMMIT and publish() and you get a
state change that nobody downstream ever hears about — an order charged but never
shipped, forever. It needs a crash in a millisecond-wide window, which is exactly
why it survives into production: it almost never happens, until it does, and then
it's a silent permanent inconsistency.

Stage 3 papered over it with "re-publish the event even on a duplicate delivery".
That works only because we remembered to write that hack in every handler.

--- The fix ----------------------------------------------------------------

Stop trying to be atomic across two systems. Use one.

    BEGIN
      INSERT INTO processed_events (event_id) ...       -- idempotency
      UPDATE ...                                        -- the real work
      INSERT INTO outbox (event_id, routing_key, body)  -- the event, as a ROW
    COMMIT            <- all three, or none

A separate **relay** loop polls the outbox table and publishes each unpublished
row to RabbitMQ, marking it published afterwards. If the relay crashes mid-batch
it just re-publishes on restart — and that's fine, because Stage 3 made every
consumer idempotent. Each stage depends on the one before it.

Now there is no gap. The event is committed with the work, or not at all. The
"re-publish on duplicate" hack is gone: handlers just write to the outbox once,
inside the transaction, and never call publish() directly.

--- What the relay does NOT guarantee -------------------------------------

Ordering across services, or exactly-once publish (a crash after publish() but
before the UPDATE that marks the row leaves it to be re-sent). Both are fine
here: consumers dedupe, and single-service ordering is preserved by polling the
outbox in id order with one relay per service.
"""

from __future__ import annotations

import json
import time
from typing import Iterator

import pika.exceptions
import psycopg

from .connection import connect
from .db import run_script
from .publisher import publish

# Flipped by the control channel. When True, every relay stops draining its
# outbox — rows pile up (visible on the dashboard) so you can then kill the
# service and prove the events were safe on disk. Restart clears it.
_RELAY_PAUSED = False


def set_relay_paused(on: bool) -> None:
    global _RELAY_PAUSED
    _RELAY_PAUSED = bool(on)
    print(f"[outbox-relay] paused -> {_RELAY_PAUSED}", flush=True)


def relay_paused() -> bool:
    return _RELAY_PAUSED

OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    body         JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;
"""


def stage_event(
    cur: psycopg.Cursor,
    *,
    event_id: str,
    routing_key: str,
    body: dict,
) -> None:
    """
    Write one event to the outbox using the caller's cursor — so it lands in
    whatever transaction the caller already has open (the same one doing the
    state change). Does NOT commit; the caller's `with` block does.
    """
    cur.execute(
        "INSERT INTO outbox (event_id, routing_key, body) VALUES (%s, %s, %s)",
        (event_id, routing_key, json.dumps(body)),
    )


def relay_loop(
    dsn: str,
    *,
    exchange: str = "orders",
    poll_interval: float = 0.5,
    batch: int = 50,
) -> None:
    """
    Forever: read unpublished outbox rows in id order, publish each, mark it
    published. One connection to Postgres, one to RabbitMQ, both owned here.

    Runs on its own thread in each service (see the services' main()). Its own DB
    connection — never shares the handler's.

    Crash safety, two layers:
      - a row is marked published only after publish() returns, all inside one
        transaction, so a crash before COMMIT rolls the claim back and the rows
        are retried;
      - the RabbitMQ connection is rebuilt if it drops (an idle relay's
        connection can be closed by the broker on missed heartbeats, then a
        burst of rows hits a dead channel). Without this the relay thread would
        die silently and its outbox would stop draining forever.
    Consumers are idempotent (Stage 3), so a re-publish after a reconnect is
    harmless.
    """
    db = psycopg.connect(dsn, autocommit=False)
    run_script(db, OUTBOX_DDL)  # advisory-locked; safe alongside the handler's schema init

    ch = _open_channel(exchange)
    print(f"[outbox-relay] running (exchange={exchange}, poll={poll_interval}s)", flush=True)

    while True:
        if _RELAY_PAUSED:
            time.sleep(poll_interval)
            continue
        try:
            published = _drain_once(db, ch, exchange=exchange, batch=batch)
        except (
            pika.exceptions.AMQPError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ConnectionWrongStateError,
            pika.exceptions.StreamLostError,
        ) as err:
            # The batch's DB transaction was rolled back by _drain_once's own
            # error handling (or never committed), so nothing is marked
            # published prematurely. Rebuild the channel and retry next tick.
            print(f"[outbox-relay] broker connection lost ({err!r}); reconnecting", flush=True)
            db.rollback()
            ch = _open_channel(exchange)
            time.sleep(poll_interval)
            continue
        if published == 0:
            time.sleep(poll_interval)


def _open_channel(exchange: str):
    amqp = connect()
    ch = amqp.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    return ch


def _drain_once(
    db: psycopg.Connection,
    ch,
    *,
    exchange: str,
    batch: int,
) -> int:
    """
    Publish one batch of unpublished rows. Returns how many were published.

    `FOR UPDATE SKIP LOCKED` lets several relays (one per service replica) run
    against the same outbox without stepping on each other: each grabs a
    different set of rows and any row another relay already holds is skipped.
    The whole batch — claim, publish, mark — is one transaction, so if publish()
    throws partway through, the whole claim is rolled back (nothing marked
    published) and the caller reconnects and retries.
    """
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT id, routing_key, body FROM outbox "
                "WHERE published_at IS NULL ORDER BY id LIMIT %s "
                "FOR UPDATE SKIP LOCKED",
                (batch,),
            )
            rows = cur.fetchall()

            if not rows:
                db.rollback()
                return 0

            for row_id, routing_key, body in rows:
                publish(ch, exchange=exchange, routing_key=routing_key, body=body)
                cur.execute(
                    "UPDATE outbox SET published_at = now() WHERE id = %s", (row_id,)
                )

        db.commit()
    except Exception:
        db.rollback()  # release the row claims; nothing was marked published
        raise

    print(f"[outbox-relay] published {len(rows)} event(s)", flush=True)
    return len(rows)
