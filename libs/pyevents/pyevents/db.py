"""
Minimal Postgres helper — a connection and the idempotency check.

Stage 3 is about surviving duplicate deliveries. Stage 2 guaranteed them: a
crash-redelivery, a retry, a network blip — any of these can hand a consumer the
same message twice. If "charge the card" runs twice, that's a bug.

We can't prevent duplicates (there's no exactly-once over a network). Instead we
make the second run a no-op:

  BEGIN
    -- has this event_id been processed by THIS service before?
    INSERT INTO processed_events (event_id) VALUES (%s)
    ON CONFLICT (event_id) DO NOTHING      -- returns 0 rows if already there
    -- if 0 rows: this is a duplicate. roll back, ack, done.
    -- if 1 row:  do the real work now, in this same transaction.
  COMMIT

The INSERT and the work commit together or not at all. If the process dies after
the work but before COMMIT, nothing was written — the redelivery re-runs cleanly.
If it dies after COMMIT, the redelivery sees the row and skips. Either way: the
effect happens exactly once, even though delivery is at-least-once.

Plain psycopg (v3), hand-written SQL, no ORM — the transaction boundary is the
whole lesson here and it should be impossible to miss.
"""

from __future__ import annotations

import contextlib
import os
import time
from typing import Iterator

import psycopg

# Each service passes its own DSN. Defaults match docker-compose's postgres.
DB_URL = os.environ.get("DB_URL", "postgresql://demo:demo@localhost:5432/postgres")


def connect_db(*, retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    """Connect to Postgres, retrying while the container boots."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg.connect(DB_URL, autocommit=False)
            print(f"[pyevents.db] connected on attempt {attempt}", flush=True)
            return conn
        except psycopg.OperationalError as err:
            last_err = err
            print(
                f"[pyevents.db] postgres not ready ({attempt}/{retries}), "
                f"retrying in {delay}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError("could not connect to postgres") from last_err


PROCESSED_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id    TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_processed_events(conn: psycopg.Connection) -> None:
    """Create the processed_events table if it isn't there yet."""
    with conn.cursor() as cur:
        cur.execute(PROCESSED_EVENTS_DDL)
    conn.commit()


def run_script(conn: psycopg.Connection, sql_text: str) -> None:
    """
    Execute a multi-statement SQL script (a schema.sql file read from disk) and
    commit. psycopg's execute() is typed for LiteralString; a file's contents are
    a runtime str, so this is the one sanctioned place we pass one through.
    """
    with conn.cursor() as cur:
        cur.execute(sql_text)  # type: ignore[arg-type]  # DDL script from a trusted file
    conn.commit()


@contextlib.contextmanager
def process_once(conn: psycopg.Connection, event_id: str) -> Iterator[bool]:
    """
    Run the body exactly once for `event_id`, atomically.

    Usage:

        with process_once(conn, msg.event_id) as first_time:
            if not first_time:
                return                      # duplicate — the with-block rolled back
            cur.execute("UPDATE ...")        # your real work
        # commit happens here, on clean exit

    Yields True if this is the first time we've seen event_id (caller should do
    the work), False if it's a duplicate (caller should bail). On any exception
    the transaction is rolled back and the exception propagates — the message
    then goes down the retry path and can be tried again safely.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_events (event_id) VALUES (%s) "
                "ON CONFLICT (event_id) DO NOTHING",
                (event_id,),
            )
            is_first = cur.rowcount == 1

        yield is_first

        if is_first:
            conn.commit()
        else:
            # Nothing to commit; make sure we don't hold a transaction open.
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
