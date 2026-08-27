-- payment-service's own database (payment_db). Nothing else touches it.

-- The idempotency ledger. pyevents.process_once writes here.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Real state, so a double-processed OrderPlaced has a visibly wrong result:
-- charged_cents would be counted twice.
CREATE TABLE IF NOT EXISTS payments (
    order_id      BIGINT PRIMARY KEY,
    charged_cents BIGINT NOT NULL,
    charged_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Running total of everything ever charged. The demo watches this number:
-- with idempotency on it matches the sum of order totals; without it, it drifts.
CREATE TABLE IF NOT EXISTS ledger (
    id            INTEGER PRIMARY KEY DEFAULT 1,
    total_charged_cents BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT ledger_singleton CHECK (id = 1)
);
INSERT INTO ledger (id, total_charged_cents) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;

-- Stage 4: the transactional outbox. Handlers INSERT their outgoing events here
-- in the SAME transaction as the state change; a relay (pyevents.relay_loop)
-- publishes them to RabbitMQ and marks them published.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    body         JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
