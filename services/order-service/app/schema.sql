-- order-service's own database (order_db).

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGSERIAL PRIMARY KEY,
    customer_id  TEXT NOT NULL,
    total_cents  BIGINT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    placed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Start order ids at 1001 to match the earlier stages' numbering.
SELECT setval(pg_get_serial_sequence('orders', 'order_id'), 1000, true)
WHERE NOT EXISTS (SELECT 1 FROM orders);

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
