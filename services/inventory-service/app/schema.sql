-- inventory-service's own database (inventory_db).

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stock levels. Every order reserves 1 unit of WIDGET-1. A double-processed
-- PaymentCaptured would decrement twice — the demo watches `qty` to see that.
CREATE TABLE IF NOT EXISTS stock (
    sku TEXT PRIMARY KEY,
    qty BIGINT NOT NULL
);
INSERT INTO stock (sku, qty) VALUES ('WIDGET-1', 100000)
    ON CONFLICT (sku) DO NOTHING;

-- One row per reservation, so we can also see reservation count vs. order count.
CREATE TABLE IF NOT EXISTS reservations (
    order_id     BIGINT PRIMARY KEY,
    sku          TEXT NOT NULL,
    qty          BIGINT NOT NULL,
    reserved_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
