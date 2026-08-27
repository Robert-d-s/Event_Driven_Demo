-- shipping-service's own database (shipping_db).

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One shipment per order. A double-processed StockReserved would try to ship
-- twice — with the guard, the second delivery is a no-op and no second
-- OrderShipped goes out.
CREATE TABLE IF NOT EXISTS shipments (
    order_id      BIGINT PRIMARY KEY,
    tracking_code TEXT NOT NULL,
    shipped_at    TIMESTAMPTZ NOT NULL DEFAULT now()
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
