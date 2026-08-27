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
