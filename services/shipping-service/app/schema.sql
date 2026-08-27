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
