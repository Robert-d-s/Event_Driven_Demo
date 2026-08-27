// Shapes the dashboard receives from observer. Kept deliberately loose — the
// dashboard is a spectator and shouldn't break if an event gains a field.

export interface BusEvent {
  seen_at: string;
  routing_key: string;
  event_id: string;
  redelivered: boolean;
  order_id: number | null;
  body: Record<string, unknown>;
}

export interface QueueStat {
  name: string;
  ready: number;
  unacked: number;
  consumers: number;
}

// Order lifecycle, derived on the client from the event stream. order-service
// owns the real status; this is just what the dashboard can infer from what it
// has seen fly past.
export type OrderStatus =
  | "PENDING"
  | "PAID"
  | "RESERVED"
  | "SHIPPED"
  | "UNKNOWN";

export interface OrderView {
  order_id: number;
  status: OrderStatus;
  lastSeen: string;
}

// Cross-service consistency snapshot from observer /stats (stage 3 + 4).
export interface Stats {
  orders: number;
  orders_total_cents: number;
  shipped: number;
  payment_rows: number;
  payment_total_cents: number;
  reservations: number;
  stock_consumed: number;
  shipments: number;
  processed_events: Record<string, number>;
  outbox_pending: Record<string, number>;
  consistent: boolean;
}
