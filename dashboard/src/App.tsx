import { useMemo, useState } from "react";
import { useEventStream } from "./useEventStream";
import { useQueues } from "./useQueues";
import type { BusEvent, OrderStatus, OrderView } from "./types";

const STATUS_FROM_KEY: Record<string, OrderStatus> = {
  "order.placed": "PENDING",
  "payment.captured": "PAID",
  "stock.reserved": "RESERVED",
  "order.shipped": "SHIPPED",
};

const STATUS_ORDER: OrderStatus[] = [
  "PENDING",
  "PAID",
  "RESERVED",
  "SHIPPED",
];

// Fold the event stream into a per-order view. Only advances status forward.
function deriveOrders(events: BusEvent[]): OrderView[] {
  const map = new Map<number, OrderView>();
  // events are newest-first; walk oldest-first so status advances naturally
  for (const ev of [...events].reverse()) {
    if (ev.order_id == null) continue;
    const next = STATUS_FROM_KEY[ev.routing_key];
    if (!next) continue;
    const cur = map.get(ev.order_id);
    const curRank = cur ? STATUS_ORDER.indexOf(cur.status) : -1;
    const nextRank = STATUS_ORDER.indexOf(next);
    if (!cur || nextRank > curRank) {
      map.set(ev.order_id, {
        order_id: ev.order_id,
        status: next,
        lastSeen: ev.seen_at,
      });
    }
  }
  return [...map.values()].sort((a, b) => b.order_id - a.order_id);
}

function timeOnly(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour12: false });
}

export function App() {
  const { events, connected } = useEventStream();
  const queues = useQueues();
  const orders = useMemo(() => deriveOrders(events), [events]);
  const [busy, setBusy] = useState(false);

  async function placeOrders(n: number) {
    setBusy(true);
    try {
      for (let i = 0; i < n; i++) {
        await fetch("/api/orders/orders", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            customer_id: `cust-${Math.floor(Math.random() * 900 + 100)}`,
            items: [{ sku: "WIDGET-1", qty: 1 }],
            total_cents: 1000 + Math.floor(Math.random() * 9000),
          }),
        });
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="app">
      <header>
        <h1>Event-Driven Demo</h1>
        <span className={connected ? "dot ok" : "dot bad"}>
          {connected ? "observer connected" : "observer offline"}
        </span>
      </header>

      <div className="controls">
        <button disabled={busy} onClick={() => placeOrders(1)}>
          Place 1 order
        </button>
        <button disabled={busy} onClick={() => placeOrders(20)}>
          Place 20 orders
        </button>
        <a
          href="http://localhost:15672"
          target="_blank"
          rel="noreferrer"
          className="link"
        >
          RabbitMQ management ↗
        </a>
      </div>

      <div className="grid">
        <section className="panel stream">
          <h2>Event stream</h2>
          <ul>
            {events.map((ev, i) => (
              <li key={`${ev.event_id}-${i}`}>
                <span className="ts">{timeOnly(ev.seen_at)}</span>
                <span className={`rk rk-${ev.routing_key.split(".")[0]}`}>
                  {ev.routing_key}
                </span>
                <span className="oid">
                  {ev.order_id != null ? `#${ev.order_id}` : "—"}
                </span>
                {ev.redelivered && <span className="redeliv">redelivered</span>}
              </li>
            ))}
            {events.length === 0 && (
              <li className="empty">
                No events yet. Click “Place 1 order”.
              </li>
            )}
          </ul>
        </section>

        <section className="panel orders">
          <h2>Orders</h2>
          <ul>
            {orders.map((o) => (
              <li key={o.order_id}>
                <span className="oid">#{o.order_id}</span>
                <span className="track">
                  {STATUS_ORDER.map((s) => (
                    <i
                      key={s}
                      className={
                        STATUS_ORDER.indexOf(s) <=
                        STATUS_ORDER.indexOf(o.status)
                          ? "on"
                          : "off"
                      }
                    />
                  ))}
                </span>
                <span className="status">{o.status}</span>
              </li>
            ))}
            {orders.length === 0 && <li className="empty">No orders yet.</li>}
          </ul>
        </section>

        <section className="panel queues">
          <h2>Queues</h2>
          <ul>
            {queues
              .filter((q) => !q.name.startsWith("amq."))
              .map((q) => (
                <li key={q.name}>
                  <span className="qname">{q.name}</span>
                  <span className="bar">
                    <i
                      style={{
                        width: `${Math.min(100, q.ready * 6)}%`,
                      }}
                    />
                  </span>
                  <span className="qcount">
                    {q.ready}
                    {q.unacked > 0 && (
                      <em title="unacknowledged"> +{q.unacked}</em>
                    )}
                  </span>
                  <span className="qcons">{q.consumers}c</span>
                </li>
              ))}
            {queues.length === 0 && (
              <li className="empty">Waiting for broker…</li>
            )}
          </ul>
        </section>
      </div>
    </div>
  );
}
