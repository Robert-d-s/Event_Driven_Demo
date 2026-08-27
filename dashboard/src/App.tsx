import { useMemo, useState } from "react";
import { useEventStream } from "./useEventStream";
import { useQueues } from "./useQueues";
import { useStats } from "./useStats";
import type { BusEvent, OrderStatus, OrderView } from "./types";

const money = (cents: number) => `$${(cents / 100).toFixed(2)}`;

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

type Toggles = {
  paymentFail: boolean;
  shippingFail: boolean;
  inventorySlow: boolean;
  duplicate: boolean;
  pauseRelay: boolean;
};

export function App() {
  const { events, connected } = useEventStream();
  const queues = useQueues();
  const stats = useStats();
  const orders = useMemo(() => deriveOrders(events), [events]);
  const [busy, setBusy] = useState(false);
  const [toggles, setToggles] = useState<Toggles>({
    paymentFail: false,
    shippingFail: false,
    inventorySlow: false,
    duplicate: false,
    pauseRelay: false,
  });

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

  async function sendControl(
    target: string,
    action: string,
    value: boolean | number,
  ) {
    await fetch("/api/observer/control", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ target, action, value }),
    });
  }

  function toggle(key: keyof Toggles, send: (v: boolean) => void) {
    const v = !toggles[key];
    setToggles((t) => ({ ...t, [key]: v }));
    send(v);
  }

  const dlqTotal = queues
    .filter((q) => q.name.endsWith(".dlq"))
    .reduce((sum, q) => sum + q.ready, 0);

  return (
    <div className="app">
      <header>
        <h1>Event-Driven Demo</h1>
        <span className={connected ? "dot ok" : "dot bad"}>
          {connected ? "observer connected" : "observer offline"}
        </span>
        {dlqTotal > 0 && (
          <span className="dot bad" title="messages in dead-letter queues">
            {dlqTotal} in DLQ
          </span>
        )}
        {stats && (
          <span
            className={stats.consistent ? "dot ok" : "dot bad"}
            title="order totals vs. what each service recorded"
          >
            {stats.consistent ? "consistent" : "INCONSISTENT"}
          </span>
        )}
      </header>

      <div className="controls">
        <button disabled={busy} onClick={() => placeOrders(1)}>
          Place 1 order
        </button>
        <button disabled={busy} onClick={() => placeOrders(20)}>
          Place 20 orders
        </button>
        <span className="sep" />
        <button
          className={toggles.duplicate ? "toggle on" : "toggle"}
          title="every service publishes each event twice"
          onClick={() =>
            toggle("duplicate", (v) => sendControl("all", "duplicate", v))
          }
        >
          {toggles.duplicate ? "✓ " : ""}duplicate everything
        </button>
        <button
          className={toggles.paymentFail ? "toggle on" : "toggle"}
          onClick={() =>
            toggle("paymentFail", (v) => sendControl("payment", "fail", v))
          }
        >
          {toggles.paymentFail ? "✓ " : ""}fail payments
        </button>
        <button
          className={toggles.shippingFail ? "toggle on" : "toggle"}
          onClick={() =>
            toggle("shippingFail", (v) => sendControl("shipping", "fail", v))
          }
        >
          {toggles.shippingFail ? "✓ " : ""}fail shipping
        </button>
        <button
          className={toggles.inventorySlow ? "toggle on" : "toggle"}
          onClick={() =>
            toggle("inventorySlow", (v) =>
              sendControl("inventory", "slow_ms", v ? 8000 : 0),
            )
          }
        >
          {toggles.inventorySlow ? "✓ " : ""}slow inventory 8s
        </button>
        <button
          className={toggles.pauseRelay ? "toggle on" : "toggle"}
          title="freeze every outbox relay — staged events pile up in the DB instead of being published"
          onClick={() =>
            toggle("pauseRelay", (v) => sendControl("all", "pause_relay", v))
          }
        >
          {toggles.pauseRelay ? "✓ " : ""}pause outbox relays
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
              .filter((q) => !q.name.startsWith("amq.") && !q.name.startsWith("control."))
              .sort((a, b) => a.name.localeCompare(b.name))
              .map((q) => {
                const kind = q.name.endsWith(".dlq")
                  ? "dlq"
                  : q.name.endsWith(".retry")
                    ? "retry"
                    : "work";
                return (
                  <li key={q.name} className={`q-${kind}`}>
                    <span className="qname">{q.name}</span>
                    <span className="bar">
                      <i style={{ width: `${Math.min(100, q.ready * 6)}%` }} />
                    </span>
                    <span className="qcount">
                      {q.ready}
                      {q.unacked > 0 && (
                        <em title="unacknowledged (in flight)"> +{q.unacked}</em>
                      )}
                    </span>
                    <span className="qcons">{q.consumers}c</span>
                  </li>
                );
              })}
            {queues.length === 0 && (
              <li className="empty">Waiting for broker…</li>
            )}
          </ul>
          <p className="legend">
            <i className="swatch work" /> work&nbsp;&nbsp;
            <i className="swatch retry" /> retry (waiting out TTL)&nbsp;&nbsp;
            <i className="swatch dlq" /> dead-letter
          </p>
        </section>

        <section className="panel consistency">
          <h2>Cross-service consistency</h2>
          {!stats && <p className="empty">Waiting for stats…</p>}
          {stats && (
            <table>
              <tbody>
                <tr>
                  <td>orders placed</td>
                  <td className="num">{stats.orders}</td>
                  <td />
                </tr>
                <tr className={stats.orders === stats.payment_rows ? "" : "bad-row"}>
                  <td>payments recorded</td>
                  <td className="num">{stats.payment_rows}</td>
                  <td>{stats.orders === stats.payment_rows ? "✓" : "✗"}</td>
                </tr>
                <tr className={stats.orders === stats.reservations ? "" : "bad-row"}>
                  <td>stock reservations</td>
                  <td className="num">{stats.reservations}</td>
                  <td>{stats.orders === stats.reservations ? "✓" : "✗"}</td>
                </tr>
                <tr className={stats.orders === stats.shipments ? "" : "bad-row"}>
                  <td>shipments</td>
                  <td className="num">{stats.shipments}</td>
                  <td>{stats.orders === stats.shipments ? "✓" : "✗"}</td>
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                <tr>
                  <td>Σ order totals</td>
                  <td className="num">{money(stats.orders_total_cents)}</td>
                  <td />
                </tr>
                <tr
                  className={
                    stats.orders_total_cents === stats.payment_total_cents
                      ? ""
                      : "bad-row"
                  }
                >
                  <td>Σ charged (payment ledger)</td>
                  <td className="num">{money(stats.payment_total_cents)}</td>
                  <td>
                    {stats.orders_total_cents === stats.payment_total_cents
                      ? "✓"
                      : "✗ drifted"}
                  </td>
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                <tr>
                  <td>stock consumed</td>
                  <td className="num">{stats.stock_consumed}</td>
                  <td>
                    {stats.stock_consumed === stats.reservations ? "✓" : "✗"}
                  </td>
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                <tr>
                  <td colSpan={3} className="subhead">
                    outbox — events staged, not yet relayed
                  </td>
                </tr>
                {Object.entries(stats.outbox_pending).map(([svc, n]) => (
                  <tr key={svc} className={n > 0 ? "warn-row" : ""}>
                    <td>{svc}</td>
                    <td className="num">{n}</td>
                    <td>{n > 0 ? "⧗" : "✓"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <p className="legend">
            {toggles.pauseRelay
              ? "relays paused — staged events pile up in the DB. SIGKILL a service now; on restart its relay drains the outbox and the order still completes. Nothing lost."
              : toggles.duplicate
              ? "duplicate mode ON — every event published twice. Rows stay ✓ because consumers dedupe."
              : "outbox rows sit >0 only briefly. Try “pause outbox relays”, place orders, watch them pile up, then un-pause."}
          </p>
        </section>
      </div>
    </div>
  );
}
