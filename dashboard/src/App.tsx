import { useState } from "react";
import { useEventStream } from "./useEventStream";
import { useQueues } from "./useQueues";
import { useStats } from "./useStats";
import { useSaga } from "./useSaga";

const money = (cents: number) => `$${(cents / 100).toFixed(2)}`;

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

const SAGA_STATE_COLOR: Record<string, string> = {
  STARTED: "var(--accent)",
  CHARGED: "var(--accent)",
  RESERVED: "var(--accent)",
  COMPLETED: "var(--ok)",
  COMPENSATING: "var(--warn)",
  CANCELLING: "var(--warn)",
  CANCELLED: "var(--bad)",
};

export function App() {
  const { events, connected } = useEventStream();
  const queues = useQueues();
  const stats = useStats();
  const saga = useSaga();
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

  async function killService(service: string) {
    await fetch("/api/observer/kill", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ service }),
    });
  }

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  // Self-contained scenario runners — no terminal needed.
  const [scenario, setScenario] = useState<string | null>(null);

  async function runOutboxScenario() {
    setScenario("outbox");
    try {
      await sendControl("all", "pause_relay", true);
      setToggles((t) => ({ ...t, pauseRelay: true }));
      await sleep(500);
      await placeOrders(5);
      await sleep(2500);
      await killService("order-service");
      await sleep(2500);
      await sendControl("all", "pause_relay", false);
      setToggles((t) => ({ ...t, pauseRelay: false }));
    } finally {
      setTimeout(() => setScenario(null), 12000);
    }
  }

  async function runCompensationScenario() {
    setScenario("compensation");
    try {
      await sendControl("shipping", "fail", true);
      setToggles((t) => ({ ...t, shippingFail: true }));
      await sleep(500);
      await placeOrders(1);
      // charge → reserve → dispatch fails → release + refund → CANCELLED
      await sleep(8000);
      await sendControl("shipping", "fail", false);
      setToggles((t) => ({ ...t, shippingFail: false }));
    } finally {
      setScenario(null);
    }
  }

  async function runTimeoutScenario() {
    setScenario("timeout");
    try {
      await sendControl("inventory", "silent", true);
      await sleep(500);
      await placeOrders(1);
      // charge ok; inventory reserves the stock but sends no reply → 30s
      // watchdog → release (undoes the reservation) + refund → CANCELLED
      await sleep(40000);
      await sendControl("inventory", "silent", false);
    } finally {
      setScenario(null);
    }
  }

  async function runDuplicateScenario() {
    setScenario("duplicate");
    try {
      await sendControl("all", "duplicate", true);
      setToggles((t) => ({ ...t, duplicate: true }));
      await sleep(500);
      await placeOrders(10);
      await sleep(8000);
      await sendControl("all", "duplicate", false);
      setToggles((t) => ({ ...t, duplicate: false }));
    } finally {
      setScenario(null);
    }
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

      <div className="controls scenarios">
        <span className="scen-label">Scenarios (no terminal):</span>
        <button
          disabled={scenario !== null || busy}
          onClick={runCompensationScenario}
        >
          {scenario === "compensation"
            ? "running…"
            : "▶ Stage 5 — shipping fails → compensate"}
        </button>
        <button
          disabled={scenario !== null || busy}
          onClick={runTimeoutScenario}
        >
          {scenario === "timeout"
            ? "running…"
            : "▶ Stage 5 — silent inventory → timeout"}
        </button>
        <button
          disabled={scenario !== null || busy}
          onClick={runOutboxScenario}
        >
          {scenario === "outbox"
            ? "running…"
            : "▶ Stage 4 — pause relays, kill order-service"}
        </button>
        <button
          disabled={scenario !== null || busy}
          onClick={runDuplicateScenario}
        >
          {scenario === "duplicate" ? "running…" : "▶ Stage 3 — duplicate storm"}
        </button>
        <span className="sep" />
        <span className="scen-label">Kill (SIGKILL + restart):</span>
        {[
          "orchestrator",
          "order-service",
          "payment-service",
          "inventory-service",
          "shipping-service",
        ].map((s) => (
          <button
            key={s}
            className="toggle"
            disabled={scenario !== null}
            onClick={() => killService(s)}
          >
            ✗ {s.replace("-service", "")}
          </button>
        ))}
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

        <section className="panel saga">
          <h2>Saga — the orchestrator's audit log</h2>
          {saga && (
            <div className="saga-states">
              {Object.entries(saga.by_state).map(([s, n]) => (
                <span
                  key={s}
                  className="saga-chip"
                  style={{ borderColor: SAGA_STATE_COLOR[s] ?? "var(--border)" }}
                >
                  {s} {n}
                </span>
              ))}
            </div>
          )}
          <ul className="saga-log">
            {saga?.log.map((e, i) => (
              <li key={i} className={`saga-${e.kind}`}>
                <span className="oid">#{e.order_id}</span>
                <span className="saga-kind">{e.kind}</span>
                <span className="saga-detail">{e.detail}</span>
              </li>
            ))}
            {!saga?.log.length && (
              <li className="empty">
                No sagas yet. Place an order — the orchestrator drives it.
              </li>
            )}
          </ul>
          <p className="legend">
            forward: charge → reserve → dispatch. On failure or a{" "}
            {"30s"} step timeout, compensations run in reverse (release, refund).
          </p>
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
                <tr>
                  <td>
                    &nbsp;&nbsp;completed{" "}
                    <span className="muted-inline">/ cancelled</span>
                  </td>
                  <td className="num">
                    {stats.shipped}{" "}
                    <span className="muted-inline">/ {stats.cancelled}</span>
                  </td>
                  <td />
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                {/* completed orders' footprint should line up exactly;
                    cancelled orders leave nothing (compensation removed it) */}
                <tr className={stats.shipped === stats.payment_rows ? "" : "bad-row"}>
                  <td>payments recorded</td>
                  <td className="num">{stats.payment_rows}</td>
                  <td>{stats.shipped === stats.payment_rows ? "✓" : "✗"}</td>
                </tr>
                <tr className={stats.shipped === stats.reservations ? "" : "bad-row"}>
                  <td>stock reservations</td>
                  <td className="num">{stats.reservations}</td>
                  <td>{stats.shipped === stats.reservations ? "✓" : "✗"}</td>
                </tr>
                <tr className={stats.shipped === stats.shipments ? "" : "bad-row"}>
                  <td>shipments</td>
                  <td className="num">{stats.shipments}</td>
                  <td>{stats.shipped === stats.shipments ? "✓" : "✗"}</td>
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                <tr>
                  <td>Σ completed-order totals</td>
                  <td className="num">{money(stats.shipped_total_cents)}</td>
                  <td />
                </tr>
                <tr
                  className={
                    stats.shipped_total_cents === stats.payment_total_cents
                      ? ""
                      : "bad-row"
                  }
                >
                  <td>Σ charged (payment ledger)</td>
                  <td className="num">{money(stats.payment_total_cents)}</td>
                  <td>
                    {stats.shipped_total_cents === stats.payment_total_cents
                      ? "✓"
                      : "✗ drifted"}
                  </td>
                </tr>
                <tr className="spacer">
                  <td colSpan={3} />
                </tr>
                <tr className={stats.stock_consumed === stats.reservations ? "" : "bad-row"}>
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
            Rows compare the footprint of <em>completed</em> orders. A cancelled
            order leaves nothing — the compensation removed its payment,
            reservation and shipment.
          </p>
        </section>
      </div>
    </div>
  );
}
