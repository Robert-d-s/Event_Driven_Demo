/**
 * notification-service — the one service NOT written in Python.
 *
 * Why it exists: the moment a consumer can't `import` your Python event classes,
 * your events have to be a real contract on the wire — plain JSON, described by
 * the schemas in /contracts. This service is the forcing function for that.
 *
 * What it does: binds its OWN queue (notification.q) to several routing keys, so
 * it sees EVERY interesting event and "sends a notification" (logs one). Because
 * it has its own queue, it gets a copy of every message — unlike the 3 replicas
 * of payment-service which share one queue and split the messages between them.
 *
 * Uses amqplib directly — the Node equivalent of Python's pika. Same AMQP
 * concepts: connection, channel, exchange, queue, bindings, manual ack.
 */

import amqp from "amqplib";

const BROKER_URL =
  process.env.BROKER_URL ?? "amqp://guest:guest@localhost:5672";
const EXCHANGE = "orders";
const QUEUE = "notification.q";
const BINDINGS = [
  "order.placed",
  "payment.captured",
  "stock.reserved",
  "order.shipped",
];

// Human-readable line per event type.
function describe(routingKey: string, body: Record<string, unknown>): string {
  const order = body.order_id;
  switch (routingKey) {
    case "order.placed":
      return `Order #${order} received — we're on it.`;
    case "payment.captured":
      return `Payment for order #${order} confirmed.`;
    case "stock.reserved":
      return `Order #${order} items reserved.`;
    case "order.shipped":
      return `Order #${order} shipped — tracking ${body.tracking_code}.`;
    default:
      return `Order #${order}: ${routingKey}`;
  }
}

async function connectWithRetry(retries = 30, delayMs = 2000): Promise<amqp.ChannelModel> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const conn = await amqp.connect(BROKER_URL);
      console.log(`[notification] connected on attempt ${attempt}`);
      return conn;
    } catch {
      console.log(
        `[notification] broker not ready (${attempt}/${retries}), retrying in ${delayMs}ms`,
      );
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error("could not connect to broker");
}

async function main(): Promise<void> {
  const conn = await connectWithRetry();
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });
  for (const key of BINDINGS) {
    await ch.bindQueue(QUEUE, EXCHANGE, key);
  }

  // One unacked message at a time, same as pika's prefetch=1.
  await ch.prefetch(1);

  console.log(`[notification] waiting for messages on ${QUEUE}`);
  await ch.consume(QUEUE, (msg) => {
    if (!msg) return;
    try {
      const body = JSON.parse(msg.content.toString()) as Record<string, unknown>;
      console.log(
        `[notification] >> ${describe(msg.fields.routingKey, body)}` +
          (msg.fields.redelivered ? "  (redelivered)" : ""),
      );
      ch.ack(msg);
    } catch (err) {
      console.error("[notification] bad message, dropping:", err);
      ch.nack(msg, false, false);
    }
  });
}

main().catch((err) => {
  console.error("[notification] fatal:", err);
  process.exit(1);
});
