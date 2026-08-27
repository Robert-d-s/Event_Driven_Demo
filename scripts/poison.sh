#!/usr/bin/env bash
# Publish a message to the "orders" exchange whose body is NOT valid JSON.
# consume() can't parse it, so it goes straight to <queue>.dlq without retrying —
# retrying something structurally broken is pointless.
#
# Uses the rabbitmq container's own CLI so we don't need a local AMQP client.
set -euo pipefail

echo "publishing a poison (unparseable) message with routing key order.placed"
docker compose exec -T rabbitmq rabbitmqadmin --non-interactive publish message \
  --exchange orders \
  --routing-key order.placed \
  --payload 'this is not json {{{' \
  --properties '{"delivery_mode":2}'

echo
echo "watch payment.q.dlq on the dashboard — it should get 1 message, with NO retries"
echo "(and payment.q.retry stays empty — poison messages skip the retry cycle)"
