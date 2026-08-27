#!/usr/bin/env bash
# Place N orders against order-service. Used by the `make demo-*` targets.
#
#   scripts/place_orders.sh [N]   (default 20)
set -euo pipefail

N="${1:-20}"
URL="${ORDER_URL:-http://localhost:8000}/orders"

echo "placing $N orders -> $URL"
for i in $(seq 1 "$N"); do
  curl -s -o /dev/null -w "  order %{http_code}\n" \
    -X POST "$URL" \
    -H 'content-type: application/json' \
    -d "{\"customer_id\":\"cust-$((RANDOM % 900 + 100))\",\"items\":[{\"sku\":\"WIDGET-1\",\"qty\":1}],\"total_cents\":$((RANDOM % 9000 + 1000))}"
done
echo "done — watch http://localhost:5173"
