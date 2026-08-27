# Event-Driven Demo — task runner
#
# Stage 0-1 targets. Later stages add demo-stage-2 … demo-stage-5.

.PHONY: help up down logs ps rebuild topology dashboard broker \
        demo-stage-0 demo-stage-1 demo-stage-2 demo-stage-3 demo-stage-4 \
        chaos-poison chaos-kill-payment

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## build + start the whole stack
	docker compose up --build -d
	@echo ""
	@echo "  dashboard  -> http://localhost:5173"
	@echo "  broker UI  -> http://localhost:15672  (guest/guest)"
	@echo ""
	@echo "  then: make demo-stage-1"

down: ## stop everything and remove volumes (fresh broker + db next time)
	docker compose down -v

logs: ## tail logs from all services
	docker compose logs -f --tail=50

ps: ## show running containers
	docker compose ps

rebuild: ## rebuild images without cache
	docker compose build --no-cache

topology: ## re-run the topology declaration (safe, idempotent)
	docker compose run --rm topology

dashboard: ## open the dashboard
	open http://localhost:5173

broker: ## open the RabbitMQ management UI
	open http://localhost:15672

demo-stage-0: ## stage 0 — place a single order and watch one event flow
	@echo "STAGE 0: one order end to end. Watch the event stream at :5173"
	./scripts/place_orders.sh 1

demo-stage-1: ## stage 1 — place 20 orders; watch payment split across 3 replicas
	@echo "STAGE 1: 20 orders."
	@echo "  - payment-service has 3 replicas sharing payment.q -> work splits"
	@echo "  - notification-service has its own queue -> sees all 20"
	@echo "  Compare in 'docker compose logs payment-service' vs 'logs notification-service'"
	./scripts/place_orders.sh 20

demo-stage-2: ## stage 2 — fail payments, place orders, watch retries then DLQ
	@echo "STAGE 2: retry + dead-letter."
	@echo "  1. turning ON 'fail payments' (via the control channel)"
	curl -s -XPOST localhost:8001/control -H 'content-type: application/json' \
	  -d '{"target":"payment","action":"fail","value":true}' >/dev/null
	@echo "  2. placing 3 orders"
	./scripts/place_orders.sh 3
	@echo ""
	@echo "  Now watch the dashboard Queues panel:"
	@echo "   - payment.q.retry fills, drains every 5s, fills again (3 attempts)"
	@echo "   - after ~15s, payment.q.dlq gets the 3 messages"
	@echo "  Then turn 'fail payments' back OFF (button, or):"
	@echo "   curl -XPOST localhost:8001/control -H 'content-type: application/json' \\"
	@echo "     -d '{\"target\":\"payment\",\"action\":\"fail\",\"value\":false}'"

demo-stage-3: ## stage 3 — turn on "duplicate everything", place orders, prove consistency holds
	@echo "STAGE 3: idempotency."
	@echo "  1. turning ON 'duplicate everything' — every event is published twice"
	curl -s -XPOST localhost:8001/control -H 'content-type: application/json' \
	  -d '{"target":"all","action":"duplicate","value":true}' >/dev/null
	@echo "  2. placing 10 orders (= 20 of every event on the bus)"
	./scripts/place_orders.sh 10
	@sleep 3
	@echo ""
	@echo "  Consistency snapshot (every row should be ✓):"
	@curl -s localhost:8001/stats | python3 -m json.tool
	@echo ""
	@echo "  orders == payment_rows == reservations == shipments, and"
	@echo "  orders_total_cents == payment_total_cents — despite every event arriving twice."
	@echo "  Turn duplicate mode back off:"
	@echo "   curl -XPOST localhost:8001/control -H 'content-type: application/json' \\"
	@echo "     -d '{\"target\":\"all\",\"action\":\"duplicate\",\"value\":false}'"

demo-stage-4: ## stage 4 — pause the relays, kill a service, watch the outbox survive
	@echo "STAGE 4: the transactional outbox."
	@echo "  1. pausing every outbox relay — staged events will pile up in the DB"
	curl -s -XPOST localhost:8001/control -H 'content-type: application/json' \
	  -d '{"target":"all","action":"pause_relay","value":true}' >/dev/null
	@sleep 1
	@echo "  2. placing 5 orders — each handler commits its work + an outbox row,"
	@echo "     but nothing publishes because the relays are frozen"
	./scripts/place_orders.sh 5
	@sleep 3
	@echo "  3. outbox_pending now (order-service has 5 unpublished OrderPlaced rows):"
	@curl -s localhost:8001/stats | python3 -c "import sys,json; d=json.load(sys.stdin); print('    ', d['outbox_pending'], ' orders=%d shipments=%d' % (d['orders'], d['shipments']))"
	@echo "  4. SIGKILL order-service — its process dies with 5 events still unpublished"
	docker compose kill --signal=SIGKILL order-service
	@sleep 2
	@echo "     (pre-outbox: those 5 OrderPlaced events would be gone; orders stuck at PENDING forever)"
	@echo "  5. un-pausing relays + restarting order-service"
	curl -s -XPOST localhost:8001/control -H 'content-type: application/json' \
	  -d '{"target":"all","action":"pause_relay","value":false}' >/dev/null
	docker compose up -d order-service
	@sleep 12
	@echo "  6. final state:"
	@curl -s localhost:8001/stats | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"     orders={d['orders']} shipments={d['shipments']} consistent={d['consistent']} outbox_pending={d['outbox_pending']}\")"
	@echo "  All 5 orders completed. The events were durably on disk the whole time —"
	@echo "  the relay picked up where it left off after the crash."

chaos-poison: ## send an unparseable message — goes straight to DLQ, no retries
	./scripts/poison.sh

chaos-kill-payment: ## SIGKILL one payment replica mid-work — watch redelivery
	@echo "killing one payment-service replica (no graceful shutdown)"
	docker compose kill --signal=SIGKILL payment-service
	@echo "compose will restart it. Any message it held unacked is redelivered."
	docker compose up -d payment-service
