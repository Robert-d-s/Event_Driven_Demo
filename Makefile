# Event-Driven Demo — task runner
#
# Stage 0-1 targets. Later stages add demo-stage-2 … demo-stage-5.

.PHONY: help up down logs ps rebuild topology dashboard broker \
        demo-stage-0 demo-stage-1

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
