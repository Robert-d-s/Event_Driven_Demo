-- One Postgres server, one database per service. Nothing is shared.
-- Postgres runs every .sql file in /docker-entrypoint-initdb.d once, on first boot.
--
-- Stages 0-2 don't touch these databases at all (the services just print).
-- They come into play at stage 3 (processed_events) and stage 4 (outbox).
-- Creating them now keeps the compose file stable across stages.

CREATE DATABASE order_db;
CREATE DATABASE payment_db;
CREATE DATABASE inventory_db;
CREATE DATABASE shipping_db;
CREATE DATABASE orchestrator_db;  -- stage 5: the saga state machine + audit log
