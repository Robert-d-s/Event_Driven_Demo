-- orchestrator's own database (orchestrator_db). The sagas + saga_log + outbox
-- + processed_events tables are all created by pyevents.saga.SAGA_DDL, run at
-- startup. This file is a placeholder so the service's run_script call has
-- something to point at and the schema stays greppable next to the code.

-- (intentionally empty — see pyevents/saga.py SAGA_DDL)
SELECT 1;
