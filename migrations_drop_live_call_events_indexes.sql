-- Run on bala_billing database.
-- Drops unused secondary indexes to reduce write amplification on live_call_events.

DROP INDEX IF EXISTS idx_live_call_events_caller;
DROP INDEX IF EXISTS idx_live_call_events_callee;
DROP INDEX IF EXISTS idx_live_call_events_customer_id;
DROP INDEX IF EXISTS idx_live_call_events_event_ts;
DROP INDEX IF EXISTS idx_live_call_events_uuid;

-- Verify remaining indexes:
-- \d live_call_events
-- Expected to keep:
--   live_call_events_pkey (id)
--   live_call_events_uuid_key (unique uuid)
