-- Run on bala_billing database.
-- Drops unused billing/metadata columns from live_call_events table.

ALTER TABLE live_call_events DROP COLUMN IF EXISTS gateway_id;
ALTER TABLE live_call_events DROP COLUMN IF EXISTS currency;
ALTER TABLE live_call_events DROP COLUMN IF EXISTS transaction_id;
ALTER TABLE live_call_events DROP COLUMN IF EXISTS is_rated;

-- Verify schema after drop:
-- \d live_call_events
