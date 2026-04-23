# 200-300 CC Scaling Plan

## Goal
Reach stable 200-300 concurrent calls with low CDR lag and no growing backlog.

## Current Main Bottleneck
Primary bottleneck is PostgreSQL pool capacity.

## Recommended Settings

### 1) PostgreSQL Pool and DB
- Set `PG_MIN_CONNECTIONS` to 10
- Set `PG_MAX_CONNECTIONS` to 50 (range 40-60)
- Keep `PG_CONNECT_TIMEOUT` at 5 or reduce to 3
- Ensure PostgreSQL server `max_connections` is sized for all services, not monitor only
- Keep write-side indexes on `live_call_events` minimal to reduce insert overhead

### 2) CDR Batch and Durable Spool
- Keep `CDR_DURABLE_SPOOL_ENABLED=true`
- Set `CDR_BATCH_SIZE` to 100 (range 80-120)
- Set `CDR_BATCH_TIMEOUT` to 1.5 (range 1-2 seconds)
- Watch heartbeat `cdr_queue`; if it grows continuously, DB is still bottlenecked

### 3) Event Processing
- Set `EVENT_WORKER_POOL_SIZE` to 400 (range 350-450)
- Increase `EVENT_QUEUE_MAX_SIZE` to 2000-5000
- Keep orphan reaper enabled so missed hangups are force-finalized and cleaned

### 4) Redis
- Keep `REDIS_MAX_CONNECTIONS=300` (usually enough for 200-300 CC)
- Ensure low latency between monitor and Redis
- Verify stale active call entries are being cleaned by reconciliation

## Infra Baseline
- CPU: 8 vCPU
- RAM: 16 GB
- Storage: fast SSD
- Network: low latency between monitor, Redis, and PostgreSQL

## Load Test Rollout (Required)
Run staged tests, not one big jump.

1. 120 CC
2. 180 CC
3. 220 CC
4. 260 CC
5. 300 CC

At each stage (20-30 min), verify:
- Heartbeat `active` stable and expected
- Heartbeat `cache` not drifting upward after calls finish
- Heartbeat `cdr_queue` near zero
- No rising error count
- CDR rows inserted continuously without long lag

## Suggested Starting Values for Next Test
- `PG_MAX_CONNECTIONS=50`
- `EVENT_WORKER_POOL_SIZE=400`
- `CDR_BATCH_SIZE=100`
- `CDR_BATCH_TIMEOUT=1.5`

## Exit Criteria for 300 CC Readiness
- 300 CC sustained for at least 30 minutes
- No growing `cdr_queue`
- No recurring connection exhaustion
- No persistent stale `active_calls` after call completion
- CDR insert path remains stable without back-pressure growth
