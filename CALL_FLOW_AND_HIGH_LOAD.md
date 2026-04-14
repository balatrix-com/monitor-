# ESL Monitor Call Flow and High-Load Behavior

## Purpose
This document explains how one call moves through the system from start to finish, and how the same system behaves under sustained high load (100-120 concurrent calls).

## Scenario 1: Single Call Flow (Start to End)

### 1. Call Arrival (Ingress)
- An inbound call reaches the PBX through a trunk or gateway.
- PBX creates the call leg and assigns a unique call identifier.
- Initial call metadata becomes available, such as caller, callee (DID), network source, and channel identifiers.

### 2. Processing Inside PBX
- PBX executes dialplan and routing logic.
- PBX may create A-leg and B-leg relationships if the call is bridged or forwarded.
- PBX updates call state internally as the call progresses (ringing, answered, bridged, hangup).

### 3. Event Generation by PBX
- PBX emits real-time ESL events such as:
- CHANNEL_CREATE
- CHANNEL_PROGRESS
- CHANNEL_ANSWER
- CHANNEL_BRIDGE
- CHANNEL_HANGUP_COMPLETE
- The monitor consumes these events and maps them to call state transitions.

### 4. Live Call State Handling in Monitor
- On create/progress/answer/bridge events, monitor updates Redis call state.
- Redis acts as fast in-memory state storage for active call lifecycle tracking.
- Customer mapping is resolved from subtest.tfns (tenantId/text customer id) and cached.

### 5. CDR Creation Trigger
- On CHANNEL_HANGUP_COMPLETE for the tracked leg, monitor finalizes the CDR.
- It calculates and assigns final fields such as:
- event_type (answered/no_answer/busy/failed/declined)
- duration and billsec
- call_type and destination details
- ingress and egress context

### 6. Durable Spool Write (SQLite)
- Finalized CDR is written first to local SQLite spool.
- This write is durable and survives process restart or PostgreSQL temporary outage.
- This step is the core zero-loss protection layer.

### 7. Batch Processing
- Background flusher wakes on interval and also on demand.
- It reads queued rows from SQLite spool in batches.
- Batches are converted into one multi-row SQL insert.

### 8. Final Insert into PostgreSQL
- Batch insert writes CDR rows into live_call_events.
- Insert uses conflict-safe behavior for idempotency on uuid.
- On success, corresponding rows are deleted from spool.
- On failure, rows remain in spool and are retried with backoff.

### 9. End State
- Call is removed from active Redis state.
- CDR is safely persisted in PostgreSQL.
- If DB is temporarily unavailable, data remains in spool until successful replay.

## Scenario 2: High Load (100-120 Concurrent Calls)

## Conditions
- Calls are continuous.
- Concurrent calls fluctuate between 100 and 120.
- Mixed durations exist (short around 1 minute, long around 10-15 minutes).
- Hangups happen at random times, not in a single synchronized wave.

### 1. Event Generation Under Load
- PBX continuously emits events for many simultaneous call legs.
- Event stream naturally interleaves call states from many calls.
- Monitor processes these events in parallel worker context and updates Redis quickly.

### 2. Multiple CDRs Finalized Simultaneously
- Each hangup triggers independent CDR finalization.
- Several CDRs can be produced in the same second.
- Each finalized CDR is immediately durably enqueued into SQLite spool.

### 3. Spool Behavior Under Sustained Load
- Spool acts as a shock absorber between event rate and DB write rate.
- If insert throughput momentarily drops, spool depth increases.
- No CDR is dropped because data is already durably persisted before DB insert.
- When DB catches up, spool depth shrinks as flusher drains backlog.

### 4. Batch Behavior During Bursts
- Random call endings still create micro-bursts.
- Flusher groups pending rows into batch inserts, reducing commit overhead.
- One batched insert is significantly cheaper than many single-row inserts.
- Retry with backoff avoids overload loops during temporary DB stress.

### 5. Data Loss Prevention Model
- Zero-loss principle is enforced by write order:
- First durable local spool write.
- Then async replay to PostgreSQL.
- If monitor crashes after spool write and before DB commit, data is replayed on restart.
- If PostgreSQL is slow or unavailable, spool retains data until success.

### 6. Efficient Insert Management
- Multi-row inserts reduce transaction overhead.
- Fewer indexes on live_call_events reduce write amplification.
- Controlled connection pool avoids over-saturating shared PostgreSQL.
- Retry and backoff smooth transient failures instead of dropping rows.

## Practical Outcome at 100-120 CC
- System remains stable because write path is decoupled from immediate event pressure.
- Redis handles active call state quickly.
- SQLite spool guarantees durability and absorbs temporary bursts.
- PostgreSQL receives efficient batched inserts with replay safety.
- End result: predictable behavior with no CDR loss, even during fluctuating call traffic.

## Function-by-Function Flow Map (Actual Code Path)

### A. PBX Event Intake and Dispatch
- ESL listener starts in `monitor.py` at `run_esl_listener()`.
- PBX events are subscribed and callback is registered via `fs.register_handle("*", pooled_handler)`.
- Each event is pushed to worker pool with `worker_pool.spawn(process_event_async, event)`.
- Worker calls `process_event_async()` which calls `process_event()` in `handlers.py`.

### B. Event Router and Handlers
- `process_event()` reads `Event-Name` and routes through `EVENT_HANDLERS`.
- `CHANNEL_CREATE` -> `handle_create()`
- `CHANNEL_PROGRESS` -> `handle_progress()`
- `CHANNEL_ANSWER` -> `handle_answer()`
- `CHANNEL_BRIDGE` -> `handle_bridge()`
- `CHANNEL_HANGUP_COMPLETE` -> `handle_hangup_complete()`

### C. Redis Live State Functions
- Write/update state: `store_call_in_redis()`
- Read state by UUID: `get_call_from_redis()`
- Cleanup state on end: `remove_call_from_redis()`

### D. Customer Mapping Functions
- DID-based mapping: `get_customer_id_from_number()`
- Extension-based mapping: `get_customer_id_from_extension()`
- Both use cached lookup and read `tenantId` from `subtest.tfns` (text ids like `U0006`).

### E. How End-of-Call Is Detected
- PBX sends `CHANNEL_HANGUP_COMPLETE`.
- `handle_hangup_complete()` verifies tracked leg in Redis.
- It computes final disposition (`event_type`), duration, billsec, call_type, and destination details.
- It builds final CDR payload and calls `save_cdr_to_postgres(cdr)`.

### F. Durable Write Path (Always Spool)
- `save_cdr_to_postgres()` writes CDR to SQLite spool first (zero-loss path).
- Spool is implemented by `CDRSpool` with:
- `enqueue()` for durable append
- `fetch_batch()` for replay batches
- `delete_batch()` after successful DB insert

### G. Batch Replay to PostgreSQL
- `start_cdr_workers()` starts batch flusher thread through `cdr_batcher.start()`.
- Flusher loop periodically calls `flush()`.
- `flush()` pulls rows from spool and uses `_bulk_insert_with_retry()`.
- `_bulk_insert_to_db()` executes one multi-row insert into `live_call_events`.
- On success, spool rows are deleted; on failure, rows stay and retry later.

## High Load 100-120 CC: What Happens Internally

### 1. Random Call Durations and End Times
- Long calls (10-15 min) stay active in Redis longer.
- Short calls (~1 min) finalize early and become CDRs quickly.
- Because hangups are random, CDR generation is continuous rather than one giant wave.

### 2. Concurrent CDR Finalization
- Multiple `CHANNEL_HANGUP_COMPLETE` events can be processed in parallel.
- Each finalized CDR is durably queued in spool immediately.
- This decouples event arrival speed from DB commit speed.

### 3. Burst Handling
- If many calls end near the same time, spool depth increases temporarily.
- Flusher drains in batches, reducing PostgreSQL transaction overhead.
- Retry with backoff prevents loss and avoids aggressive DB hammering.

### 4. Why Data Is Not Lost
- CDR persistence is spool-first, DB-second.
- Crash after spool write still keeps CDR on disk.
- Restart resumes replay from spool to PostgreSQL.

### 5. Efficiency Under Sustained 100-120 CC
- Redis handles active state updates quickly.
- SQLite spool absorbs pressure spikes cheaply.
- PostgreSQL receives compact batched inserts instead of heavy per-row commits.
