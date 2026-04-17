# Redis Data Format (Current)

This document describes the Redis structures produced by the monitor service after the recent key-format and payload updates.

## 1) Live Call Keys

The service now writes each active call into two hash keys:

1. Primary key (source of truth)
   - call:{uuid}
2. Compatibility key (for older consumers)
   - customer:{customer_id}:call:{uuid}

Both keys store the same hash fields and use TTL.

Related set:
- active_calls (Redis Set)
  - Contains uuid for active states: new, ringing, answered, connected

Pub/Sub channel:
- calls_stream
  - Receives JSON payload for state updates and final CDR

## 2) Hash Field Schema (Live Call State)

All hash values are stored as strings.

Common fields:
- uuid
- b_uuid
- start_ts
- caller
- callee
- customer_id
- call_status
- ingress_trunk
- call_type
- originating_extension
- outbound_caller_id
- forwarded_to
- has_rdnis
- original_did
- answer_ts
- route_key
- inbound_did
- entry_route_key
- entry_route_type

Notes:
- customer_id is forced to unknown when empty.
- b_uuid is empty until B-leg exists.
- has_rdnis is "true" or "false".
- call_status transitions: new -> ringing -> answered -> connected -> hangup

## 3) Example Live Hash

Key:
- call:8ec9c2f8-1d3f-4ad2-90c4-2d7c74e42d12

Example value (hash fields shown as JSON-like for readability):

{
  "uuid": "8ec9c2f8-1d3f-4ad2-90c4-2d7c74e42d12",
  "b_uuid": "",
  "start_ts": "1776201002",
  "caller": "2154697017",
  "callee": "8883336661",
  "customer_id": "U0004",
  "call_status": "ringing",
  "ingress_trunk": "51.79.127.122",
  "call_type": "pending",
  "originating_extension": "",
  "outbound_caller_id": "",
  "forwarded_to": "",
  "has_rdnis": "false",
  "original_did": "8883336661",
  "answer_ts": "",
  "route_key": "ext2_u0004",
  "inbound_did": "",
  "entry_route_key": "ext2_u0004",
  "entry_route_type": "extension"
}

## 4) calls_stream Payloads

The same channel carries two payload types.

1. Live state update payload
   - Sent whenever store_call_in_redis updates a call hash.
2. Final CDR payload
   - Sent on hangup completion.

### 4.1 Live State Payload Example

{
  "uuid": "8ec9c2f8-1d3f-4ad2-90c4-2d7c74e42d12",
  "b_uuid": "2f6f1f31-0c7a-4ae7-b9be-633ea56d65e1",
  "start_ts": "1776201002",
  "caller": "2154697017",
  "callee": "8883336661",
  "customer_id": "U0004",
  "call_status": "connected",
  "ingress_trunk": "51.79.127.122",
  "call_type": "pending",
  "originating_extension": "",
  "outbound_caller_id": "",
  "forwarded_to": "9885564228",
  "has_rdnis": "true",
  "original_did": "8883336661",
  "answer_ts": "1776201008",
  "dest_type": "external",
  "dest_value": "9885564228",
  "route_key": "fwd1_u0004",
  "entry_route_key": "ext2_u0004",
  "entry_route_type": "did_forward"
}

### 4.2 Final CDR Payload Example

{
  "uuid": "8ec9c2f8-1d3f-4ad2-90c4-2d7c74e42d12",
  "b_uuid": "2f6f1f31-0c7a-4ae7-b9be-633ea56d65e1",
  "event_type": "answered",
  "event_ts": 1776201045,
  "caller": "2154697017",
  "callee": "8883336661",
  "customer_id": "U0004",
  "dest_type": "external",
  "dest_value": "9885564228",
  "status_code": "NORMAL_CLEARING",
  "call_type": "DID_FORWARD",
  "duration": 43,
  "billsec": 37,
  "ingress_trunk": "51.79.127.122",
  "egress_trunk": "9885564228",
  "call_status": "hangup"
}

## 5) Important Change Summary

Old pattern used by some consumers:
- customer:{customer_id}:call:{uuid}

Current pattern:
- Primary: call:{uuid}
- Compatibility: customer:{customer_id}:call:{uuid}

Impact:
- New consumers should read call:{uuid} when possible.
- Existing consumers can continue using customer:{customer_id}:call:{uuid}.

## 6) Quick Redis CLI Checks

List active UUIDs:
- SMEMBERS active_calls

Inspect one call hash by new key:
- HGETALL call:{uuid}

Inspect one call hash by compatibility key:
- HGETALL customer:{customer_id}:call:{uuid}

Watch live stream:
- SUBSCRIBE calls_stream
