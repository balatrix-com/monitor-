#!/usr/bin/env python3
"""
Event Handlers Module for ESL Monitor
======================================
Optimized event handlers for FreeSWITCH ESL events.
Uses connection pools and efficient Redis operations.
"""
import json
import logging
import time
import queue
import threading
from typing import Dict, Optional, Any
from functools import lru_cache

from psycopg2.extras import RealDictCursor

# Local imports - work both as package and direct run
try:
    from . import config
    from .connections import get_redis, get_pg_connection
except ImportError:
    import config
    from connections import get_redis, get_pg_connection, get_customer_pg_connection

logger = logging.getLogger("monitor.handlers")


# =============================================================================
# CUSTOMER ID CACHE
# =============================================================================

class CustomerCache:
    """LRU cache for UUID to customer_id mapping."""
    
    def __init__(self, max_size: int = 10000):
        self._cache: Dict[str, str] = {}
        self._max_size = max_size
        self._access_order: list = []
    
    def get(self, uuid: str) -> Optional[str]:
        """Get cached customer_id."""
        if uuid in self._cache:
            # Move to end (most recently used)
            if uuid in self._access_order:
                self._access_order.remove(uuid)
            self._access_order.append(uuid)
            return self._cache[uuid]
        return None
    
    def set(self, uuid: str, customer_id: str):
        """Cache UUID to customer_id mapping."""
        if len(self._cache) >= self._max_size:
            # Remove oldest entries
            to_remove = self._access_order[:self._max_size // 4]
            for key in to_remove:
                self._cache.pop(key, None)
            self._access_order = self._access_order[self._max_size // 4:]
        
        self._cache[uuid] = customer_id
        self._access_order.append(uuid)
    
    def remove(self, uuid: str):
        """Remove UUID from cache."""
        self._cache.pop(uuid, None)
        if uuid in self._access_order:
            self._access_order.remove(uuid)
    
    def size(self) -> int:
        """Get cache size."""
        return len(self._cache)


# Global cache instance
customer_cache = CustomerCache(config.CUSTOMER_CACHE_MAX_SIZE)


# =============================================================================
# NUMBER TO CUSTOMER LOOKUP CACHE
# =============================================================================

@lru_cache(maxsize=1000)
def _cached_customer_lookup(number: str) -> Optional[str]:
    """Cached database lookup for number to tenantid from tfns table.
    Numbers in DB can be stored in various formats: +18001231234, 18001231234, +1-800-123-1234, etc.
    Try multiple variations to find match.
    """
    try:
        with get_customer_pg_connection() as conn:
            if not conn:
                return None
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Generate all possible variations of the number to try
                variations = set()
                
                # Remove all non-digit characters except leading +
                clean_number = ''.join(c for c in number if c.isdigit())
                
                # If number is 10 digits (US/Canada), add +1 prefix
                if len(clean_number) == 10:
                    clean_number = '1' + clean_number
                
                # Try all variations
                variations.add(clean_number)  # e.g., 18001231234
                variations.add(f"+{clean_number}")  # e.g., +18001231234
                variations.add(f"+1{clean_number.lstrip('1')}" if clean_number.startswith('1') else f"+1{clean_number}")
                variations.add(number)  # Original
                
                # Try each variation
                for search_number in variations:
                    if not search_number:
                        continue
                    cur.execute("""
                        SELECT "tenantId"
                        FROM tfns
                        WHERE number = %s
                        LIMIT 1
                    """, (search_number,))
                    result = cur.fetchone()
                    if result:
                        logger.debug(f"Customer lookup matched {number} as {search_number}: {result['tenantId']}")
                        return result['tenantId']
                
                logger.debug(f"Customer lookup found no match for {number} (tried: {', '.join(variations)})")
                return None
    except Exception as e:
        logger.error(f"Customer lookup error for number {number}: {e}")
        return None


def get_customer_id_from_number(number: str) -> Optional[str]:
    """Get customer_id (tenentid) from number via tfns table lookup."""
    if not number:
        return None
    return _cached_customer_lookup(number)


def clear_customer_lookup_cache():
    """Clear the customer lookup cache (call at startup to clear stale entries)."""
    _cached_customer_lookup.cache_clear()
    logger.info("Customer lookup cache cleared")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_internal(number: str) -> bool:
    """Check if number is internal extension vs external/trunk."""
    if not number:
        return False
    # 3-6 digits = internal extension
    # 7+ digits or non-numeric = external
    return len(number) <= 6 and number.isdigit()


def determine_destination(event_data: Dict[str, str]) -> tuple:
    """Determine destination type and value from event data."""
    bridge_to = (
        event_data.get("Other-Leg-Destination-Number") or
        event_data.get("variable_bridge_to") or 
        event_data.get("variable_sip_req_user") or
        event_data.get("variable_sip_to_user") or
        event_data.get("variable_dialed_extension") or
        event_data.get("Caller-Destination-Number") or ""
    )
    
    if bridge_to and bridge_to.isdigit():
        if len(bridge_to) <= 6:
            if bridge_to.startswith('5'):
                return "queue", bridge_to
            elif bridge_to.startswith('6'):
                return "ivr", bridge_to
            return "extension", bridge_to
        return "external", bridge_to
    elif bridge_to:
        return "external", bridge_to
    return "unknown", ""


def determine_call_direction(event_dict: Dict[str, str]) -> str:
    """Determine call direction (legacy behavior)."""
    return (
        event_dict.get("Call-Direction") or
        event_dict.get("Caller-Direction") or
        ""
    ).lower()


def classify_call_type(a_leg_data: Dict[str, str]) -> tuple:
    """
    Determine call type and return tuple:
    (call_type, dest_type, dest_value, egress_trunk)
    
    Call Type Rules:
    - INBOUND: External caller -> Internal extension (no forwarding)
    - DID_FORWARD: External caller -> External destination (forwarded call, has RDNIS)
    - OUTBOUND: Internal extension -> External destination (user-initiated)
    - INTERNAL: Internal extension -> Internal extension
    """
    b_uuid = a_leg_data.get("b_uuid")
    forwarded_to = a_leg_data.get("forwarded_to", "")
    original_callee = a_leg_data.get("callee", "")
    caller = a_leg_data.get("caller", "")
    has_rdnis = a_leg_data.get("has_rdnis", "false") == "true"
    
    caller_internal = is_internal(caller)
    dest_internal = is_internal(forwarded_to)
    
    # No B-leg - simple call with no bridge
    if not b_uuid:
        if not caller_internal:  # External caller, no bridge = INBOUND
            return "INBOUND", "extension", original_callee, ""
        return "OUTBOUND", "unknown", "", ""
    
    # === DID_FORWARD Detection ===
    # External caller -> External destination + RDNIS present = forwarded call
    if not caller_internal and not dest_internal and has_rdnis:
        return "DID_FORWARD", "external", forwarded_to, forwarded_to
    
    # === INBOUND Detection ===
    # External caller -> Internal extension = answered internally
    if not caller_internal and dest_internal:
        return "INBOUND", "extension", forwarded_to, ""
    
    # === OUTBOUND Detection ===
    # Internal extension -> External destination (user-initiated outbound)
    if caller_internal and not dest_internal:
        return "OUTBOUND", "external", forwarded_to, forwarded_to
    
    # === INTERNAL Detection ===
    # Internal extension -> Internal extension
    if caller_internal and dest_internal:
        return "INTERNAL", "extension", forwarded_to, ""
    
    # Fallback
    dest_type = "extension" if dest_internal else "external"
    return "OUTBOUND", dest_type, forwarded_to, forwarded_to


# =============================================================================
# REDIS OPERATIONS
# =============================================================================

def store_call_in_redis(call_data: Dict[str, Any]) -> bool:
    """Store call data in Redis with optimized pipeline."""
    redis_client = get_redis()
    if not redis_client:
        return False
    
    try:
        uuid = call_data.get("uuid")
        customer_id = call_data.get("customer_id") or "unknown"
        if not uuid:
            return False
        
        key = f"customer:{customer_id}:call:{uuid}"
        new_status = call_data.get("call_status")
        
        # Prevent status downgrade
        if new_status == "ringing":
            existing = redis_client.hget(key, "call_status")
            if existing in ["answered", "connected"]:
                call_data["call_status"] = existing
        
        # Pipeline for atomic operations
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(key, mapping={k: str(v) if v is not None else "" for k, v in call_data.items()})
        pipe.expire(key, config.REDIS_CALL_TTL)
        
        status = call_data.get("call_status")
        if status in ["new", "ringing", "answered", "connected"]:
            pipe.sadd("active_calls", uuid)
        
        pipe.publish("calls_stream", json.dumps(call_data))
        pipe.execute()
        
        customer_cache.set(uuid, customer_id)
        return True
        
    except Exception as e:
        logger.error(f"Redis store error: {e}")
        return False


def get_call_from_redis(uuid: str) -> Optional[Dict[str, str]]:
    """Get call data from Redis using cache."""
    redis_client = get_redis()
    if not redis_client:
        return None
    
    try:
        # Try cache first
        customer_id = customer_cache.get(uuid)
        if customer_id:
            key = f"customer:{customer_id}:call:{uuid}"
            data = redis_client.hgetall(key)
            if data:
                return data
        
        # Fallback: scan (slower)
        pattern = f"customer:*:call:{uuid}"
        for key in redis_client.scan_iter(match=pattern, count=100):
            data = redis_client.hgetall(key)
            if data:
                parts = key.split(":")
                if len(parts) >= 2:
                    customer_cache.set(uuid, parts[1])
                return data
        return None
        
    except Exception as e:
        logger.error(f"Redis get error: {e}")
        return None


def remove_call_from_redis(uuid: str, customer_id: str):
    """Remove call from Redis."""
    redis_client = get_redis()
    if not redis_client:
        return
    
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.delete(f"customer:{customer_id}:call:{uuid}")
        pipe.srem("active_calls", uuid)
        pipe.execute()
        customer_cache.remove(uuid)
    except Exception as e:
        logger.error(f"Redis remove error: {e}")


# =============================================================================
# CDR BATCHER FOR PHASE 1 SCALING (300-400 CC)
# =============================================================================

class CDRBatcher:
    """Batch CDRs for bulk insert - reduces DB load for high CC."""
    
    def __init__(self, batch_size: int = 50, timeout: float = 2.0):
        self.batch = []
        self.batch_size = batch_size
        self.timeout = timeout
        self.last_flush = time.time()
        self.lock = threading.Lock()
    
    def add(self, cdr: Dict[str, Any]):
        """Add CDR to batch, flush if needed."""
        with self.lock:
            self.batch.append(cdr)
            
            # Flush if batch full
            if len(self.batch) >= self.batch_size:
                self._flush_internal()
            # Flush if timeout exceeded
            elif time.time() - self.last_flush >= self.timeout:
                self._flush_internal()
    
    def _flush_internal(self):
        """Internal flush (must hold lock)."""
        if self.batch:
            batch_to_save = self.batch.copy()
            self.batch = []
            self.last_flush = time.time()
            # Don't hold lock during DB write
            self._bulk_insert_to_db(batch_to_save)
    
    def flush(self):
        """Force flush remaining CDRs."""
        with self.lock:
            self._flush_internal()
    
    def _bulk_insert_to_db(self, cdrs: list):
        """Bulk insert CDRs to database."""
        if not cdrs:
            return
        
        try:
            with get_pg_connection() as conn:
                if not conn:
                    logger.error("Cannot get DB connection for bulk insert")
                    return
                
                with conn.cursor() as cur:
                    # Build multi-row VALUES clause
                    placeholders = []
                    values = []
                    
                    for i, cdr in enumerate(cdrs):
                        # Each CDR has 22 parameters
                        idx = i * 22
                        placeholders.append(f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                        
                        values.extend([
                            cdr.get("uuid"),
                            cdr.get("b_uuid") or None,
                            cdr.get("call_status"),
                            cdr.get("event_ts") or cdr.get("start_ts"),
                            cdr.get("caller"),
                            cdr.get("callee"),
                            cdr.get("customer_id") or None,
                            cdr.get("dest_type"),
                            cdr.get("dest_value"),
                            cdr.get("status_code", "UNKNOWN"),
                            cdr.get("call_type"),
                            cdr.get("outbound_caller_id") or None,
                            cdr.get("originating_extension") or None,
                            cdr.get("originating_leg_uuid") or None,
                            cdr.get("ingress_trunk") or None,
                            cdr.get("egress_trunk") or None,
                            cdr.get("gateway_id") or None,
                            int(cdr.get("duration", 0)),
                            int(cdr.get("billsec", 0)),
                            cdr.get("currency", "USD"),
                            int(cdr.get("transaction_id", 0)),
                            cdr.get("is_rated", False)
                        ])
                    
                    # Single bulk insert
                    query = f"""
                        INSERT INTO live_call_events 
                        (uuid, b_uuid, event_type, event_ts, caller, callee, 
                         customer_id, dest_type, dest_value, status_code, 
                         call_type, outbound_caller_id, originating_extension, 
                         originating_leg_uuid, ingress_trunk, egress_trunk, gateway_id, 
                         duration, billsec, currency, transaction_id, is_rated)
                        VALUES {','.join(placeholders)}
                        ON CONFLICT (uuid) DO NOTHING
                    """
                    
                    cur.execute(query, values)
                    conn.commit()
                    logger.info(f"Bulk inserted {len(cdrs)} CDRs in 1 query")
                    
        except Exception as e:
            logger.error(f"Bulk insert error: {e}")


# Global batcher instance
cdr_batcher = CDRBatcher(
    batch_size=config.CDR_BATCH_SIZE,
    timeout=config.CDR_BATCH_TIMEOUT
)


# =============================================================================
# CDR STORAGE WITH ASYNC QUEUE
# =============================================================================

# CDR Queue for background processing
cdr_queue: queue.Queue = queue.Queue()
_cdr_workers_started = False


def _save_cdr_to_postgres_sync(call_data: Dict[str, Any]) -> bool:
    """Internal: Save CDR to PostgreSQL (blocking)."""
    try:
        with get_pg_connection() as conn:
            if not conn:
                return False
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO live_call_events 
                    (uuid, b_uuid, event_type, event_ts, caller, callee, 
                     customer_id, dest_type, dest_value, status_code, 
                     call_type, outbound_caller_id, originating_extension, 
                     originating_leg_uuid, ingress_trunk, egress_trunk, gateway_id, 
                     duration, billsec, currency, transaction_id, is_rated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (uuid) DO NOTHING
                """, (
                    call_data.get("uuid"),
                    call_data.get("b_uuid") or None,
                    call_data.get("call_status"),
                    call_data.get("event_ts") or call_data.get("start_ts"),
                    call_data.get("caller"),
                    call_data.get("callee"),
                    call_data.get("customer_id") or None,
                    call_data.get("dest_type"),
                    call_data.get("dest_value"),
                    call_data.get("status_code", "UNKNOWN"),
                    call_data.get("call_type"),
                    call_data.get("outbound_caller_id") or None,
                    call_data.get("originating_extension") or None,
                    call_data.get("originating_leg_uuid") or None,
                    call_data.get("ingress_trunk") or None,
                    call_data.get("egress_trunk") or None,
                    call_data.get("gateway_id") or None,
                    int(call_data.get("duration", 0)),
                    int(call_data.get("billsec", 0)),
                    call_data.get("currency", "USD"),  # Default USD
                    int(call_data.get("transaction_id", 0)),  # Default 0
                    call_data.get("is_rated", False)  # Default False
                ))
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"CDR save error: {e}")
        return False


def _cdr_worker_thread(worker_id: int):
    """Background worker thread to process CDR queue."""
    logger.info(f"CDR worker {worker_id} started")
    while True:
        try:
            call_data = cdr_queue.get(timeout=5)
            if call_data is None:  # Shutdown signal
                break
            _save_cdr_to_postgres_sync(call_data)
            cdr_queue.task_done()
        except queue.Empty:
            continue  # No items, keep waiting
        except Exception as e:
            logger.error(f"CDR worker {worker_id} error: {e}")


def start_cdr_workers():
    """Start background CDR worker threads."""
    global _cdr_workers_started
    if _cdr_workers_started:
        return
    
    num_workers = getattr(config, 'CDR_QUEUE_WORKERS', 3)
    for i in range(num_workers):
        t = threading.Thread(target=_cdr_worker_thread, args=(i+1,), daemon=True)
        t.start()
    
    _cdr_workers_started = True
    logger.info(f"Started {num_workers} CDR worker threads")


def save_cdr_to_postgres(call_data: Dict[str, Any]) -> bool:
    """Queue CDR for async saving to PostgreSQL via batcher (non-blocking)."""
    try:
        # Use batcher for Phase 1 scaling (auto batches and flushes)
        cdr_batcher.add(call_data.copy())
        return True
    except Exception as e:
        logger.error(f"CDR batcher error: {e}")
        return False


def get_cdr_queue_size() -> int:
    """Get current CDR queue size (for monitoring)."""
    return cdr_queue.qsize()


# =============================================================================
# EVENT HANDLERS
# =============================================================================

class EventStats:
    """Track event statistics."""
    def __init__(self):
        self.count = 0
        self.last_time = time.time()
        self.errors = 0
    
    def increment(self):
        self.count += 1
        self.last_time = time.time()
    
    def error(self):
        self.errors += 1

stats = EventStats()


def handle_create(event_dict: Dict[str, str]):
    """Handle CHANNEL_CREATE - capture A-leg and B-leg."""
    try:
        uuid = event_dict.get("Unique-ID")
        direction = determine_call_direction(event_dict)
        other_leg = event_dict.get("Other-Leg-Unique-ID")  # Present if this is B-leg
        start_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        if direction == "inbound" and not other_leg:
            # === A-LEG (Incoming call) ===
            caller = event_dict.get("Caller-Caller-ID-Number", "")
            callee = event_dict.get("Caller-Destination-Number", "")  # The DID
            
            # Determine ingress
            ingress = (event_dict.get("variable_sip_received_ip") or 
                      event_dict.get("Caller-Network-Addr", ""))
            
            customer_id = get_customer_id_from_number(callee) or ""
            
            call_data = {
                "uuid": uuid,
                "b_uuid": "",
                "start_ts": str(start_ts),
                "caller": caller,
                "callee": callee,  # Original DID dialed
                "customer_id": customer_id,
                "call_status": "new",
                "ingress_trunk": ingress,
                "call_type": "pending",
                "originating_extension": "",
                "outbound_caller_id": "",
                "forwarded_to": "",      # Will fill when B-leg arrives
                "has_rdnis": "false",    # Will set to true if B-leg has RDNIS
                "original_did": callee,  # Preserve original DID
                "answer_ts": "",
            }
            
            store_call_in_redis(call_data)
            logger.info(f"CREATE A-leg: {uuid[:8]}... {caller} -> {callee}")
            
        elif other_leg:
            # === B-LEG (Outbound leg from bridge) ===
            # Link to A-leg
            a_leg = get_call_from_redis(other_leg)
            if not a_leg:
                return
                
            # Capture forward indicators
            rdnis = event_dict.get("Caller-RDNIS", "")  # KEY FIELD for DID_FORWARD
            b_dest = event_dict.get("Caller-Destination-Number", "")
            b_caller_id = event_dict.get("Caller-Caller-ID-Number", "")
            
            # Update A-leg record with B-leg info
            a_leg["b_uuid"] = uuid
            a_leg["forwarded_to"] = b_dest
            a_leg["has_rdnis"] = "true" if rdnis else "false"
            
            if rdnis:
                a_leg["original_did"] = rdnis  # Confirm original DID
                
            # For outbound calls, store the originating extension
            if is_internal(a_leg.get("caller", "")):
                a_leg["originating_extension"] = a_leg["caller"]
                a_leg["outbound_caller_id"] = b_caller_id
                
            store_call_in_redis(a_leg)
            logger.info(f"CREATE B-leg: {uuid[:8]}... -> {b_dest} (RDNIS: {rdnis or 'none'})")
            
    except Exception as e:
        stats.error()
        logger.error(f"CREATE error: {e}")


def handle_progress(event_dict: Dict[str, str]):
    """Handle CHANNEL_PROGRESS - ringing."""
    try:
        uuid = event_dict.get("Unique-ID")
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        # Don't downgrade status
        current_status = call_data.get("call_status", "")
        if current_status in ["answered", "connected"]:
            return
        
        call_data["call_status"] = "ringing"
        store_call_in_redis(call_data)
        stats.increment()
        logger.debug(f"PROGRESS: {uuid[:8]}... ringing")
        
    except Exception as e:
        stats.error()
        logger.error(f"PROGRESS error: {e}")


def handle_answer(event_dict: Dict[str, str]):
    """Handle CHANNEL_ANSWER - call answered."""
    try:
        uuid = event_dict.get("Unique-ID")
        answer_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        call_data["call_status"] = "answered"
        call_data["answer_ts"] = str(answer_ts)
        
        dest_type, dest_value = determine_destination(event_dict)
        call_data["dest_type"] = dest_type
        call_data["dest_value"] = dest_value
        
        store_call_in_redis(call_data)
        stats.increment()
        logger.info(f"ANSWER: {uuid[:8]}... -> {dest_type}:{dest_value}")
        
    except Exception as e:
        stats.error()
        logger.error(f"ANSWER error: {e}")


def handle_bridge(event_dict: Dict[str, str]):
    """Handle CHANNEL_BRIDGE - call connected to agent/extension."""
    try:
        uuid = event_dict.get("Unique-ID")
        b_uuid = event_dict.get("Other-Leg-Unique-ID") or ""
        bridge_ts = int(event_dict.get("Event-Date-Timestamp") or 0) // 1000000
        
        call_data = get_call_from_redis(uuid)
        if not call_data:
            return
        
        if b_uuid:
            call_data["b_uuid"] = b_uuid
            call_data["call_status"] = "connected"
            
            dest_type, dest_value = determine_destination(event_dict)
            call_data["dest_type"] = dest_type
            call_data["dest_value"] = dest_value
            
            # Handle race condition - set answer_ts if missing
            if not call_data.get("answer_ts") or call_data.get("answer_ts") == "":
                call_data["answer_ts"] = str(bridge_ts)
            
            store_call_in_redis(call_data)
            stats.increment()
            logger.info(f"BRIDGE: {uuid[:8]}... -> {b_uuid[:8]}... ({dest_type}:{dest_value})")
        
    except Exception as e:
        stats.error()
        logger.error(f"BRIDGE error: {e}")


def handle_hangup_complete(event_dict: Dict[str, str]):
    """Handle CHANNEL_HANGUP_COMPLETE - aggregate and finalize CDR."""
    try:
        uuid = event_dict.get("Unique-ID")
        other_leg = event_dict.get("Other-Leg-Unique-ID")
        
        logger.debug(f"HANGUP_COMPLETE received: uuid={uuid[:8]}... other_leg={other_leg[:8] if other_leg else 'none'}")
        
        # Check if THIS uuid is in Redis (means it's the A-leg we're tracking)
        redis_data = get_call_from_redis(uuid)
        
        # If this UUID is NOT in Redis but has other_leg, it's a B-leg hangup - ignore
        if not redis_data and other_leg:
            logger.debug(f"B-leg hangup ignored (uuid not in Redis)")
            return
        
        # If we have no Redis data at all, nothing to process
        if not redis_data:
            logger.debug(f"HANGUP_COMPLETE: {uuid[:8]}... - no Redis data")
            return
        
        # === A-LEG HANGUP - Finalize CDR ===
        logger.info(f"HANGUP_COMPLETE A-leg: {uuid[:8]}... processing CDR")
        
        # Extract metrics from HANGUP_COMPLETE event (authoritative)
        duration = int(event_dict.get("variable_duration", 0))
        billsec = int(event_dict.get("variable_billsec", 0))
        hangup_cause = event_dict.get("Hangup-Cause", "UNKNOWN")
        hangup_ts = int(event_dict.get("Caller-Channel-Hangup-Time", 0)) // 1000000
        
        # Determine event_type (final disposition)
        if hangup_cause == "USER_BUSY":
            event_type = "busy"
        elif hangup_cause == "CALL_REJECTED":
            event_type = "declined"
        elif hangup_cause in ["NO_ANSWER", "NO_USER_RESPONSE", "ORIGINATOR_CANCEL", "RECOVERY_ON_TIMER_EXPIRE"]:
            event_type = "no_answer"
        elif hangup_cause == "INCOMPATIBLE_DESTINATION":
            event_type = "failed"
        elif hangup_cause == "NORMAL_CLEARING" and billsec > 0:
            event_type = "answered"
        elif hangup_cause == "NORMAL_CLEARING":
            event_type = "no_answer"
        else:
            event_type = "answered" if billsec > 0 else "failed"
        
        # Classify call type using RDNIS detection
        call_type, dest_type, dest_value, egress_trunk = classify_call_type(redis_data)
        
        # Build CDR
        cdr = {
            "uuid": uuid,
            "b_uuid": redis_data.get("b_uuid"),
            "event_type": event_type,
            "event_ts": hangup_ts,
            "caller": redis_data.get("caller"),
            "callee": redis_data.get("original_did", redis_data.get("callee")),  # Use original DID
            "customer_id": redis_data.get("customer_id"),
            "dest_type": dest_type,
            "dest_value": dest_value,  # Where it actually went
            "status_code": hangup_cause,
            "call_type": call_type,
            "duration": duration,
            "billsec": billsec,
            "ingress_trunk": redis_data.get("ingress_trunk"),
            "egress_trunk": egress_trunk or event_dict.get("variable_default_gateway", ""),
            "gateway_id": event_dict.get("variable_sofia_profile_name", ""),
            "call_status": "hangup",
            "currency": "USD",  # Default currency for billing
            "transaction_id": 0,  # Will be set by billing service
            "is_rated": False,  # Will be set by billing service
        }
        
        # Type-specific fields
        if call_type == "OUTBOUND":
            cdr["outbound_caller_id"] = redis_data.get("outbound_caller_id", redis_data.get("caller"))
            cdr["originating_extension"] = redis_data.get("originating_extension", redis_data.get("caller"))
            cdr["originating_leg_uuid"] = uuid
            
        elif call_type == "INBOUND":
            cdr["outbound_caller_id"] = ""
            cdr["originating_extension"] = ""
        
        # Save to DB
        save_cdr_to_postgres(cdr)
        
        # Publish to stream
        redis_client = get_redis()
        if redis_client:
            redis_client.publish("calls_stream", json.dumps(cdr))
        
        # Cleanup Redis
        customer_id = redis_data.get("customer_id", "unknown")
        remove_call_from_redis(uuid, customer_id)
        
        logger.info(f"CDR: {uuid[:8]}... {call_type} {event_type} "
                   f"to {dest_value} ({duration}s/{billsec}s)")
        
    except Exception as e:
        stats.error()
        logger.error(f"HANGUP error: {e}", exc_info=True)


# CHANNEL_DESTROY removed - cleanup done in CHANNEL_HANGUP_COMPLETE
# CHANNEL_HANGUP removed - using CHANNEL_HANGUP_COMPLETE for accuracy


# =============================================================================
# EVENT ROUTER
# =============================================================================

EVENT_HANDLERS = {
    "CHANNEL_CREATE": handle_create,
    "CHANNEL_PROGRESS": handle_progress,
    "CHANNEL_ANSWER": handle_answer,
    "CHANNEL_BRIDGE": handle_bridge,
    "CHANNEL_HANGUP_COMPLETE": handle_hangup_complete,
}


def process_event(event):
    """Route event to appropriate handler."""
    try:
        event_dict = event.headers if hasattr(event, 'headers') else event
        event_name = event_dict.get("Event-Name")
        
        # Debug: Log ALL events to see what we're receiving
        logger.debug(f"Event received: {event_name}")
        
        handler = EVENT_HANDLERS.get(event_name)
        if handler:
            handler(event_dict)
        else:
            # Log unhandled events for debugging
            logger.debug(f"Unhandled event: {event_name}")
            
    except Exception as e:
        stats.error()
        logger.error(f"Event processing error: {e}")
