#!/usr/bin/env python3
"""
Configuration Module for ESL Monitor
=====================================
Centralized configuration with environment variable support.
"""
import os
from pathlib import Path

# =============================================================================
# ENVIRONMENT LOADER
# =============================================================================

def _env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.getenv(key, default)

def _env_int(key: str, default: int) -> int:
    """Get integer environment variable."""
    return int(os.getenv(key, str(default)))

def _env_bool(key: str, default: bool = False) -> bool:
    """Get boolean environment variable."""
    return os.getenv(key, str(default)).lower() in ('true', '1', 'yes')


# =============================================================================
# APPLICATION PATHS
# =============================================================================

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR
LOG_FILE = LOG_DIR / "monitor.log"


# =============================================================================
# FREESWITCH ESL CONFIGURATION
# =============================================================================

FS_ESL_HOST = _env("FS_ESL_HOST", "51.79.127.113")
FS_ESL_PORT = _env_int("FS_ESL_PORT", 8021)
FS_ESL_PASSWORD = _env("FS_ESL_PASSWORD", "ClueCon")


# =============================================================================
# REDIS CONFIGURATION
# =============================================================================

REDIS_HOST = _env("REDIS_HOST", "10.10.0.2")
REDIS_PORT = _env_int("REDIS_PORT", 6379)
REDIS_DB = _env_int("REDIS_DB", 0)
REDIS_PASSWORD = _env("REDIS_PASSWORD", "OnidaMaruti1)")

# Redis connection settings (Phase 1: 300-400 CC)
REDIS_SOCKET_TIMEOUT = _env_int("REDIS_SOCKET_TIMEOUT", 5)
REDIS_MAX_CONNECTIONS = _env_int("REDIS_MAX_CONNECTIONS", 300)  # Phase 1: 2x increase
REDIS_HEALTH_CHECK_INTERVAL = _env_int("REDIS_HEALTH_CHECK_INTERVAL", 30)

# Redis key settings
REDIS_CALL_TTL = _env_int("REDIS_CALL_TTL", 86400)  # 24 hours


# =============================================================================
# POSTGRESQL CONFIGURATION (CDR / live_call_events)
# =============================================================================

PG_HOST = _env("PG_HOST", "10.10.0.6")
PG_PORT = _env_int("PG_PORT", 5432)
PG_DATABASE = _env("PG_DATABASE", "frendsahil")
PG_USER = _env("PG_USER", "frend1")
PG_PASSWORD = _env("PG_PASSWORD", "SolidMasti1!")

# PostgreSQL pool settings
# Keep this conservative for shared DB (total max_connections=150 across services).
PG_MIN_CONNECTIONS = _env_int("PG_MIN_CONNECTIONS", 5)
PG_MAX_CONNECTIONS = _env_int("PG_MAX_CONNECTIONS", 15)
PG_CONNECT_TIMEOUT = _env_int("PG_CONNECT_TIMEOUT", 5)


# =============================================================================
# CUSTOMER DATABASE CONFIGURATION (tfns table for customer lookup)
# =============================================================================

CUSTOMER_PG_HOST = _env("CUSTOMER_PG_HOST", "10.10.0.7")
CUSTOMER_PG_PORT = _env_int("CUSTOMER_PG_PORT", 5432)
CUSTOMER_PG_DATABASE = _env("CUSTOMER_PG_DATABASE", "subtest")
CUSTOMER_PG_USER = _env("CUSTOMER_PG_USER", "admin")
CUSTOMER_PG_PASSWORD = _env("CUSTOMER_PG_PASSWORD", "OnidaMaruti1)")
CUSTOMER_PG_MIN_CONNECTIONS = _env_int("CUSTOMER_PG_MIN_CONNECTIONS", 5)
CUSTOMER_PG_MAX_CONNECTIONS = _env_int("CUSTOMER_PG_MAX_CONNECTIONS", 50)


# =============================================================================
# PERFORMANCE SETTINGS
# =============================================================================

# Worker pool for event processing
EVENT_WORKER_POOL_SIZE = _env_int("EVENT_WORKER_POOL_SIZE", 250)

# Legacy CDR queue worker setting (not used by spool batcher path)
CDR_QUEUE_WORKERS = _env_int("CDR_QUEUE_WORKERS", 10)

# Event queue settings
EVENT_QUEUE_MAX_SIZE = _env_int("EVENT_QUEUE_MAX_SIZE", 1000)

# Cache settings
CUSTOMER_CACHE_MAX_SIZE = _env_int("CUSTOMER_CACHE_MAX_SIZE", 10000)
CUSTOMER_LOOKUP_CACHE_TTL = _env_int("CUSTOMER_LOOKUP_CACHE_TTL", 300)  # 5 minutes


# =============================================================================
# MONITORING & HEALTH CHECK
# =============================================================================

HEALTH_CHECK_INTERVAL = _env_int("HEALTH_CHECK_INTERVAL", 30)
HEARTBEAT_INTERVAL = _env_int("HEARTBEAT_INTERVAL", 60)
ESL_RECONNECT_DELAY = _env_int("ESL_RECONNECT_DELAY", 5)
ESL_KEEPALIVE_INTERVAL = _env_int("ESL_KEEPALIVE_INTERVAL", 10)


# =============================================================================
# LOGGING SETTINGS
# =============================================================================

LOG_LEVEL = _env("LOG_LEVEL", "DEBUG")  # Temporarily DEBUG to see all events
LOG_MAX_BYTES = _env_int("LOG_MAX_BYTES", 10 * 1024 * 1024)  # 10 MB
LOG_BACKUP_COUNT = _env_int("LOG_BACKUP_COUNT", 5)
LOG_DEBUG_EVENTS = _env_bool("LOG_DEBUG_EVENTS", False)


# =============================================================================
# CDR SETTINGS
# =============================================================================

# Batch CDR inserts for performance and durability.
CDR_BATCH_SIZE = _env_int("CDR_BATCH_SIZE", 50)
CDR_BATCH_TIMEOUT = _env_int("CDR_BATCH_TIMEOUT", 4)

# Durable local spool for zero-loss without external MQ.
CDR_DURABLE_SPOOL_ENABLED = _env_bool("CDR_DURABLE_SPOOL_ENABLED", True)
CDR_SPOOL_DB_PATH = _env("CDR_SPOOL_DB_PATH", str(BASE_DIR / "cdr_spool.db"))

# Insert retry and periodic flush controls.
CDR_INSERT_MAX_RETRIES = _env_int("CDR_INSERT_MAX_RETRIES", 8)
CDR_INSERT_RETRY_BACKOFF = float(_env("CDR_INSERT_RETRY_BACKOFF", "0.5"))
CDR_FLUSH_INTERVAL = float(_env("CDR_FLUSH_INTERVAL", "0.5"))
