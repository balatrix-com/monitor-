#!/usr/bin/env python3
"""
Database Connections Module for ESL Monitor
============================================
Optimized connection pools for Redis and PostgreSQL.
Thread-safe with auto-reconnect capabilities.
"""
import logging
import time
import os
import socket
from typing import Optional, Dict
from contextlib import contextmanager

import redis
from redis import ConnectionPool
import psycopg2
from psycopg2 import pool

# Local imports - work both as package and direct run
try:
    from . import config
except ImportError:
    import config

logger = logging.getLogger("monitor.connections")

_runtime_redis_client_name: str = ""
_runtime_lookup_redis_client_name: str = ""


def _build_client_name(base: str) -> str:
    """Build a process-unique Redis client name."""
    host = socket.gethostname()
    return f"{base}:{host}:{os.getpid()}"


# =============================================================================
# REDIS CONNECTION MANAGER
# =============================================================================

class RedisManager:
    """Thread-safe Redis connection manager with connection pooling."""
    
    def __init__(self):
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._last_error_time: float = 0
        self._error_count: int = 0
    
    def connect(self) -> bool:
        """Initialize Redis connection pool."""
        global _runtime_redis_client_name
        try:
            self._pool = ConnectionPool(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                password=config.REDIS_PASSWORD,
                decode_responses=True,
                max_connections=config.REDIS_MAX_CONNECTIONS,
                socket_timeout=config.REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=config.REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,
                health_check_interval=config.REDIS_HEALTH_CHECK_INTERVAL
            )
            self._client = redis.Redis(connection_pool=self._pool)
            self._client.ping()
            _runtime_redis_client_name = _build_client_name(config.REDIS_CLIENT_NAME_BASE)
            self._client.client_setname(_runtime_redis_client_name)
            self._error_count = 0
            logger.info(
                f"Redis connected: {config.REDIS_HOST}:{config.REDIS_PORT} name={_runtime_redis_client_name}"
            )
            return True
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            self._error_count += 1
            self._last_error_time = time.time()
            return False
    
    @property
    def client(self) -> Optional[redis.Redis]:
        """Get Redis client, reconnecting if needed."""
        if self._client is None:
            self.connect()
        return self._client
    
    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        try:
            if self._client:
                self._client.ping()
                return True
        except:
            pass
        return False
    
    def ensure_connected(self) -> bool:
        """Ensure connection, reconnect if needed."""
        if self.is_connected():
            return True
        logger.warning("Redis disconnected, reconnecting...")
        return self.connect()
    
    def close(self):
        """Close all connections."""
        try:
            if self._client:
                self._client.close()
            if self._pool:
                self._pool.disconnect()
        except:
            pass
        self._client = None
        self._pool = None


class LookupRedisManager:
    """Thread-safe lookup Redis manager (DB2 by default)."""

    def __init__(self):
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._last_error_time: float = 0
        self._error_count: int = 0

    def connect(self) -> bool:
        """Initialize lookup Redis connection pool."""
        global _runtime_lookup_redis_client_name
        try:
            self._pool = ConnectionPool(
                host=config.LOOKUP_REDIS_HOST,
                port=config.LOOKUP_REDIS_PORT,
                db=config.LOOKUP_REDIS_DB,
                password=config.LOOKUP_REDIS_PASSWORD,
                decode_responses=True,
                max_connections=config.LOOKUP_REDIS_MAX_CONNECTIONS,
                socket_timeout=config.LOOKUP_REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=config.LOOKUP_REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,
                health_check_interval=config.LOOKUP_REDIS_HEALTH_CHECK_INTERVAL
            )
            self._client = redis.Redis(connection_pool=self._pool)
            self._client.ping()
            _runtime_lookup_redis_client_name = _build_client_name(config.LOOKUP_REDIS_CLIENT_NAME_BASE)
            self._client.client_setname(_runtime_lookup_redis_client_name)
            self._error_count = 0
            logger.info(
                f"Lookup Redis connected: {config.LOOKUP_REDIS_HOST}:{config.LOOKUP_REDIS_PORT}/{config.LOOKUP_REDIS_DB} "
                f"name={_runtime_lookup_redis_client_name}"
            )
            return True
        except Exception as e:
            logger.error(f"Lookup Redis connection failed: {e}")
            self._error_count += 1
            self._last_error_time = time.time()
            return False

    @property
    def client(self) -> Optional[redis.Redis]:
        """Get lookup Redis client, reconnecting if needed."""
        if self._client is None:
            self.connect()
        return self._client

    def is_connected(self) -> bool:
        """Check if lookup Redis is connected."""
        try:
            if self._client:
                self._client.ping()
                return True
        except:
            pass
        return False

    def ensure_connected(self) -> bool:
        """Ensure lookup Redis connection, reconnect if needed."""
        if self.is_connected():
            return True
        logger.warning("Lookup Redis disconnected, reconnecting...")
        return self.connect()

    def close(self):
        """Close all lookup Redis connections."""
        try:
            if self._client:
                self._client.close()
            if self._pool:
                self._pool.disconnect()
        except:
            pass
        self._client = None
        self._pool = None


# =============================================================================
# POSTGRESQL CONNECTION MANAGER
# =============================================================================

class PostgresManager:
    """Thread-safe PostgreSQL connection pool manager."""
    
    def __init__(self):
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._last_error_time: float = 0
        self._error_count: int = 0
    
    def connect(self) -> bool:
        """Initialize PostgreSQL connection pool."""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=config.PG_MIN_CONNECTIONS,
                maxconn=config.PG_MAX_CONNECTIONS,
                host=config.PG_HOST,
                port=config.PG_PORT,
                database=config.PG_DATABASE,
                user=config.PG_USER,
                password=config.PG_PASSWORD,
                connect_timeout=config.PG_CONNECT_TIMEOUT
            )
            # Test connection
            conn = self._pool.getconn()
            conn.cursor().execute("SELECT 1")
            self._pool.putconn(conn)
            
            self._error_count = 0
            logger.info(f"PostgreSQL connected: {config.PG_HOST}:{config.PG_PORT}/{config.PG_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self._error_count += 1
            self._last_error_time = time.time()
            return False
    
    def is_connected(self) -> bool:
        """Check if PostgreSQL pool is healthy."""
        if not self._pool:
            return False
        try:
            conn = self._pool.getconn()
            conn.cursor().execute("SELECT 1")
            self._pool.putconn(conn)
            return True
        except:
            return False
    
    def ensure_connected(self) -> bool:
        """Ensure connection, reconnect if needed."""
        if self.is_connected():
            return True
        logger.warning("PostgreSQL disconnected, reconnecting...")
        self.close()
        return self.connect()
    
    @contextmanager
    def get_connection(self):
        """Context manager for PostgreSQL connections."""
        conn = None
        try:
            if self._pool:
                conn = self._pool.getconn()
                yield conn
            else:
                yield None
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn and self._pool:
                try:
                    self._pool.putconn(conn)
                except:
                    pass
    
    def close(self):
        """Close all connections."""
        try:
            if self._pool:
                self._pool.closeall()
        except:
            pass
        self._pool = None


# =============================================================================
# CUSTOMER DATABASE CONNECTION MANAGER
# =============================================================================

class CustomerPostgresManager:
    """PostgreSQL connection pool manager for customer lookup database."""
    
    def __init__(self):
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._last_error_time: float = 0
        self._error_count: int = 0
    
    def connect(self) -> bool:
        """Initialize customer PostgreSQL connection pool."""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=config.CUSTOMER_PG_MIN_CONNECTIONS,
                maxconn=config.CUSTOMER_PG_MAX_CONNECTIONS,
                host=config.CUSTOMER_PG_HOST,
                port=config.CUSTOMER_PG_PORT,
                database=config.CUSTOMER_PG_DATABASE,
                user=config.CUSTOMER_PG_USER,
                password=config.CUSTOMER_PG_PASSWORD,
                connect_timeout=config.PG_CONNECT_TIMEOUT
            )
            # Test connection
            conn = self._pool.getconn()
            conn.cursor().execute("SELECT 1")
            self._pool.putconn(conn)
            
            self._error_count = 0
            logger.info(f"Customer PostgreSQL connected: {config.CUSTOMER_PG_HOST}:{config.CUSTOMER_PG_PORT}/{config.CUSTOMER_PG_DATABASE}")
            return True
        except Exception as e:
            logger.error(f"Customer PostgreSQL connection failed: {e}")
            self._error_count += 1
            self._last_error_time = time.time()
            return False
    
    def is_connected(self) -> bool:
        """Check if customer PostgreSQL pool is healthy."""
        if not self._pool:
            return False
        try:
            conn = self._pool.getconn()
            conn.cursor().execute("SELECT 1")
            self._pool.putconn(conn)
            return True
        except:
            return False
    
    def ensure_connected(self) -> bool:
        """Ensure connection, reconnect if needed."""
        if self.is_connected():
            return True
        logger.warning("Customer PostgreSQL disconnected, reconnecting...")
        self.close()
        return self.connect()
    
    @contextmanager
    def get_connection(self):
        """Context manager for customer PostgreSQL connections."""
        conn = None
        try:
            if self._pool:
                conn = self._pool.getconn()
                yield conn
            else:
                yield None
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn and self._pool:
                try:
                    self._pool.putconn(conn)
                except:
                    pass
    
    def close(self):
        """Close all connections."""
        try:
            if self._pool:
                self._pool.closeall()
        except:
            pass
        self._pool = None


# =============================================================================
# GLOBAL INSTANCES
# =============================================================================

redis_manager = RedisManager()
lookup_redis_manager = LookupRedisManager()
postgres_manager = PostgresManager()
customer_postgres_manager = CustomerPostgresManager()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_redis() -> Optional[redis.Redis]:
    """Get Redis client."""
    return redis_manager.client


def get_lookup_redis() -> Optional[redis.Redis]:
    """Get lookup Redis client (DB2 by default)."""
    return lookup_redis_manager.client


def get_redis_client_name() -> str:
    """Get runtime Redis client name used by this monitor process."""
    return _runtime_redis_client_name


def get_lookup_redis_client_name() -> str:
    """Get runtime lookup Redis client name used by this monitor process."""
    return _runtime_lookup_redis_client_name


def get_pg_connection():
    """Get PostgreSQL connection context manager (CDR database)."""
    return postgres_manager.get_connection()


def get_customer_pg_connection():
    """Get customer PostgreSQL connection context manager (tfns lookup)."""
    return customer_postgres_manager.get_connection()


def init_connections() -> bool:
    """Initialize all connections."""
    redis_ok = redis_manager.connect()
    lookup_redis_ok = lookup_redis_manager.connect()
    pg_ok = postgres_manager.connect()
    customer_pg_ok = customer_postgres_manager.connect()
    if not customer_pg_ok:
        logger.warning("Customer PostgreSQL unavailable at startup (lookup now uses Redis)")
    return redis_ok and lookup_redis_ok and pg_ok


def close_connections():
    """Close all connections."""
    redis_manager.close()
    lookup_redis_manager.close()
    postgres_manager.close()
    customer_postgres_manager.close()


def health_check() -> Dict[str, bool]:
    """Check health of all connections."""
    return {
        "redis": redis_manager.is_connected(),
        "lookup_redis": lookup_redis_manager.is_connected(),
        "postgres": postgres_manager.is_connected(),
        "customer_postgres": customer_postgres_manager.is_connected()
    }
