#!/usr/bin/env python3
"""
Real-time Redis Stream Monitor
Subscribes to Redis pub/sub and prints updates as they come in
"""
import sys
import json
from datetime import datetime

try:
    from . import config
    from .connections import get_redis
except ImportError:
    import config
    from connections import get_redis


def format_data(data, indent=0):
    """Format data for display."""
    prefix = "  " * indent
    if isinstance(data, dict):
        for key, value in sorted(data.items()):
            if isinstance(value, dict):
                print(f"{prefix}{key}:")
                format_data(value, indent + 1)
            elif isinstance(value, list):
                print(f"{prefix}{key}: [{', '.join(str(v) for v in value)}]")
            else:
                # Truncate long values
                val_str = str(value)
                if len(val_str) > 80:
                    val_str = val_str[:77] + "..."
                print(f"{prefix}{key}: {val_str}")
    else:
        print(f"{prefix}{data}")


def monitor_stream():
    """Subscribe to Redis stream and print updates."""
    redis_client = get_redis()
    
    if not redis_client:
        print("ERROR: Cannot connect to Redis")
        return
    
    # Create a new connection with no socket timeout
    import redis
    redis_client = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD,
        db=config.REDIS_DB,
        socket_keepalive=True,
        socket_timeout=None,
        health_check_interval=30
    )
    
    print("=" * 120)
    print("Redis Call Stream Monitor")
    print("=" * 120)
    print(f"Connected to: {config.REDIS_HOST}:{config.REDIS_PORT}")
    print(f"Listening on channel: calls_stream\n")
    print("Press Ctrl+C to stop...\n")
    
    try:
        # Subscribe to the calls_stream channel
        pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe("calls_stream")
        
        print("Waiting for updates...\n")
        
        event_count = 0
        
        # Use polling with short timeout
        while True:
            try:
                # Poll with 0.1s timeout to keep responsive
                message = pubsub.get_message(timeout=0.1)
                
                if message and message['type'] == 'message':
                    event_count += 1
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    try:
                        # Parse the JSON data
                        if isinstance(message['data'], bytes):
                            data_str = message['data'].decode('utf-8')
                        else:
                            data_str = message['data']
                        
                        data = json.loads(data_str)
                        
                        # Print header
                        print(f"\n[EVENT #{event_count}] {timestamp}")
                        print("─" * 120)
                        
                        # Print the data
                        format_data(data, indent=1)
                        
                        print("─" * 120)
                        
                    except json.JSONDecodeError as e:
                        print(f"[ERROR] Failed to parse JSON: {e}")
                        print(f"Raw data: {message['data']}")
                        
            except TimeoutError:
                # Normal timeout, just continue polling
                continue
            except Exception as e:
                print(f"Poll error: {e}, reconnecting...")
                import time
                time.sleep(1)
                # Reconnect
                pubsub.close()
                pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe("calls_stream")
                    
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
        pubsub.close()
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    monitor_stream()
