#!/usr/bin/env python3
"""
Capture raw ESL events and save to events.txt
"""
import socket
import os

try:
    from . import config
except ImportError:
    import config

txt_file = os.path.join(os.path.dirname(__file__), "events.txt")

print(f"Connecting to {config.FS_ESL_HOST}:{config.FS_ESL_PORT}...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((config.FS_ESL_HOST, config.FS_ESL_PORT))
print("Connected!")

# Read welcome
welcome = sock.recv(1024)
print("Got welcome")

# Auth
sock.sendall(f"auth {config.FS_ESL_PASSWORD}\n\n".encode())
sock.recv(1024)
print("Authenticated")

# Subscribe
sock.sendall("event plain CHANNEL_CREATE CHANNEL_ANSWER CHANNEL_BRIDGE CHANNEL_HANGUP_COMPLETE\n\n".encode())
sock.recv(1024)
print(f"Subscribed! Writing to {txt_file}")

with open(txt_file, "w") as f:
    f.write("=== EVENT CAPTURE STARTED ===\n\n")
    f.flush()
    
    while True:
        try:
            data = sock.recv(4096).decode('utf-8', errors='ignore')
            if data:
                print(f"Got {len(data)} bytes")
                with open(txt_file, "a") as f:
                    f.write(data)
                    f.write("\n" + "="*80 + "\n")
                    f.flush()
        except KeyboardInterrupt:
            print("Stopped")
            break
        except Exception as e:
            print(f"Error: {e}")
            break

sock.close()
