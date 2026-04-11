#!/usr/bin/env python3
"""
Simple local WebSocket broker for MCP demo.
Broadcasts text messages to other connected clients.
Usage: python local_mcp_broker.py
"""

import asyncio
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

CONNECTED = set()

async def handler(ws, path=None):
    CONNECTED.add(ws)
    print(f"client connected: {ws.remote_address}")
    try:
        async for message in ws:
            # Forward text messages to all other connected clients
            for c in list(CONNECTED):
                if c is ws:
                    continue
                try:
                    await c.send(message)
                except (ConnectionClosedOK, ConnectionClosedError):
                    CONNECTED.discard(c)
    except Exception as exc:
        print("handler error:", exc)
    finally:
        CONNECTED.discard(ws)
        print("client disconnected")

async def main():
    print("Starting local MCP broker on ws://0.0.0.0:8765")
    async with websockets.serve(handler, "0.0.0.0", 8765):
        await asyncio.Future()  # run forever

if __name__ == '__main__':
    asyncio.run(main())
