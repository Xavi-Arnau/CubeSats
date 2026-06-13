"""
Visualization Module — ConnectionManager

Manages active WebSocket connections and broadcasts telemetry to all of them.
The `broadcast` method is a coroutine (async) and runs in the FastAPI event loop.

Thread-safety note:
  ConsumerThread calls `asyncio.run_coroutine_threadsafe(manager.broadcast(data), loop)`
  to safely schedule the broadcast from outside the event loop.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self) -> None:
        self._active: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._active.append(ws)
        logger.info("[WebSocket] Client connected — total=%d", len(self._active))

    def disconnect(self, ws: WebSocket) -> None:
        self._active.remove(ws)
        logger.info("[WebSocket] Client disconnected — total=%d", len(self._active))

    async def broadcast(self, data: dict[str, Any]) -> None:
        """Send JSON payload to every connected WebSocket client."""
        dead: list[WebSocket] = []
        for ws in list(self._active):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._active.remove(ws)
