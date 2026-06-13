"""
Visualization Module — FastAPI routes

Endpoints:
  GET  /satellites/{satellite_id}/latest
       → Last known telemetry for a satellite.

  GET  /telemetry?satellite_id=1&since=2026-06-13T00:00:00Z
       → Historical data (up to 500 records).

  WS   /ws/telemetry
       → Live stream; every validated frame is broadcast as JSON.

  GET  /satellites
       → List of configured satellite IDs (convenience).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

from app.visualization.ws_manager import ConnectionManager

logger = logging.getLogger(__name__)

router = APIRouter()

# These are injected by pipeline.py after the app starts.
_manager: ConnectionManager | None = None
_repo = None  # TelemetryRepository
_sat_ids: list[int] = []


def configure(manager: ConnectionManager, repo, sat_ids: list[int]) -> None:
    """Called once during app lifespan to wire dependencies."""
    global _manager, _repo, _sat_ids
    _manager = manager
    _repo = repo
    _sat_ids = sat_ids


# ------------------------------------------------------------------ #
# REST endpoints                                                       #
# ------------------------------------------------------------------ #

@router.get("/satellites", summary="List configured satellites")
def list_satellites():
    return {"satellite_ids": _sat_ids}


@router.get(
    "/satellites/{satellite_id}/latest",
    summary="Latest telemetry for a satellite",
)
def get_latest(satellite_id: int):
    doc = _repo.get_latest(satellite_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"No telemetry data for satellite {satellite_id}",
        )
    return doc


@router.get("/telemetry", summary="Historical telemetry query")
def get_history(
    satellite_id: int = Query(..., description="Satellite identifier"),
    since: datetime = Query(
        ...,
        description="Return frames newer than this ISO-8601 timestamp",
        example="2026-06-13T00:00:00Z",
    ),
):
    # Ensure UTC awareness
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    docs = _repo.get_history(satellite_id, since)
    return {"satellite_id": satellite_id, "count": len(docs), "records": docs}


# ------------------------------------------------------------------ #
# WebSocket endpoint                                                   #
# ------------------------------------------------------------------ #

@router.websocket("/ws/telemetry")
async def ws_telemetry(websocket: WebSocket):
    """
    Live telemetry stream.

    Clients connect and receive a JSON message for every valid frame processed
    by the pipeline.  No client-side polling required.

    Why WebSocket over Polling:
      - Polling (e.g. every 1 s) wastes bandwidth with empty responses when no
        new data is available and adds 1-second latency at minimum.
      - WebSocket maintains a single persistent TCP connection; the server
        pushes each frame the instant it is processed (~5 frames/s here).
        Latency is essentially zero, and bandwidth is used only when data arrives.
    """
    await _manager.connect(websocket)
    try:
        # Keep the connection alive; we only send (broadcast) from ConsumerThread.
        # We still need to await something to detect client disconnect.
        while True:
            await websocket.receive_text()  # blocks until client sends or disconnects
    except WebSocketDisconnect:
        _manager.disconnect(websocket)
