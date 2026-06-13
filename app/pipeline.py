"""
Pipeline — wires Acquisition, Processing, Persistence, and Visualization.

Threading model:
  Thread 1  SimulatorThread  (Acquisition)   — puts raw bytes into queue.Queue
  Thread 2  ConsumerThread   (Processing + Persistence)
                              — drains queue, parses TelemetryFrame,
                                saves to MongoDB, broadcasts via WebSocket
  Main      FastAPI/uvicorn  (Visualization) — async event loop

The bridge between the sync ConsumerThread and the async WebSocket broadcast
is `asyncio.run_coroutine_threadsafe(manager.broadcast(data), loop)`.
This is the canonical, thread-safe way to schedule a coroutine onto a
running event loop from outside that loop.
"""
from __future__ import annotations

import asyncio
import logging
import queue
import threading

from app.acquisition.simulator import SimulatorThread
from app.config import Settings
from app.persistence.mongo_repository import TelemetryRepository
from app.processing.telemetry_frame import InvalidChecksumError, TelemetryFrame
from app.visualization.ws_manager import ConnectionManager

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 1000


class ConsumerThread(threading.Thread):
    """
    Processing + Persistence module.

    Reads raw bytes from the shared queue, parses them into TelemetryFrame
    objects, persists valid frames to MongoDB, and broadcasts them to all
    connected WebSocket clients.
    """

    def __init__(
        self,
        raw_queue: queue.Queue,
        repo: TelemetryRepository,
        manager: ConnectionManager,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(name="ConsumerThread", daemon=True)
        self._queue = raw_queue
        self._repo = repo
        self._manager = manager
        self._loop = loop
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("[Pipeline] ConsumerThread started")
        while not self._stop_event.is_set():
            try:
                raw = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            self._process(raw)
            self._queue.task_done()

    def _process(self, raw: bytes) -> None:
        try:
            frame = TelemetryFrame(raw)
        except InvalidChecksumError as exc:
            logger.warning("[Processing] Bad checksum dropped — %s", exc)
            return
        except ValueError as exc:
            logger.warning("[Processing] Malformed frame dropped — %s", exc)
            return

        # Persist
        try:
            self._repo.save(frame)
        except Exception as exc:
            logger.error("[Persistence] Failed to save frame — %s", exc, exc_info=True)
            return

        # Broadcast to WebSocket clients (thread → async bridge)
        from datetime import datetime, timezone
        data = {
            "satellite_id": frame.satellite_id,
            "temperature_c": frame.temperature_c,
            "voltage_v": frame.voltage_v,
            "battery_pct": frame.battery_pct,
            "checksum_valid": True,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            asyncio.run_coroutine_threadsafe(
                self._manager.broadcast(data), self._loop
            )
        except Exception as exc:
            logger.error("[Visualization] Broadcast failed — %s", exc)

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("[Pipeline] ConsumerThread stop requested")


# ------------------------------------------------------------------ #
# Module-level handles (used by lifespan)                             #
# ------------------------------------------------------------------ #

_simulator: SimulatorThread | None = None
_consumer: ConsumerThread | None = None
_repo: TelemetryRepository | None = None


def start(settings: Settings, manager: ConnectionManager) -> TelemetryRepository:
    """
    Start all pipeline threads.  Called from the FastAPI lifespan startup hook.
    Returns the repository so it can be injected into the route layer.
    """
    global _simulator, _consumer, _repo

    loop = asyncio.get_event_loop()
    raw_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

    _repo = TelemetryRepository(settings.mongo_url, settings.db_name)

    _simulator = SimulatorThread(raw_queue, settings.sat_ids, settings.frame_rate_hz)
    _consumer = ConsumerThread(raw_queue, _repo, manager, loop)

    _simulator.start()
    _consumer.start()

    logger.info("[Pipeline] Started — satellites=%s @ %.1f Hz", settings.sat_ids, settings.frame_rate_hz)
    return _repo


def stop() -> None:
    """Graceful shutdown — request threads to stop (they are daemons so the
    process will exit even if they don't finish promptly)."""
    if _simulator:
        _simulator.stop()
    if _consumer:
        _consumer.stop()
    if _repo:
        _repo.close()
    logger.info("[Pipeline] Stopped")
