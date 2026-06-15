"""
Pipeline — wires Acquisition, Processing, Persistence, and Visualization.

Threading model:
  Thread 1      SimulatorThread  (Acquisition)  — puts raw bytes into queue.Queue
  Threads 2..N  WorkerThread pool (Processing + Persistence)
                — N workers all drain the same queue.Queue concurrently;
                  each worker independently parses TelemetryFrame,
                  saves to MongoDB, and broadcasts via WebSocket.
                  queue.Queue is thread-safe so no extra locking is needed.
  Main          FastAPI/uvicorn  (Visualization) — async event loop

The bridge between sync WorkerThreads and the async WebSocket broadcast
is `asyncio.run_coroutine_threadsafe(manager.broadcast(data), loop)`.
This is the canonical, thread-safe way to schedule a coroutine onto a
running event loop from outside that loop. It is safe to call from
multiple worker threads simultaneously.
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


class WorkerThread(threading.Thread):
    """
    Processing + Persistence worker.

    One of N workers that concurrently drain the shared queue. Each worker
    independently parses raw bytes into TelemetryFrame objects, persists
    valid frames to MongoDB, and broadcasts them to all connected WebSocket
    clients. queue.Queue is thread-safe so multiple workers share it safely.
    """

    def __init__(
        self,
        worker_id: int,
        raw_queue: queue.Queue,
        repo: TelemetryRepository,
        manager: ConnectionManager,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(name=f"WorkerThread-{worker_id}", daemon=True)
        self._worker_id = worker_id
        self._queue = raw_queue
        self._repo = repo
        self._manager = manager
        self._loop = loop
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("[Pipeline] %s started", self.name)
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
        logger.info("[Pipeline] %s stop requested", self.name)


# ------------------------------------------------------------------ #
# Module-level handles (used by lifespan)                             #
# ------------------------------------------------------------------ #

_simulator: SimulatorThread | None = None
_workers: list[WorkerThread] = []
_repo: TelemetryRepository | None = None


def start(settings: Settings, manager: ConnectionManager) -> TelemetryRepository:
    """
    Start all pipeline threads.  Called from the FastAPI lifespan startup hook.
    Returns the repository so it can be injected into the route layer.
    """
    global _simulator, _workers, _repo

    loop = asyncio.get_event_loop()
    raw_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

    _repo = TelemetryRepository(settings.mongo_url, settings.db_name)

    _simulator = SimulatorThread(raw_queue, settings.sat_ids, settings.frame_rate_hz)
    _workers = [
        WorkerThread(i, raw_queue, _repo, manager, loop)
        for i in range(settings.num_workers)
    ]

    _simulator.start()
    for worker in _workers:
        worker.start()

    logger.info(
        "[Pipeline] Started — satellites=%s @ %.1f Hz, workers=%d",
        settings.sat_ids, settings.frame_rate_hz, settings.num_workers,
    )
    return _repo


def stop() -> None:
    """Graceful shutdown — request all threads to stop (they are daemons so
    the process will exit even if they don't finish promptly)."""
    if _simulator:
        _simulator.stop()
    for worker in _workers:
        worker.stop()
    if _repo:
        _repo.close()
    logger.info("[Pipeline] Stopped")
