"""
Acquisition Module — SimulatorThread

Mimics an RS-232 serial port driver.  Runs in its own daemon thread and
continuously generates synthetic binary telemetry frames for each configured
satellite at the configured rate.  Frames are placed onto a thread-safe
queue.Queue consumed by the pipeline's ConsumerThread.

About 5 % of produced frames are intentionally corrupted (wrong checksum) to
exercise the InvalidChecksumError path in TelemetryFrame.
"""
from __future__ import annotations

import logging
import queue
import random
import struct
import threading
import time

from app.processing.telemetry_frame import FRAME_LEN, SYNC_BYTE

logger = logging.getLogger(__name__)

# Probability that a generated frame will have a bad checksum.
BAD_FRAME_PROBABILITY = 0.05


def _build_frame(satellite_id: int, *, corrupt: bool = False) -> bytes:
    """
    Build a raw 10-byte telemetry frame.

    Args:
        satellite_id: integer 1–255.
        corrupt: if True the checksum byte is intentionally wrong.
    """
    temperature = random.uniform(-20.0, 60.0)
    voltage_mv = random.randint(3000, 4200)   # 3.0 V – 4.2 V range
    battery_pct = random.randint(10, 100)

    # Pack fields (checksum placeholder = 0x00 for now)
    frame = bytearray(FRAME_LEN)
    frame[0] = SYNC_BYTE
    frame[1] = satellite_id & 0xFF
    struct.pack_into(">f", frame, 2, temperature)
    struct.pack_into(">H", frame, 6, voltage_mv)
    frame[8] = battery_pct

    # Compute correct XOR checksum over bytes 0–8
    checksum = 0
    for b in frame[:9]:
        checksum ^= b
    frame[9] = checksum

    if corrupt:
        # Flip the checksum to guarantee a mismatch
        frame[9] = (~checksum) & 0xFF

    return bytes(frame)


class SimulatorThread(threading.Thread):
    """
    Acquisition Module: produces raw telemetry frames at a fixed rate.

    Each satellite gets one frame per period (1 / frame_rate_hz seconds).
    The thread is a daemon so the process exits cleanly without joining it.
    """

    def __init__(
        self,
        raw_queue: queue.Queue,
        sat_ids: list[int],
        frame_rate_hz: float = 1.0,
    ) -> None:
        super().__init__(name="SimulatorThread", daemon=True)
        self._queue = raw_queue
        self._sat_ids = sat_ids
        self._interval = 1.0 / max(frame_rate_hz, 0.01)
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info(
            "[Acquisition] SimulatorThread started — satellites=%s, interval=%.2fs",
            self._sat_ids,
            self._interval,
        )
        while not self._stop_event.is_set():
            for sat_id in self._sat_ids:
                corrupt = random.random() < BAD_FRAME_PROBABILITY
                frame = _build_frame(sat_id, corrupt=corrupt)
                try:
                    self._queue.put_nowait(frame)
                except queue.Full:
                    logger.warning("[Acquisition] Queue full — dropping frame for sat %d", sat_id)
            time.sleep(self._interval)

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("[Acquisition] SimulatorThread stop requested")
