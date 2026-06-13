"""
Persistence Module — TelemetryRepository

Wraps the MongoDB 'telemetry' collection.  All operations are synchronous
(pymongo) because callers run inside daemon threads, not the async event loop.

Collection schema (one document per frame):
  {
    "satellite_id": int,
    "timestamp":    datetime (UTC),
    "temperature_c": float,
    "voltage_v":     float,
    "battery_pct":   int,
    "checksum_valid": bool,
  }

Indexes (created once at startup):
  1. { satellite_id: 1, timestamp: -1 }  — compound; covers sat=X AND date>Y
  2. { timestamp: -1 }                   — global latest-first queries
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection

from app.processing.telemetry_frame import TelemetryFrame

logger = logging.getLogger(__name__)

COLLECTION_NAME = "telemetry"


class TelemetryRepository:
    def __init__(self, mongo_url: str, db_name: str) -> None:
        self._client: MongoClient = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        self._col: Collection = self._client[db_name][COLLECTION_NAME]
        self._ensure_indexes()

    # ------------------------------------------------------------------ #
    # Index bootstrap                                                      #
    # ------------------------------------------------------------------ #

    def _ensure_indexes(self) -> None:
        self._col.create_index(
            [("satellite_id", pymongo.ASCENDING), ("timestamp", pymongo.DESCENDING)],
            name="sat_id_timestamp",
            background=True,
        )
        self._col.create_index(
            [("timestamp", pymongo.DESCENDING)],
            name="timestamp_desc",
            background=True,
        )
        logger.info("[Persistence] MongoDB indexes ensured on '%s'", COLLECTION_NAME)

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    def save(self, frame: TelemetryFrame) -> None:
        """Persist a validated TelemetryFrame as a MongoDB document."""
        doc: dict[str, Any] = {
            "satellite_id": frame.satellite_id,
            "timestamp": datetime.now(tz=timezone.utc),
            "temperature_c": frame.temperature_c,
            "voltage_v": frame.voltage_v,
            "battery_pct": frame.battery_pct,
            "checksum_valid": True,
        }
        self._col.insert_one(doc)

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def get_latest(self, satellite_id: int) -> dict[str, Any] | None:
        """
        Return the most recent telemetry document for a satellite, or None.
        Uses the compound index (satellite_id, timestamp -1).
        """
        doc = self._col.find_one(
            {"satellite_id": satellite_id},
            sort=[("timestamp", pymongo.DESCENDING)],
        )
        return _serialize(doc) if doc else None

    def get_history(
        self,
        satellite_id: int,
        since: datetime,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """
        Return up to `limit` documents for a satellite newer than `since`.
        Equivalent to: SELECT * WHERE satellite_id = X AND timestamp > Y
        """
        cursor = (
            self._col.find(
                {
                    "satellite_id": satellite_id,
                    "timestamp": {"$gt": since},
                }
            )
            .sort("timestamp", pymongo.DESCENDING)
            .limit(limit)
        )
        return [_serialize(doc) for doc in cursor]

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        self._client.close()
        logger.info("[Persistence] MongoDB connection closed")


def _serialize(doc: dict[str, Any]) -> dict[str, Any]:
    """Convert a MongoDB document to a JSON-serialisable dict."""
    doc = dict(doc)
    doc.pop("_id", None)
    if isinstance(doc.get("timestamp"), datetime):
        # Attach UTC offset explicitly so browsers parse it correctly
        ts = doc["timestamp"].replace(tzinfo=timezone.utc)
        doc["timestamp"] = ts.isoformat()
    return doc
