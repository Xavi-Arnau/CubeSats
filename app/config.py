from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    mongo_url: str
    db_name: str
    sat_ids: list[int]
    frame_rate_hz: float
    num_workers: int


def get_settings() -> Settings:
    sat_ids_raw = os.getenv("SAT_IDS", "1,2,3,4,5")
    # Railway injects MONGODB_URL; local docker-compose uses MONGO_URL
    mongo_url = (
        os.getenv("MONGO_URL")
        or os.getenv("MONGODB_URL")
        or "mongodb://localhost:27017"
    )
    return Settings(
        mongo_url=mongo_url,
        db_name=os.getenv("DB_NAME", "cubesat_gs"),
        sat_ids=[int(s.strip()) for s in sat_ids_raw.split(",") if s.strip()],
        frame_rate_hz=float(os.getenv("FRAME_RATE_HZ", "0.2")),
        num_workers=int(os.getenv("WORKER_THREADS", "3")),
    )
