"""
Ground Station — Entry Point

Starts the FastAPI application.  The lifespan context manager boots the
pipeline (Acquisition + Processing + Persistence threads) on startup and
shuts them down cleanly when the server stops.

Run locally (no Docker):
    uvicorn main:app --reload

Run via Docker Compose:
    docker compose up
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app import pipeline
from app.config import get_settings
from app.visualization import routes
from app.visualization.ws_manager import ConnectionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    manager = ConnectionManager()

    # Boot pipeline — returns a repo reference injected into the routes layer
    repo = pipeline.start(settings, manager)
    routes.configure(manager, repo, settings.sat_ids)
    logger.info("[App] Ground Station started — listening for telemetry")

    yield  # FastAPI serves requests here

    pipeline.stop()
    logger.info("[App] Ground Station shut down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="CubeSat Ground Station API",
        description=(
            "Receives, processes, stores, and exposes real-time telemetry "
            "from a network of CubeSat satellites."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(routes.router)

    static_dir = Path(__file__).parent / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/static/index.html")

    return app


app = create_app()
