# CubeSat Ground Station

A Python + MongoDB application that simulates receiving telemetry from a network of small satellites (CubeSats) over a serial port (RS-232), processes and stores the data, and exposes it through a REST API, WebSocket stream, and a live web dashboard.

Built as a hands-on implementation of a software architecture exercise covering:

- **Concurrency** — Producer-Consumer pattern with threads
- **OOP** — binary frame parsing with custom exceptions
- **NoSQL persistence** — MongoDB with compound indexes
- **Real-time API** — FastAPI with REST endpoints and WebSockets

---

## Architecture

```
+----------------------+        +--------------------------------+
|  SimulatorThread     |        |  ConsumerThread                |
|  (Acquisition)       |------> |  (Processing + Persistence)    |
|                      | queue  |                                |
|  Mimics RS-232 port  | .Queue |  Parses TelemetryFrame         |
|  @ 1 Hz per sat      |        |  Validates XOR checksum        |
|  5 satellites        |        |  Saves to MongoDB              |
|  ~5% corrupt frames  |        |  Broadcasts via WebSocket      |
+----------------------+        +--------------------------------+
                                          |
                              +-----------v-----------+
                              |  FastAPI (main thread) |
                              |  (Visualization)       |
                              |                        |
                              |  GET /satellites       |
                              |  GET /satellites/{id}/latest
                              |  GET /telemetry?...    |
                              |  WS  /ws/telemetry     |
                              |  GET / -> Dashboard    |
                              +------------------------+
```

**Design pattern:** Producer-Consumer via `queue.Queue` — the only thread-safe bridge between the acquisition thread and the consumer thread. The consumer bridges to the async WebSocket broadcast via `asyncio.run_coroutine_threadsafe()`.

---

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)

That's it. No Python or MongoDB installation needed on your machine.

---

## Quickstart with Docker Compose

```bash
cd CubeSats

# Build and start both services (MongoDB + the app)
docker compose up --build

# Or run in the background
docker compose up --build -d
```

The first run downloads `mongo:7`, `mongo-express:1`, and `python:3.12-slim` (~250 MB total). Subsequent starts are instant.

| Service                     | URL                   |
| --------------------------- | --------------------- |
| Live dashboard              | http://localhost:8000 |
| Database UI (mongo-express) | http://localhost:8081 |

**Stop everything:**

```bash
docker compose down
```

**Stop and wipe the MongoDB data volume (start fresh):**

```bash
docker compose down -v
```

---

## Running locally (without Docker)

You need Python 3.10+ and a running MongoDB instance on `localhost:27017`.

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the app
uvicorn main:app --reload
```

Configure via environment variables (set before running):

```bash
# Windows
set MONGO_URL=mongodb://localhost:27017
set DB_NAME=cubesat_gs
set SAT_IDS=1,2,3,4,5
set FRAME_RATE_HZ=0.2
```

---

## Application URLs

| What                      | URL                                                                       |
| ------------------------- | ------------------------------------------------------------------------- |
| **Live dashboard**        | http://localhost:8000                                                     |
| **Database UI**           | http://localhost:8081                                                     |
| **API docs** (Swagger UI) | http://localhost:8000/docs                                                |
| **Latest telemetry**      | http://localhost:8000/satellites/1/latest                                 |
| **Historical query**      | http://localhost:8000/telemetry?satellite_id=1&since=2026-06-01T00:00:00Z |
| **WebSocket stream**      | ws://localhost:8000/ws/telemetry                                          |

---

## Database UI (mongo-express)

**http://localhost:8081** — a browser-based interface for inspecting MongoDB directly. No installation required; it runs as a Docker container alongside the app.

What you can do:

- Browse the `cubesat_gs` database and the `telemetry` collection
- Filter documents with MongoDB query syntax, e.g. `{ "satellite_id": 1 }`
- Sort by any field, e.g. `{ "timestamp": -1 }` to see the newest frames first
- Inspect the two indexes (`sat_id_timestamp`, `timestamp_desc`) under the _Indexes_ tab
- View document count and storage statistics

---

## Dashboard

The dashboard at **http://localhost:8000** has three sections:

- **Satellite cards** — one per satellite showing live temperature, voltage, and a colour-coded battery bar (green → yellow → red). Each card flashes blue when new data arrives.
- **Live feed** — scrolling log of the last 50 valid frames received via WebSocket. Corrupt frames (bad checksum) are discarded by the backend and never appear here.
- **History query** — select a satellite and a "since" timestamp, click _Fetch_ to pull historical records from MongoDB and display them in a table.
- **Connection indicator** (top-right) — green dot when the WebSocket is active; turns red and auto-reconnects after 3 seconds if the connection drops.

---

## Project structure

```
CubeSats/
├── docker-compose.yml              # MongoDB + app services
├── Dockerfile                      # python:3.12-slim image
├── requirements.txt
├── main.py                         # FastAPI entry point; mounts static/
├── static/
│   └── index.html                  # Single-file HTML + JS dashboard
└── app/
    ├── config.py                   # Settings loaded from environment variables
    ├── acquisition/
    │   └── simulator.py            # SimulatorThread — generates binary frames
    ├── processing/
    │   └── telemetry_frame.py      # TelemetryFrame class + InvalidChecksumError
    ├── persistence/
    │   └── mongo_repository.py     # TelemetryRepository (pymongo, sync)
    ├── visualization/
    │   ├── routes.py               # REST + WebSocket endpoints
    │   └── ws_manager.py           # ConnectionManager for WebSocket broadcast
    └── pipeline.py                 # Wires all modules; manages thread lifecycle
```

---

## Binary frame protocol

Each satellite transmits a **10-byte** binary frame:

| Offset | Size | Field          | Details                                    |
| ------ | ---- | -------------- | ------------------------------------------ |
| 0      | 1 B  | SYNC           | `0xAB` — frame start marker                |
| 1      | 1 B  | `satellite_id` | integer 1–5                                |
| 2–5    | 4 B  | `temperature`  | float32 big-endian (°C)                    |
| 6–7    | 2 B  | `voltage_mv`   | uint16 big-endian (millivolts → ÷1000 = V) |
| 8      | 1 B  | `battery_pct`  | 0–100                                      |
| 9      | 1 B  | `checksum`     | XOR of bytes 0–8                           |

`TelemetryFrame` raises `InvalidChecksumError` if `XOR(bytes[0:9]) ≠ bytes[9]`.  
The simulator injects ~5% corrupt frames to exercise this error path.

---

## MongoDB schema

**Database:** `cubesat_gs` — **Collection:** `telemetry`

```json
{
  "satellite_id": 1,
  "timestamp": "2026-06-13T10:00:00Z",
  "temperature_c": 23.5,
  "voltage_v": 3.72,
  "battery_pct": 85,
  "checksum_valid": true
}
```

**Indexes created at startup:**

| Index                                | Purpose                                     |
| ------------------------------------ | ------------------------------------------- |
| `{ satellite_id: 1, timestamp: -1 }` | Covers `satellite_id = X AND timestamp > Y` |
| `{ timestamp: -1 }`                  | Global latest-first lookups                 |

---

## Testing the WebSocket manually

**Browser console:**

```js
const ws = new WebSocket("ws://localhost:8000/ws/telemetry");
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

**wscat:**

```bash
npx wscat -c ws://localhost:8000/ws/telemetry
```

---

## Environment variables

| Variable        | Default                     | Description                                       |
| --------------- | --------------------------- | ------------------------------------------------- |
| `MONGO_URL`     | `mongodb://localhost:27017` | MongoDB connection string                         |
| `DB_NAME`       | `cubesat_gs`                | Database name                                     |
| `SAT_IDS`       | `1,2,3,4,5`                 | Comma-separated satellite IDs to simulate         |
| `FRAME_RATE_HZ` | `0.2`                       | Frames per second per satellite (0.2 = every 5 s) |
