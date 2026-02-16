# Wildfire-Twin (Real-Time Digital Twin)

A real-time “digital twin” prototype for wildfire monitoring.  
It simulates high-velocity IoT sensor events (fire/temperature points) and streams them through Kafka, with the goal of detecting fire events in real time and (next phase) performing spatial joins against static infrastructure data (e.g., building footprints) to identify structures at risk.

---

## What’s implemented right now (Phase 0)

✅ Dockerized Kafka stack (local dev)  
✅ Python producer that emits JSON wildfire sensor events to Kafka  
✅ Dashboard shell (Streamlit) to visualize/inspect the live stream (prototype)

**Next (Phase 1):** Spark Structured Streaming + Apache Sedona spatial join with building footprints.

---

## Tech Stack

- **Python:** 3.11
- **Message Broker:** Kafka (Docker)
- **Dashboard:** Streamlit + PyDeck
- **(Planned) Stream Processor:** Spark 3.4.1
- **(Planned) Spatial Engine:** Apache Sedona 1.5.0
- **(Planned) JVM:** OpenJDK 11 (recommended for Spark 3.4.1 stability)

---

## Repo Structure

```
wild-fire-twin/
  infra/                 # Docker + scripts to run Kafka locally
    docker-compose.yml
    start.sh
    stop.sh
    create_topics.sh
  producer/
    data_generator.py    # Kafka producer (wildfire sensor simulator)
  dashboard/
    backend/
      app.py             # Streamlit app (logic only; loads frontend styles)
    frontend/            # Design layer: edit to change look (no backend changes)
      .streamlit/        #   Theme (config.toml)
      styles.css         #   Layout, hover, responsive
    run.ps1, run.sh      #   Run dashboard (use these to start the app)
  docs/
    PROJECT_STATE.md     # Project state + schema contract
  requirements.txt
  README.md
```

---

## Prerequisites

### Required (Phase 0)
- Docker + Docker Compose
- Python 3.11 (recommended via Conda)
- A working local port `9092` (Kafka)

### Recommended
- Conda environment (e.g., `pyrotwin`)

---

## Quickstart (Phase 0)

### 1) Start Kafka (Docker)
From repo root:
```bash
./infra/start.sh
```

**Health check:** Kafka should be reachable at:
- `localhost:9092`

---

### 2) Create the Kafka topic
```bash
./infra/create_topics.sh
```

Default topic name (Phase 0):
- `fire_events`

---

### 3) Install Python dependencies
```bash
pip install -r requirements.txt
```

(If using conda)
```bash
conda create -n pyrotwin python=3.11 -y
conda activate pyrotwin
pip install -r requirements.txt
```

---

### 4) Run the producer (IoT wildfire event simulator)
```bash
python producer/data_generator.py
```

Expected behavior:
- The producer prints periodic confirmation that messages are being sent
- Events are published to Kafka topic `fire_events`

---

### 5) Run the dashboard (Streamlit)
From repo root:
```bash
# Windows
.\dashboard\run.ps1

# Mac/Linux
./dashboard/run.sh
```
This uses the frontend theme and styles (centered layout, no sidebar, warm dark theme).

**Customizing the dashboard (frontend):** Edit these files to change how the dashboard looks; no backend code changes needed.
- **dashboard/frontend/styles.css** — Layout, spacing, hover effects, responsive breakpoints. The app injects this when it runs. Refresh the dashboard in your browser to see CSS changes.
- **dashboard/frontend/.streamlit/config.toml** — Theme (colors, fonts, borders). Restart the app to see config/theme changes.

Expected behavior:
- Streamlit starts locally
- UI loads (even if it’s a placeholder in Phase 0)

---

## Event Schema (Contract)

Each Kafka message is JSON. Minimum required fields:

- `event_time` (string timestamp, ISO format recommended)
- `sensor_id` (string or int)
- `latitude` (float)
- `longitude` (float)
- `temperature` (float)

Example:
```json
{
  "event_time": "2026-02-09T12:34:56Z",
  "sensor_id": "sensor_17",
  "latitude": 33.9806,
  "longitude": -117.3755,
  "temperature": 87.4
}
```

---

## Configuration

These values should be consistent across `producer/` and `dashboard/`:

- Kafka bootstrap server: `localhost:9092`
- Kafka topic: `fire_events`

(If you support env vars, document them here later.)

---

## Troubleshooting

### Docker permission error (Linux)
If you see permission errors talking to Docker:
- Ensure your user is in the `docker` group:
  ```bash
  sudo usermod -aG docker $USER
  ```
- Then **log out and log back in** (or reboot).

---

### Port 9092 already in use
If Kafka fails to start because `9092` is busy:
- Stop the other service using that port, or
- Edit `infra/docker-compose.yml` to map Kafka to another host port (then update producer/dashboard config).

---

### Topic does not exist
If the producer errors with “unknown topic”:
- Ensure Kafka is running: `./infra/start.sh`
- Create topics again: `./infra/create_topics.sh`

---

### Producer runs but dashboard shows nothing
Phase 0 dashboard may be a shell / prototype depending on current implementation.

Common checks:
- Producer is actually sending to `localhost:9092`
- Topic name matches (`fire_events`)
- Dashboard is reading from the same topic / source

---

## Roadmap

### Phase 1 — Spatial “Collision”
- Load static building footprints (GeoJSON/Parquet)
- Spark Structured Streaming reads Kafka stream
- Sedona spatial join: `fire_zone intersects building_polygon`
- Output “at-risk buildings” to a sink (DB/topic/files)

### Phase 2 — Real-Time Dashboard
- Streamlit reads from the sink
- PyDeck map: live fire points + glowing at-risk buildings

### Phase 3 — Predictive Twin (Optional)
- Add wind vector to project fire spread 10 minutes ahead
- Show predicted risk zone

---

## How to resume in a new session
“I’m working on Wildfire-Twin. I have Dockerized Kafka running and a Python producer emitting JSON fire events to topic `fire_events`. Next is Phase 1: Spark 3.4.1 + Sedona spatial join against building footprints, and we’ll choose a sink so Streamlit can visualize at-risk buildings.”