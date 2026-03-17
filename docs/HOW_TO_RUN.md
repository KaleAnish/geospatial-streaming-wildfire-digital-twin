# How to Run — Wildfire Twin (Complete Guide)

> **Group:** N-Dimensional | **Course:** CS 226 — Big Data | **University:** UC Riverside

---

## Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Core runtime |
| Java (JDK) | 11 or 17 | Required by PySpark / Sedona |
| Docker Desktop | Latest | Runs Kafka + Zookeeper |
| Git | Latest | Version control |

### Python Virtual Environment Setup

```powershell
# Create virtual environment
python -m venv wildfire

# Activate (PowerShell)
.\wildfire\Scripts\activate

# Install all dependencies
pip install -r requirements.txt
pip install h3 folium streamlit-folium matplotlib seaborn
```

### Environment Variables

```powershell
$env:HADOOP_HOME = "E:\wild-fire-twin\infra\hadoop"
$env:PATH += ";$env:HADOOP_HOME\bin"
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"   # Adjust path to your JDK
```

---

## Phase 1: Data Acquisition & Preparation

These scripts are run **once** to build the static "Twin" data layer.

### Step 1 — Download Microsoft Building Footprints (~2.5 GB)

Downloads 11.5 million California building polygons from Microsoft's open dataset.

```powershell
python scripts/download_buildings.py
# Output: data/California.geojson.zip → extracts to data/California.geojson
```

### Step 2 — Fetch OpenStreetMap Essential POIs

Queries the Overpass API for hospitals, schools, fire stations, police stations, and pharmacies across all of California using a 5×5 grid strategy.

```powershell
python scripts/fetch_osm_pois.py
# Output: data/riverside_pois.csv (~79k points of interest)
```

### Step 3 — Convert GeoJSON to Parquet (via PySpark + Sedona)

Parses the massive GeoJSON into an efficient columnar format for fast distributed processing.

```powershell
# First, convert GeoJSON to newline-delimited JSON (required by Spark)
python scripts/to_ndjson.py
# Output: data/California.ndjson

# Then convert to GeoParquet
python scripts/convert_to_parquet.py
# Output: data/riverside_buildings.parquet/ (11.5M building geometry records)
```

### Step 4 — Spatial Join + H3 Indexing (via PySpark + Sedona)

Performs a distributed spatial join (`ST_Contains`) between 11.5M building polygons and 79K POIs. Assigns H3 hexagonal indices (Resolution 7) for partition-level indexing.

```powershell
python scripts/spatial_join_buildings.py
# Output: data/california_essential_buildings_indexed.parquet/
#         (H3-partitioned GeoParquet, ~79k essential buildings, 4,523 partitions)
```

> **What this produces:** A Hive-partitioned Parquet dataset where each folder (`h3_res7=608689...`) contains only the buildings within that H3 hexagonal cell. The dashboard reads only the cells it needs for the current viewport.

---

## Phase 2: Infrastructure Startup

### Step 1 — Start Kafka (via Docker Compose)

```powershell
cd infra
docker-compose up -d
cd ..
# Kafka: localhost:9092 | Zookeeper: localhost:2181
# Wait ~10 seconds for Kafka to initialize
```

### Step 2 — Start the Alert Sink Consumer

Reads processed alerts from Kafka topic `fire_alerts` and writes them to DuckDB.

```powershell
python alert_sink/consumer.py
# Runs continuously. Leave this terminal open.
```

### Step 3 — Start Spark Spatial Engine

Reads raw fire events from Kafka topic `fire_events`, performs spatial joins against building data using Sedona, and writes at-risk building alerts to Kafka topic `fire_alerts`.

```powershell
python spark_processor/spatial_engine.py
# Runs continuously. Leave this terminal open.
```

### Step 4 — Start Streamlit Dashboard

```powershell
streamlit run dashboard/backend/app.py
# Opens at: http://localhost:8501
```

### One-Command Launch (all of the above)

```powershell
.\run_all.ps1
# Launches all 4 components in separate PowerShell windows
```

---

## Phase 3: Using the Dashboard

### Simulating a Fire (What-If Analysis)

**Option A — Dashboard UI:**
1. In the sidebar, click the Folium mini-map to set ignition coordinates
2. Adjust the fallback temperature slider
3. Click "🔥 Simulate Fire Here"

**Option B — Command Line:**
```powershell
python scripts/simulate_fire.py --lat 33.9533 --lon -117.3961
# Publishes a fire event to Kafka with live weather enrichment from OpenWeatherMap
```

### Switching Data Views

- **Teleport Viewport:** Dropdown selects which city's H3 data partitions to load
- **Data Feed Focus:** Radio buttons filter between Live (FIRMS), Simulated, or All data
- **Whole State View:** Renders 206 H3 hexagons showing building density heatmap
- **City View:** Renders detailed building polygons with semantic coloring

### NASA FIRMS Live Satellite Feed

```powershell
python producer/nasa_firms_ingest.py
# Pulls real thermal anomaly data from NASA FIRMS API and publishes to Kafka
# Requires: FIRMS_API_KEY and OPENWEATHER_API_KEY environment variables
```

---

## Running Tests & Benchmarks

All test scripts are in the `tests/` directory and generate PNG charts for PowerPoint presentations.

### 1. DuckDB Store Unit Tests

```powershell
python tests/test_duckdb_store.py
# Tests: init, insert, batch insert, dedup, count, clear
# Expected: "All DuckDB store tests PASSED"
```

### 2. End-to-End Latency Measurement

**Prerequisite:** Run a simulation first so the DuckDB has data.

```powershell
python tests/measure_latency.py
# Measures: Kafka → Spark → DuckDB pipeline latency
# Target: < 30 seconds
# Output: tests/latency_distribution.png
```

### 3. Kafka Throughput Stress Test

**Prerequisite:** Kafka + Spark engine must be running.

```powershell
python tests/stress_test.py
# Floods Kafka with 10,000 concurrent fire events
# Measures: events/second ingestion rate
# Output: tests/throughput_benchmark.png
```

### 4. Spatial Join Benchmark (Sedona vs Standard Spark)

```powershell
python tests/benchmark_joins.py
# Compares: Standard nested-loop join vs Sedona R-Tree partitioned join
# Uses: 100,000 building polygons × 1,000 fire perimeters
# Output: tests/spatial_join_benchmark.png
```

---

## Project Architecture

```
wild-fire-twin/
├── infra/                  # Docker Compose (Kafka + Zookeeper)
├── producer/               # Kafka producers (IoT simulator, NASA FIRMS)
├── spark_processor/        # Spark Structured Streaming + Sedona spatial joins
│   └── spatial_engine.py
├── alert_sink/             # DuckDB consumer (Kafka → DuckDB persistence)
│   ├── consumer.py
│   └── duckdb_store.py
├── dashboard/backend/      # Streamlit + PyDeck frontend
│   ├── app.py              # Main dashboard (H3 multi-resolution map)
│   ├── data_loader.py      # H3 partition pushdown data loader
│   └── map_layers.py       # PyDeck layer builders
├── scripts/                # Data pipeline scripts
│   ├── download_buildings.py
│   ├── fetch_osm_pois.py
│   ├── convert_to_parquet.py
│   ├── spatial_join_buildings.py
│   ├── simulate_fire.py
│   └── fetch_weather_data.py
├── tests/                  # Performance evaluation scripts
│   ├── benchmark_joins.py
│   ├── measure_latency.py
│   ├── stress_test.py
│   └── test_duckdb_store.py
├── data/                   # Generated datasets (gitignored)
├── storage/                # DuckDB database files
├── requirements.txt
├── run_all.ps1             # One-command pipeline launcher
└── README.md
```

---

## Event Schema (Kafka JSON Contract)

```json
{
  "event_time": "2026-03-17T12:34:56Z",
  "event_id": "sim_abc123",
  "sensor_id": "dashboard_sim",
  "latitude": 33.9533,
  "longitude": -117.3961,
  "temperature": 95.0,
  "is_fire": true,
  "wind_speed_mph": 15.0,
  "wind_direction_deg": 270.0,
  "humidity_percent": 20.0
}
```

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `ModuleNotFoundError: h3` | `pip install h3` |
| Kafka connection refused | Ensure Docker is running: `docker-compose up -d` |
| Spark JVM crash | Set `JAVA_HOME` to JDK 11 or 17 |
| Dashboard shows no buildings | Ensure `data/california_essential_buildings_indexed.parquet/` exists |
| `typing.io` error (Python 3.13+) | Already patched in scripts via monkey-patch |
