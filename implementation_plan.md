# Implementation Plan: Wildfire-Twin

## Goal Description
Build Phase 1 of the Wildfire-Twin real-time streaming pipeline. Connect simulated wildfire events (via Kafka) with massive static building assets (GeoParquet) using Apache Spark and Apache Sedona for spatial cross-referencing.

## Proposed Changes

### Environment & Setup (Sprint 1)
- Install Spark 3.4.4 and configure for Windows (using `winutils.exe`).
- Upgrade to Python 3.13 and install correctly built `pyarrow` (`18.1.0`).
- Implement [start_spark.ps1](file:///e:/wild-fire-twin/infra/start_spark.ps1) to strictly enforce `JAVA_HOME` pointing to JDK 11 and bypass `IllegalAccessError`.
- Ensure Apache Sedona 1.7.0 is pinned correctly as the spatial engine.

### Data Ingestion (Sprint 2)
- Download `California.geojson.zip` from Microsoft Bing Maps Open Data.
- Convert 3.3GB GeoJSON to Spatially-Indexed GeoParquet for real-time memory efficiency.

### California-Wide Spatial Joining (Sprint 2.5)
We need to map semantic meaning (building types) to the raw geometric polygons across the entire state.
- Extract Point-of-Interest (POI) data for the **entire state of California** from OpenStreetMap (Overpass API). Specifically targeting: `hospital`, `school`, `university`, `fire_station`, `police`, and `emergency` infrastructure.
- Run a state-wide Spark/Sedona Spatial Join (`ST_Contains`) between 11.5M Microsoft polygons and ~50k California POIs.
- Save this combined data as `california_essential_buildings.parquet`.
- Update the Streamlit dashboard to handle state-wide navigation with smart sampling.

### Visual Dashboard
- Configure a Streamlit interface using `pydeck` to interactively visualize the high-performance building data spanning California.

### Unified Dynamic Stream (Sprint 3) 🚀
The final phase connects "where the fire is" with "what is at risk".
- **Infrastructure**: Start Kafka and Zookeeper via Docker ([docker-compose.yml](file:///e:/wild-fire-twin/infra/docker-compose.yml)).
- **Dynamic Engine**: Update [spatial_engine.py](file:///e:/wild-fire-twin/spark_processor/spatial_engine.py) to:
    1. Consume live fire events (JSON: `id`, `lat`, `lon`, `radius`).
    2. Convert fire point + radius into a geometric `ST_Buffer`.
    3. Spatial Join: Find intersections between the fire buffer and the `california_essential_buildings.parquet` static table.
    4. Emit "Alert" events for every essential building currently inside the fire zone.
- **Simulation**: Create [scripts/simulate_fire.py](file:///e:/wild-fire-twin/scripts/simulate_fire.py) to inject events into the pipeline.
- **Real-Time UI**: Display the active fire radius and "At Risk" counters in the Streamlit app.

## Sprint 4: Predictive Weather & Fire Spread Modeling

### [Component Name] Live Weather & Predictive Analytics

Summary: Evolving the Digital Twin from reactive observation to proactive prediction by integrating live weather APIs (wind speed, direction, humidity) to forecast fire movement.

#### [NEW] [fetch_weather_data.py](file:///e:/wild-fire-twin/scripts/fetch_weather_data.py)
A script to fetch live weather data for active fire coordinates using the **Open-Meteo API** (a free, open-source weather API that requires no API key). This data will enrich the fire simulation payloads.

#### [MODIFY] [spatial_engine.py](file:///e:/wild-fire-twin/spark_processor/spatial_engine.py)
Update the Spark engine to perform **Directional Risk Buffering**:
- Instead of a static 500m circular `ST_Buffer`, the engine will calculate a directional polygon (like a cone or ellipse) based on **wind speed** (length) and **wind direction** (angle).
- Integrate **temperature and humidity** metrics to adjust the overall risk severity (e.g., higher temp + low humidity = wider spread cone).

#### [MODIFY] [app.py](file:///e:/wild-fire-twin/dashboard/backend/app.py)
- **Predictive Rendering**: Visualize the predicted fire spread path on the PyDeck map (e.g., an orange cone extending from the fire origin).
- **Weather Context**: Display live wind vectors and humidity on the dashboard sidebar for situational awareness.

# [NEW] Sprint 5: Dashboard Optimization & UI/UX Refinement ⚡

## Goal Description
Enhance the performance and visual fidelity of the Wildfire-Twin dashboard to ensure it feels like a premium "Digital Twin" tool. We aim for sub-100ms viewport response times and a more interactive, professional UI.

## Proposed Changes

### 1. Performance: Spatial Indexing
- **Problem**: Current viewport filtering uses boolean masks on the full 52k-row DataFrame, which scales poorly.
- **Solution**: Implement a Spatial R-Tree index (via `geopandas.sindex`). This allows us to query only the buildings within the current map bounds in logarithmic time.

### 2. UI/UX: Non-Blocking Kafka Polling (`st.fragment`)
- **Problem**: Polling Kafka currently triggers a full Streamlit rerun, which can cause the map to flicker or reload.
- **Solution**: Use `st.fragment` to isolate the Kafka consumer logic. This allows the sidebar "Live Alerts" to update independently of the main map and heavy building dataset.

### 3. Visuals: "Premium" 3D Aesthetics
- **Design Upgrade**:
  - Replace generic colors with a custom, high-contrast HSL palette (e.g., Deep Crimson for Medical, Amber for Education).
  - Add **elevation variety**: Scale building heights based on a "calculated floor count" (randomized for realism since our dataset lacks height data) to create a more dynamic 3D skyline.
  - Implement a "Glow" effect for high-risk assets using a secondary GeoJsonLayer with wider stroke and low opacity.

### 4. Interactivity: Dynamic Filters
- Add a persistent sidebar filter for facility categories (e.g., "Show only Emergency Response").
- Add a "Facility Search" box to jump straight to a specific building name.

## Verification Plan
- **Latency Test**: Measure and log the time taken for `visible_gdf` calculation before and after R-Tree implementation.
- **Visual Audit**: Compare the new 3D skyline rendering against the previous flat/monochrome version.

## Sprint 5: Real-World Data Fusion (Satellite Integration) 🛰️

### [Component Name] NASA FIRMS API Connector

In this sprint, we replace manual simulations with live global satellite data from NASA's Fire Information for Resource Management System (FIRMS).

#### [NEW] [fetch_firms_data.py](file:///e:/wild-fire-twin/scripts/fetch_firms_data.py)
A persistent producer script that polls the NASA FIRMS MAPS API for active fire hotspots in California (VIIRS/MODIS sensors). It will publish these real-world events to the `fire_events` Kafka topic.

#### [MODIFY] [app.py](file:///e:/wild-fire-twin/dashboard/backend/app.py)
- **Data Source Toggle**: Add a filter to distinguish between "Simulated" and "Satellite Live" fire events.
- **Intensity Mapping**: Use the `brightness` or `frp` (Fire Radiative Power) from FIRMS to scale the fire origin size on the map.
