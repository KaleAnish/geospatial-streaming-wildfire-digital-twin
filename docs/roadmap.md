# Development Roadmap: Finalizing the Digital Twin

Based on the [Coursework Analysis](coursework_analysis.md), here is a concrete, step-by-step plan for implementing the remaining features claimed in your academic report.

---

## Sprint 10: Building the NASA FIRMS "Live Monitor" Mode

**Goal:** Ingest actual real-time thermal anomalies from global satellites (MODIS/VIIRS) into the pipeline.

### Step-by-Step Instructions:
1. **API Key Generation:** Go to the NASA Earthdata FIRMS portal and generate an API key for CSV/JSON streaming.
2. **Create `producer/nasa_firms_ingest.py`:**
   - Write a Python script that uses `requests.get()` to hit the FIRMS active fire endpoint for California's bounding box.
   - Parse the returned data (often CSV format) into JSON.
   - For every listed `latitude`/`longitude`, call your existing `fetch_live_weather()` function to enrich the data with wind and temperature.
   - Puhish the enriched JSON payload to the Kafka topic: `fire_events`.
3. **Execution Logic:** Use `schedule` or a standard `time.sleep(300)` `while` loop to poll the NASA API every 5 minutes and immediately pipe new anomalies to Spark.

---

## Sprint 11: Interactive Dashboard UI (Simulation Mode)

**Goal:** Fulfill the claim of an interactive "What-If" Analysis UI where users click the map to trigger simulated fires.

### Step-by-Step Instructions:
1. **Map Click Events:** Streamlit's `st.pydeck_chart` is notoriously one-way and doesn't easily capture click events back to Python. You will need to either:
   - **Option A:** Use `streamlit-folium` which natively captures user click coordinates.
   - **Option B:** Upgrade PyDeck with a secondary `st.text_input` coordinate form. "Enter Lat/Lon to Simulate."
2. **Kafka Bridge:** When the Streamlit user inputs coordinates (or clicks), immediately initialize a Kafka Producer inside `app.py`. Emit a JSON payload identical to the `simulate_fire.py` payload directly into the `fire_events` topic.
3. **UI Toggle:** Add a `st.toggle("Simulation Mode / Live Satellite Mode")`.
   - If "Live", query DuckDB for `event_type = 'satellite'`.
   - If "Simulated", query DuckDB for `event_type = 'simulation'`. 

---

## Sprint 12: Restoring the Spark Predictive Wind Cone Math

**Goal:** Re-implement the complex predictive wind-cone spatial intersection in Spark without the previous geometry translation bug.

### Step-by-Step Instructions:
1. **Mathematical Review:** In `spatial_engine.py`, the previous SQL intersection used `ST_Rotate(ST_Translate(...))`, which inadvertently shifted the cone's base off the origin point.
2. **The Corrected Logic:** You must ensure `ST_Translate` shifts the geometric center of the `ST_Scale`'d ellipse exactly halfway along its new extended length, relative to the *unscaled* origin point, *before* rotation.
3. **Alternatively (Easier Spark Math):** Create a custom UDF in PySpark that takes `lat, lon, wind_mph, wind_deg`, generates a WKT string (Well-Known Text) representing the exact cone polygon, and parses it with `ST_GeomFromText()`. This avoids massive nested SQL functions and directly mimics the logic functioning perfectly right now in `app.py -> _compute_wind_cone()`.

---

## Sprint 13: Academic Evaluation Scripts

**Goal:** Provide the metrics claimed in your project outline (latency, throughput, join benchmarking) to drop directly into your final PDF report.

### Step-by-Step Instructions:
1. **Latency Script:** Measure End-to-End Latency.
   - Ensure the Kafka Producer injects a `sent_timestamp` into the JSON payload.
   - In `alert_sink/consumer.py`, when inserting into DuckDB, calculate the difference between `datetime.now()` and `sent_timestamp`.
   - Create a Streamlit gauge chart displaying average latency (Target: < 30s).
2. **Throughput Stress Test:**
   - Create a script `stress_test.py`.
   - Rapidly generate `10,000` concurrent Kafka messages with random California coordinates.
   - Monitor Spark execution times using the Spark Web UI (port 4040) to prove the cluster didn't crash. Screenshot this for the report.
3. **Spatial Join Benchmarks:**
   - Run `spatial_engine.py` using standard PySpark native joins (no Sedona `ST_Intersects`). Measure execution time.
   - Run `spatial_engine.py` with the Sedona cluster. Document the speedup gained by R-Tree indexing to prove "Big Data efficiency."
