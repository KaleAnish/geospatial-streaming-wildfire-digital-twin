"""
Wildfire Twin — Streamlit Dashboard

Architecture:
  - Building data loaded from GeoParquet (cached)
  - Alerts loaded from DuckDB live store (populated by alert_sink consumer)
  - NO direct Kafka consumption — fully decoupled
"""

import os
import sys
import json
import uuid
from datetime import datetime, timezone
import streamlit as st
import pydeck as pdk
import folium
from streamlit_folium import st_folium
from kafka import KafkaProducer

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dashboard.backend.data_loader import (
    check_data_exists,
    load_and_classify_buildings,
    load_alerts,
    load_alert_count,
    filter_to_viewport,
)
from dashboard.backend.map_layers import (
    build_static_layers,
    build_dynamic_layers,
    apply_alert_highlighting,
)
from scripts.fetch_weather_data import fetch_live_weather
from alert_sink.duckdb_store import delete_simulations

# --- Kafka Simulation Logic ---
def trigger_simulation(lat: float, lon: float, temp: float):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )
    
    live_weather = fetch_live_weather(lat, lon)
    if not live_weather:
        live_weather = {
            "temperature_f": temp,
            "humidity_percent": 30.0,
            "wind_speed_mph": 0.0,
            "wind_direction_deg": 0.0
        }
        
    event = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event_id": f"sim_{uuid.uuid4()}",
        "sensor_id": "dashboard_sim",
        "latitude": lat,
        "longitude": lon,
        "temperature": float(live_weather["temperature_f"]),
        "is_fire": True,
        "wind_speed_mph": float(live_weather["wind_speed_mph"]),
        "wind_direction_deg": float(live_weather["wind_direction_deg"]),
        "humidity_percent": float(live_weather["humidity_percent"])
    }
    
    producer.send(topic, value=event)
    producer.flush()
    producer.close()

# --- Page Config ---
st.set_page_config(layout="wide", page_title="California Essential Buildings")
st.title("🗺️ California Essential Infrastructure")
st.caption("State-wide fusion of 11.5M Microsoft structural polygons and 79k OpenStreetMap POIs.")

# --- Data Validation ---
check_data_exists()

# --- City Coordinate Registry ---
CITIES = {
    "🌐 California (Whole State)": (36.7783, -119.4179),
    "Riverside": (33.9533, -117.3961),
    "Los Angeles": (34.0522, -118.2437),
    "San Francisco": (37.7749, -122.4194),
    "San Diego": (32.7157, -117.1611),
    "Sacramento": (38.5816, -121.4944),
    "Irvine/UCR": (33.9533, -117.3961),
}

# --- Sidebar: Navigation & Live Alerts ---
with st.sidebar:
    st.header("Navigation")
    
    # 1. Data Source Filter
    st.subheader("📡 Data Feed Focus")
    data_source_display = st.radio(
        "Isolate Dashboard Data:",
        options=["All Activity (Live + Sim)", "Live Satellite Only (FIRMS)", "Simulated Tests Only"],
        label_visibility="collapsed"
    )
    
    # Map selection to internal source string
    if data_source_display == "Live Satellite Only (FIRMS)":
        st.session_state.data_source = "live"
    elif data_source_display == "Simulated Tests Only":
        st.session_state.data_source = "sim"
    else:
        st.session_state.data_source = "all"

    selected_city = st.selectbox(
        "🗺️ Teleport Viewport", list(CITIES.keys()), index=0
    )

    st.divider()

    st.header("🎮 Simulation Mode (What-If)")
    st.caption("1. Click the map below to choose an ignition point.")
    
    # Folium Mini-Map for coordinate selection
    m = folium.Map(location=[CITIES[selected_city][0], CITIES[selected_city][1]], zoom_start=10)
    m.add_child(folium.LatLngPopup())
    map_data = st_folium(m, height=250, use_container_width=True, returned_objects=["last_clicked"])
    
    sim_lat = CITIES[selected_city][0]
    sim_lon = CITIES[selected_city][1]
    
    if map_data and map_data.get("last_clicked"):
        sim_lat = map_data["last_clicked"]["lat"]
        sim_lon = map_data["last_clicked"]["lng"]

    with st.form("sim_form"):
        st.write(f"**Target Coordinates:** `{sim_lat:.5f}`, `{sim_lon:.5f}`")
        st.caption("2. Set fallback conditions and simulate.")
        sim_temp = st.slider("Fallback Temp (°F)", min_value=50.0, max_value=120.0, value=85.0)
        
        if st.form_submit_button("🔥 Simulate Fire Here"):
            with st.spinner("Publishing simulation to Kafka..."):
                trigger_simulation(sim_lat, sim_lon, sim_temp)
            st.success("Simulation triggered! Wait a few seconds for map update.")

    if st.button("🗑️ Clear Previous Simulations", use_container_width=True):
        delete_simulations()
        st.toast("🧹 All simulated data wiped from DuckDB.")

    st.divider()

    # Live Alert Panel — auto-refreshes from DuckDB every 5 seconds
    @st.fragment(run_every=5)
    def live_alert_panel():
        st.header("⚡ Live Risk Alerts")

        # Read source from session state for filtering
        source = st.session_state.get('data_source', 'all')
        alerts = load_alerts(limit=100, source=source)
        alert_count = len(alerts)

        if alerts:
            st.metric("🔥 Active Alerts", alert_count)

            # Weather context from most recent alert
            latest = alerts[0]  # Already sorted DESC by event_time
            st.info(
                f"**Live Weather Context:**\n"
                f"🌡️ {latest.get('temperature', '--')}°F\n"
                f"💧 {latest.get('humidity_percent', '--')}%\n"
                f"💨 {latest.get('wind_speed_mph', '--')} mph @ "
                f"{latest.get('wind_direction_deg', '--')}°"
            )

            # Show the most recent 5 alerts
            for alert in alerts[:5]:
                st.warning(
                    f"**RISK**: {alert.get('building_name', 'Unnamed Facility')}\n"
                    f"Type: {alert.get('building_type')}"
                )
        else:
            st.info("No active fire threats detected.")
            st.caption("Alerts auto-refresh every 5 seconds from the live store.")

    live_alert_panel()

# --- Load Buildings (Static) ---
center_lat, center_lon = CITIES[selected_city]

if "California" in selected_city:
    zoom_level = 6
else:
    zoom_level = 13

with st.spinner("Processing state-wide assets..."):
    full_gdf = load_and_classify_buildings()

# Filter to viewport unless viewing whole state
if "California" in selected_city:
    base_visible_gdf = full_gdf
else:
    base_visible_gdf = filter_to_viewport(full_gdf, center_lat, center_lon)

# --- Auto-Refreshing Map Fragment ---
@st.fragment(run_every=5)
def render_live_map():
    # Load fresh alerts
    source = st.session_state.get('data_source', 'all')
    alerts = load_alerts(limit=500, source=source)
    
    # Copy base GDF so we don't mutate the static cached layer, then highlight
    visible_gdf = apply_alert_highlighting(base_visible_gdf.copy(), alerts)
    
    # Build Map Layers
    static_layers = build_static_layers(visible_gdf)
    dynamic_layers = build_dynamic_layers(alerts)
    all_layers = static_layers + dynamic_layers
    
    # Render Map
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom_level,
        pitch=45,
    )
    
    deck = pdk.Deck(
        views=[pdk.View(type="MapView", controller=True)],  # Unlocked pan & zoom!
        layers=all_layers,
        initial_view_state=view_state,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        tooltip={
            "text": "Facility: {building_name}\nCategory: {category}\nRaw Type: {building_type}"
        },
    )
    
    # Streamlit warning fix: use_container_width is deprecated
    st.pydeck_chart(deck, height=600)

render_live_map()

# --- Footer Stats Fragment ---
st.divider()

@st.fragment(run_every=5)
def render_footer():
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("Category Distribution")
        if not base_visible_gdf.empty:
            st.dataframe(base_visible_gdf['category'].value_counts())
    with col2:
        st.subheader("Legend")
        st.markdown("🔴 **Medical** (Hospitals)")
        st.markdown("🟡 **Education** (Schools)")
        st.markdown("🟠 **Emergency** (Fire, Police)")
        st.markdown("⭐ **ALERT** (Inside Wind Cone)")
    with col3:
        st.subheader("System Health")
        st.metric("Total Master Buildings", f"{len(full_gdf):,}")
        source = st.session_state.get('data_source', 'all')
        st.metric("Live Active Alerts", load_alert_count(source=source))

render_footer()
