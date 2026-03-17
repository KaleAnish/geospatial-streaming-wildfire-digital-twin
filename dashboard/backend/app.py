import os
import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import json
import numpy as np
import uuid
from shapely import wkt

from kafka import KafkaConsumer

st.set_page_config(layout="wide", page_title="California Essential Buildings")

st.title("🗺️ California Essential Infrastructure")
st.caption("State-wide fusion of 11.5M Microsoft structural polygons and 79k OpenStreetMap POIs.")

# --- Kafka & Data Configuration ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ALERT_TOPIC = "at_risk_assets"
data_path = os.path.join(os.getcwd(), "data", "california_essential_buildings.parquet")

if not os.path.exists(data_path):
    st.error("California master dataset not found. Please ensure the state-wide spatial join has completed.")
    st.stop()

# --- Session State for Live Alerts ---
if 'alerts' not in st.session_state:
    st.session_state.alerts = []

# City Coordinate Registry for Fixed Viewports
CITIES = {
    "Riverside": (33.9533, -117.3961),
    "Los Angeles": (34.0522, -118.2437),
    "San Francisco": (37.7749, -122.4194),
    "San Diego": (32.7157, -117.1611),
    "Sacramento": (38.5816, -121.4944),
    "Irvine/UCR": (33.9533, -117.3961)
}

with st.sidebar:
    st.header("Navigation")
    selected_city = st.selectbox("Teleport to City (Fixed Viewport)", list(CITIES.keys()), index=0)
    
    st.divider()
    st.header("⚡ Live Risk Alerts")
    
    if st.button("🔄 Sync Live Alerts"):
        try:
            # Use a unique group ID to ensure we see all messages in the topic
            session_group = f"streamlit-{uuid.uuid4()}"
            consumer = KafkaConsumer(
                ALERT_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=session_group,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000 # 5 seconds to catch everything
            )
            
            new_alerts = []
            for message in consumer:
                new_alerts.append(message.value)
            
            if new_alerts:
                # Deduplicate or just add fresh ones if they aren't already there
                existing_ids = {a.get('event_id') + a.get('building_name', '') for a in st.session_state.alerts}
                added_count = 0
                for alert in new_alerts:
                    alert_key = alert.get('event_id') + alert.get('building_name', '')
                    if alert_key not in existing_ids:
                        st.session_state.alerts.append(alert)
                        added_count += 1
                
                if added_count > 0:
                    st.toast(f"Found {added_count} new risk matches!", icon="🔥")
                else:
                    st.info("No new unique alerts found.")
            else:
                st.info("No alerts found in the fire stream.")
            
            consumer.close()
        except Exception as e:
            st.error(f"Kafka connection failed: {e}")

    if st.session_state.alerts:
        if st.button("Clear Alerts"):
            st.session_state.alerts = []
            st.rerun()
            
        # Display weather context from the most recent alert
        latest = st.session_state.alerts[-1]
        st.info(f"**Live Weather Incident Context:**\n"
               f"🌡️ {latest.get('temperature', '--')}°F\n"
               f"💧 {latest.get('humidity_percent', '--')}%\n"
               f"💨 {latest.get('wind_speed_mph', '--')} mph @ {latest.get('wind_direction_deg', '--')}°")
        
        for alert in st.session_state.alerts[-5:]: # Show last 5
            st.warning(f"**RISK**: {alert.get('building_name', 'Unnamed Facility')}\nType: {alert.get('building_type')}")
    else:
        st.info("No active fire threats detected.")

center_lat, center_lon = CITIES[selected_city]
zoom_level = 14

@st.cache_data
def load_and_classify_buildings():
    try:
        gdf = gpd.read_parquet(data_path)
    except Exception as e:
        st.error(f"Error reading dataset: {e}")
        return gpd.GeoDataFrame()
        
    if gdf.empty:
        return gdf

    # Semantic Mapping Logic
    def categorize(raw_type):
        raw_type = str(raw_type).title()
        medical = ['Hospital', 'Clinic', 'Doctors', 'Pharmacy', 'Ambulance Station', 'Nursing Home', 'Animal Hospital', 'Dentist', 'Veterinary']
        education = ['School', 'College', 'University', 'Prep School', 'Music School', 'Language School', 'Trade School', 'Kindergarten', 'Day Care', 'Childcare']
        emergency = ['Fire Station', 'Police', 'Emergency Response', 'Ambulance Station']
        
        if any(m in raw_type for m in medical): return "Medical", [220, 20, 60, 200]    # Red
        if any(e in raw_type for e in education): return "Education", [255, 215, 0, 200] # Yellow/Gold
        if any(em in raw_type for em in emergency): return "Emergency", [255, 140, 0, 200] # Orange
        return "Civic/Other", [112, 128, 144, 200] # Slate Gray

    gdf['category_data'] = gdf['building_type'].apply(categorize)
    gdf['category'] = gdf['category_data'].apply(lambda x: x[0])
    gdf['color'] = gdf['category_data'].apply(lambda x: x[1])
    
    return gdf

with st.spinner("Processing state-wide assets..."):
    full_gdf = load_and_classify_buildings()

# Filter to viewport
margin = 0.05
visible_gdf = full_gdf[
    (full_gdf.geometry.centroid.y > center_lat - margin) &
    (full_gdf.geometry.centroid.y < center_lat + margin) &
    (full_gdf.geometry.centroid.x > center_lon - margin) &
    (full_gdf.geometry.centroid.x < center_lon + margin)
].copy()

# Add Alert Highlight if building is in session state
alert_names = [a.get('building_name') for a in st.session_state.alerts]
def apply_alert_color(row):
    if row['building_name'] in alert_names:
        return [255, 255, 0, 255] # Bright Yellow for alerts
    return row['color']

if not visible_gdf.empty:
    visible_gdf['color'] = visible_gdf.apply(apply_alert_color, axis=1)

# Prepare Fire Points and Wind Cones from alerts
# We recalculate the cone here for PyDeck rendering using turf.js equivalent logic or Polygon generation.
# For PyDeck, we can render the polygon directly if we construct it, or use a Scatterplot for origin 
# and a PolygonLayer for the cone. Let's create an approximate polygon for the cone.
import math
fire_geneses = []
wind_cones = []

BASE_RADIUS_METERS = 500

for a in st.session_state.alerts:
    # Origin
    fire_geneses.append({"lat": a['fire_lat'], "lon": a['fire_lon'], "name": "Fire Origin"})
    
    # Calculate Cone Polygon
    lat, lon = a['fire_lat'], a['fire_lon']
    wind_mph = float(a.get('wind_speed_mph', 0))
    wind_deg = float(a.get('wind_direction_deg', 0))
    
    # Length of cone depends on wind speed. 1 mph = 10% longer.
    length_meters = BASE_RADIUS_METERS * (1.0 + (wind_mph * 0.1))
    width_meters = BASE_RADIUS_METERS  # Keep width roughly the same as base diameter
    
    # Earth radius in meters
    R = 6378137
    
    # Point 1: Origin (Tail of the cone)
    # Point 2 & 3: The wide part of the cone pushed forward by wind
    
    # Convert wind direction to Radians. 
    # Meteorological wind direction: 0=North, 90=East. This is where wind acts FROM.
    # To find where fire goes TO, we add 180 degrees.
    travel_deg = (wind_deg + 180) % 360
    travel_rad = math.radians(travel_deg)
    
    # Calculate the tip/center of the far end of the cone
    dest_lat = lat + (length_meters / R) * (180 / math.pi)
    dest_lon = lon + (length_meters / R) * (180 / math.pi) / math.cos(lat * math.pi/180)
    
    # For a simple visual representation in PyDeck without complex Shapely buffering on the fly:
    # We will use an arc/polygon. A simpler PyDeck approach is an ArcLayer or an oriented IconLayer, 
    # but a manual triangle/cone is best.
    
    angle_offset = math.radians(30) # 30 degree spread on each side of the central wind vector
    
    left_rad = travel_rad - angle_offset
    right_rad = travel_rad + angle_offset
    
    left_lat = lat + (length_meters / R) * math.cos(left_rad) * (180 / math.pi)
    left_lon = lon + (length_meters / R) * math.sin(left_rad) * (180 / math.pi) / math.cos(lat * math.pi/180)
    
    right_lat = lat + (length_meters / R) * math.cos(right_rad) * (180 / math.pi)
    right_lon = lon + (length_meters / R) * math.sin(right_rad) * (180 / math.pi) / math.cos(lat * math.pi/180)

    wind_cones.append({
        "polygon": [[lon, lat], [right_lon, right_lat], [left_lon, left_lat], [lon, lat]],
        "color": [255, 140, 0, 120] # Translucent Dark Orange for the cone
    })

fire_df = pd.DataFrame(fire_geneses)
cone_df = pd.DataFrame(wind_cones)

# PyDeck
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level,
    pitch=45
)

layers = [
    pdk.Layer(
        "GeoJsonLayer",
        visible_gdf,
        opacity=0.8,
        stroked=True,
        filled=True,
        extruded=True,
        get_elevation=30,
        get_fill_color="color",
        get_line_color=[255, 255, 255, 150],
        pickable=True
    )
]

if not cone_df.empty:
    layers.append(
        pdk.Layer(
            "PolygonLayer",
            cone_df,
            get_polygon="polygon",
            get_fill_color="color",
            get_line_color=[255, 69, 0, 255],
            filled=True,
            stroked=True,
            line_width_min_pixels=2
        )
    )

if not fire_df.empty:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            fire_df,
            get_position=["lon", "lat"],
            get_color=[255, 0, 0, 255], # Solid Bright Red for origin
            get_radius=150, # Small tight radius for just the origin point
            pickable=True
        )
    )

deck = pdk.Deck(
    views=[pdk.View(type="MapView", controller=False)], 
    layers=layers,
    initial_view_state=view_state,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    tooltip={"text": "Facility: {building_name}\nCategory: {category}\nRaw Type: {building_type}"}
)

st.pydeck_chart(deck, height=600, use_container_width=True)

st.divider()

col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Category Distribution")
    if not visible_gdf.empty:
        st.dataframe(visible_gdf['category'].value_counts(), use_container_width=True)
with col2:
    st.subheader("Legend")
    st.markdown("🔴 **Medical** (Hospitals)")
    st.markdown("🟡 **Education** (Schools)")
    st.markdown("🟠 **Emergency** (Fire, Police)")
    st.markdown("⭐ **ALERt** (Inside Wind Cone)")
with col3:
    st.subheader("System Health")
    st.metric("Total Master Buildings", f"{len(full_gdf):,}")
    st.metric("Live Active Alerts", len(st.session_state.alerts))
