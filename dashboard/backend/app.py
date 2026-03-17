import os
import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import numpy as np
from shapely import wkt

st.set_page_config(layout="wide", page_title="California Essential Buildings")

st.title("🏥 California Essential Buildings (Sample Viewport)")
st.caption("Map fixed to Riverside, CA. Displaying critical infrastructure. (Note: Original Microsoft dataset lacks building types; simulated here for demo purposes).")

data_path = os.path.join(os.getcwd(), "data", "essential_buildings.parquet")

if not os.path.exists(data_path):
    st.error("Building dataset not found. Please ensure the filtering script has completed.")
    st.stop()

# Riverside Center (Fixed Viewport)
center_lat, center_lon = 33.9533, -117.3961
zoom_level = 13.5

@st.cache_data
def load_essential_buildings():
    # Read the pre-filtered spark DataFrame (now just standard parquet with WKT strings)
    df = pd.read_parquet(data_path)
    
    if df.empty:
        return gpd.GeoDataFrame()
        
    # Convert WKT string to actual shapely geometries
    gdf = gpd.GeoDataFrame(df, geometry=df['wkt'].apply(wkt.loads), crs="EPSG:4326")
    
    # Simulate Essential Building Classification
    np.random.seed(42) # Consistent simulation
    types = ["Hospital", "School", "Fire Station", "Police Station", "Residential", "Commercial"]
    # Make essential buildings rare but present
    probs = [0.03, 0.05, 0.02, 0.02, 0.70, 0.18]
    gdf['building_type'] = np.random.choice(types, len(gdf), p=probs)
    
    essential_types = ["Hospital", "School", "Fire Station", "Police Station"]
    gdf = gdf[gdf['building_type'].isin(essential_types)].copy()
    
    # Assign RGB colors based on type
    colors = {
        "Hospital": [220, 20, 60, 220],        # Crimson
        "School": [255, 215, 0, 220],          # Gold
        "Fire Station": [255, 69, 0, 220],     # Red-Orange
        "Police Station": [30, 144, 255, 220], # Dodger Blue
    }
    gdf['color'] = gdf['building_type'].map(colors)
    return gdf

with st.spinner("Loading and classifying essential building data..."):
    essential_gdf = load_essential_buildings()

if essential_gdf.empty:
    st.warning("No buildings found in this immediate viewport.")
else:
    st.success(f"Found {len(essential_gdf)} critical infrastructure buildings in the current view.")

# PyDeck Rendering
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level,
    pitch=0
)

layer = pdk.Layer(
    "GeoJsonLayer",
    essential_gdf,
    opacity=0.9,
    stroked=True,
    filled=True,
    get_fill_color="color",
    get_line_color=[255, 255, 255, 100],
    pickable=True
)

deck = pdk.Deck(
    # Disables user zoom and pan for a locked map
    views=[pdk.View(type="MapView", controller=False)], 
    layers=[layer],
    initial_view_state=view_state,
    # Use a free, reliable Carto base map (doesn't require Mapbox token)
    map_style="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
    tooltip={"text": "Facility: {building_type}"}
)

st.pydeck_chart(deck, width="stretch")

st.divider()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Data Summary")
    type_counts = essential_gdf['building_type'].value_counts()
    st.dataframe(type_counts, width="stretch")
with col2:
    st.subheader("Legend")
    st.markdown("🔴 **Hospital** (Medical Response)")
    st.markdown("🟡 **School** (Evacuation Shelter)")
    st.markdown("🟠 **Fire Station** (First Responders)")
    st.markdown("🔵 **Police Station** (Emergency Command)")
