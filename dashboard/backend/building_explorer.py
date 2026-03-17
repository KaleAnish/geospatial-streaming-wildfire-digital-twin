import os
import streamlit as st
import pandas as pd
import pydeck as pdk
import geopandas as gpd
from shapely import wkb

# Config - Removed st.set_page_config for multi-page compatibility

st.title("🏠 California Building Footprint Explorer")
st.caption("Visualizing the 11.5 Million buildings ingested in Sprint 2.")

# Sidebar for controls
st.sidebar.header("Visualization Settings")
sample_size = st.sidebar.slider("Number of buildings to sample", 1000, 20000, 10000)
refresh = st.sidebar.button("Get New Random Sample")

buildings_path = os.path.join(os.getcwd(), "data", "riverside_buildings.parquet")

if not os.path.exists(buildings_path):
    st.error(f"Dataset not found at {buildings_path}. Please complete Sprint 2 first.")
else:
    with st.spinner(f"Loading a random sample of {sample_size} buildings..."):
        try:
            # We use geopandas to read a sample of the parquet
            # Since it's a directory (Spark format), we read the first bit or sample
            # For simplicity in this demo, we read the whole thing and sample (caution: might be slow if file is huge)
            # Better: Read just one part file if it's too big.
            
            # actually, let's just use pandas to sample indices if we wanted to be fast, 
            # but for 11M, even reading the metadata can be slow.
            # Let's find a part file.
            data_dir = os.path.join(buildings_path)
            part_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
            if not part_files:
                st.error("No parquet part files found.")
                st.stop()
            
            # Read from the first part file for the sample to keep it snappy
            sample_df = gpd.read_parquet(os.path.join(data_dir, part_files[0]))
            
            if len(sample_df) > sample_size:
                sample_df = sample_df.sample(sample_size)
            
            # Prepare for pydeck
            # Pydeck likes GeoJSON or lat/lon. geopandas.to_json is slow for many records.
            # We'll extract coordinates for a PolygonLayer or just use the geometry.
            
            st.write(f"Showing {len(sample_df)} buildings from part file `{part_files[0]}`")
            
            # Layer for buildings
            building_layer = pdk.Layer(
                "GeoJsonLayer",
                sample_df,
                opacity=0.8,
                stroked=False,
                filled=True,
                get_fill_color=[200, 200, 200, 150],
                get_line_color=[255, 255, 255],
                pickable=True
            )

            # View state - center on the sample
            bounds = sample_df.total_bounds # [minx, miny, maxx, maxy]
            view_state = pdk.ViewState(
                latitude=(bounds[1] + bounds[3]) / 2,
                longitude=(bounds[0] + bounds[2]) / 2,
                zoom=12,
                pitch=45
            )

            r = pdk.Deck(
                layers=[building_layer],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/light-v9",
                tooltip={"text": "Building Footprint"}
            )

            st.pydeck_chart(r, width="stretch")
            
            st.subheader("Data Preview")
            st.dataframe(sample_df.head(10), width="stretch")

        except Exception as e:
            st.error(f"Error loading visualization: {e}")

st.info("Note: This explorer samples a subset of the 11.5M records to maintain browser performance. In Sprint 3, this will be integrated with live fire events.")
