"""
Data Loader — Extracted data loading logic for clean separation.

Handles:
  - Cached building GeoDataFrame loading from Parquet
  - Alert loading from DuckDB live store
"""

import os
import sys
import streamlit as st
import geopandas as gpd

# Add project root to path for alert_sink imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from alert_sink.duckdb_store import get_latest_alerts, get_alert_count

# Paths
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "california_essential_buildings.parquet")
DATA_PATH = os.path.abspath(DATA_PATH)


def check_data_exists():
    """Verify that the master building dataset is available."""
    if not os.path.exists(DATA_PATH):
        st.error("California master dataset not found. Please ensure the state-wide spatial join has completed.")
        st.stop()


@st.cache_data
def load_and_classify_buildings() -> gpd.GeoDataFrame:
    """
    Load the California buildings GeoParquet and apply semantic categorization.
    Results are cached by Streamlit — only runs once per session.
    """
    try:
        gdf = gpd.read_parquet(DATA_PATH)
    except Exception as e:
        st.error(f"Error reading dataset: {e}")
        return gpd.GeoDataFrame()

    if gdf.empty:
        return gdf

    # Semantic Mapping Logic
    def categorize(raw_type):
        raw_type = str(raw_type).title()
        medical = ['Hospital', 'Clinic', 'Doctors', 'Pharmacy', 'Ambulance Station',
                    'Nursing Home', 'Animal Hospital', 'Dentist', 'Veterinary']
        education = ['School', 'College', 'University', 'Prep School', 'Music School',
                     'Language School', 'Trade School', 'Kindergarten', 'Day Care', 'Childcare']
        emergency = ['Fire Station', 'Police', 'Emergency Response', 'Ambulance Station']

        if any(m in raw_type for m in medical):
            return "Medical", [220, 20, 60, 200]     # Deep Crimson
        if any(e in raw_type for e in education):
            return "Education", [255, 215, 0, 200]    # Amber Gold
        if any(em in raw_type for em in emergency):
            return "Emergency", [255, 140, 0, 200]    # Vivid Orange
        return "Civic/Other", [112, 128, 144, 200]    # Slate Gray

    gdf['category_data'] = gdf['building_type'].apply(categorize)
    gdf['category'] = gdf['category_data'].apply(lambda x: x[0])
    gdf['color'] = gdf['category_data'].apply(lambda x: x[1])

    return gdf


def load_alerts(limit: int = 500, source: str = "all") -> list[dict]:
    """Load latest alerts from DuckDB live store."""
    return get_latest_alerts(limit=limit, source=source)


def load_alert_count(source: str = "all") -> int:
    """Quick count of alerts in the live store."""
    return get_alert_count(source=source)


def filter_to_viewport(gdf: gpd.GeoDataFrame, center_lat: float, center_lon: float,
                       margin: float = 0.05) -> gpd.GeoDataFrame:
    """Filter GeoDataFrame to buildings visible in the current map viewport."""
    return gdf[
        (gdf.geometry.centroid.y > center_lat - margin) &
        (gdf.geometry.centroid.y < center_lat + margin) &
        (gdf.geometry.centroid.x > center_lon - margin) &
        (gdf.geometry.centroid.x < center_lon + margin)
    ].copy()
