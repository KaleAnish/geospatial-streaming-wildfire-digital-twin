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
import h3

# Add project root to path for alert_sink imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from alert_sink.duckdb_store import get_latest_alerts, get_alert_count

# Paths
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "california_essential_buildings_indexed.parquet")
DATA_PATH = os.path.abspath(DATA_PATH)


def check_data_exists():
    """Verify that the master building dataset is available."""
    if not os.path.exists(DATA_PATH):
        st.error("California master dataset not found. Please ensure the state-wide spatial join has completed.")
        st.stop()


@st.cache_data(ttl=3600)
def load_and_classify_buildings(center_lat=None, center_lon=None, margin_deg=0.3) -> gpd.GeoDataFrame:
    """
    Load the California buildings GeoParquet and apply semantic categorization.
    Results are cached by Streamlit — only runs once per session.
    If center coordinates are provided, dynamically map those to H3 grid hexes 
    and push the filter down to the partition loader for extreme performance.
    """
    filters = None
    if center_lat is not None and center_lon is not None:
        # Calculate central H3 hex (Resolution 7 ~ 1.2km edge length)
        center_hex = h3.latlng_to_cell(float(center_lat), float(center_lon), 7)
        # Margin of say ~25 kilometers is roughly k=15 rings
        k_rings = 15
        if margin_deg < 0.2:
            k_rings = 10
        elif margin_deg > 0.5:
            k_rings = 25
            
        hexes = list(h3.grid_disk(center_hex, k_rings))
        # PySpark/Sedona stored H3 as BIGINT (decimal strings), but Python h3 returns hex strings.
        # Convert: '8729a0158ffffff' -> '608689658158645247'
        hexes = [str(int(h, 16)) for h in hexes]
        filters = [("h3_res7", "in", hexes)]

    try:
        # PyArrow only loads the parquet fragments matching our H3 criteria!
        gdf = gpd.read_parquet(DATA_PATH, filters=filters)
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


@st.cache_data(ttl=3600)
def load_h3_summary():
    """
    Build a lightweight H3 summary for state-wide visualization.
    Scans partition folders, counts buildings per res4 parent hex.
    Returns ~206 rows instead of ~79k polygons — instant PyDeck rendering.
    """
    import pyarrow.dataset as ds

    dataset = ds.dataset(DATA_PATH, partitioning="hive")
    # Read just the h3_res7 column (no geometry!) for speed
    table = dataset.to_table(columns=["h3_res7"])
    h3_col = table.column("h3_res7").to_pylist()

    # Aggregate: convert res7 decimal strings → hex → res4 parent → count
    from collections import Counter
    parent_counts = Counter()
    for dec_str in h3_col:
        try:
            hex_str = hex(int(dec_str))[2:]
            parent = h3.cell_to_parent(hex_str, 4)
            parent_counts[parent] += 1
        except Exception:
            continue

    # Build a simple DataFrame for H3HexagonLayer
    import pandas as pd
    summary = pd.DataFrame([
        {"h3_index": h3_id, "building_count": count}
        for h3_id, count in parent_counts.items()
    ])
    return summary


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
