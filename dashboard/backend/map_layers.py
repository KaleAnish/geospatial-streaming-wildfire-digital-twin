"""
Map Layers — Separated PyDeck layer construction.

Static layers (buildings) are loaded once.
Dynamic layers (fire points, wind cones) update with alerts.
"""

import math
import pandas as pd
import pydeck as pdk
import geopandas as gpd


# Base risk radius in meters (matches Spark engine)
BASE_RADIUS_METERS = 500
EARTH_RADIUS = 6378137  # meters


def build_static_layers(visible_gdf: gpd.GeoDataFrame) -> list:
    """
    Build PyDeck layers for the static building context.
    These should only reload when the viewport city changes.
    """
    if visible_gdf.empty:
        return []

    return [
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
            pickable=True,
        )
    ]


def _compute_wind_cone(alert: dict) -> dict:
    """
    Compute a triangular wind cone polygon for a single alert.
    The cone extends from the fire origin in the downwind direction.
    """
    lat = alert['fire_lat']
    lon = alert['fire_lon']
    wind_mph = float(alert.get('wind_speed_mph', 0))
    wind_deg = float(alert.get('wind_direction_deg', 0))

    # Length scales with wind speed (1 mph = +10% base length)
    length_meters = BASE_RADIUS_METERS * (1.0 + (wind_mph * 0.1))

    # Fire travels OPPOSITE to meteorological wind direction
    travel_deg = (wind_deg + 180) % 360
    travel_rad = math.radians(travel_deg)

    # 30-degree spread on each side
    angle_offset = math.radians(30)
    left_rad = travel_rad - angle_offset
    right_rad = travel_rad + angle_offset

    left_lat = lat + (length_meters / EARTH_RADIUS) * math.cos(left_rad) * (180 / math.pi)
    left_lon = lon + (length_meters / EARTH_RADIUS) * math.sin(left_rad) * (180 / math.pi) / math.cos(
        lat * math.pi / 180)

    right_lat = lat + (length_meters / EARTH_RADIUS) * math.cos(right_rad) * (180 / math.pi)
    right_lon = lon + (length_meters / EARTH_RADIUS) * math.sin(right_rad) * (180 / math.pi) / math.cos(
        lat * math.pi / 180)

    return {
        "polygon": [[lon, lat], [right_lon, right_lat], [left_lon, left_lat], [lon, lat]],
        "color": [255, 140, 0, 120]  # Translucent Dark Orange
    }


def build_dynamic_layers(alerts: list[dict]) -> list:
    """
    Build PyDeck layers for fire origins and wind risk cones.
    These update with each alert refresh cycle.
    """
    if not alerts:
        return []

    layers = []

    # Fire origin points
    fire_points = [
        {"lat": a['fire_lat'], "lon": a['fire_lon'], "name": "Fire Origin"}
        for a in alerts
        if a.get('fire_lat') and a.get('fire_lon')
    ]

    # Wind cone polygons
    wind_cones = [
        _compute_wind_cone(a)
        for a in alerts
        if a.get('fire_lat') and a.get('fire_lon')
    ]

    # Wind cone layer
    if wind_cones:
        cone_df = pd.DataFrame(wind_cones)
        layers.append(
            pdk.Layer(
                "PolygonLayer",
                cone_df,
                get_polygon="polygon",
                get_fill_color="color",
                get_line_color=[255, 69, 0, 255],
                filled=True,
                stroked=True,
                line_width_min_pixels=2,
            )
        )

    # Fire origin scatter layer
    if fire_points:
        fire_df = pd.DataFrame(fire_points)
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                fire_df,
                get_position=["lon", "lat"],
                get_color=[255, 0, 0, 255],  # Bright Red
                get_radius=150,
                pickable=True,
            )
        )

    return layers


def apply_alert_highlighting(visible_gdf: gpd.GeoDataFrame, alerts: list[dict]) -> gpd.GeoDataFrame:
    """
    Override building colors for at-risk facilities.
    Buildings matching alert names get bright yellow highlighting.
    """
    if visible_gdf.empty or not alerts:
        return visible_gdf

    alert_names = {a.get('building_name') for a in alerts if a.get('building_name')}

    def _highlight(row):
        if row['building_name'] in alert_names:
            return [255, 255, 0, 255]  # Bright Yellow
        return row['color']

    visible_gdf['color'] = visible_gdf.apply(_highlight, axis=1)
    return visible_gdf
