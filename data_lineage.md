# Wildfire-Twin — Data Engineering Tracking

This document tracks the lineage, processing steps, and provenance of the datasets used in the Wildfire-Twin pipeline.

## 1. Microsoft Building Footprints
- **Source**: Microsoft Maps US Building Footprints v2 (Legacy Bing Maps team)
- **Coverage**: California state wide
- **Format (Source)**: GeoJSON (`California.geojson.zip` -> `California.geojson`)
- **Format (Intermediate)**: Line-delimited JSON (`California.ndjson`) to bypass Spark's complex nested-schema inference bugs.
- **Format (Final)**: Apache Sedona Spatially-Indexed GeoParquet (`riverside_buildings.parquet`)
- **Record Count**: 11,542,912
- **Purpose**: Provides high-accuracy satellite-derived coordinate geometries (polygons) for every structure.
- **Limitation**: Pure geometries; no human-readable semantic data (zoning, building type, occupancy).

## 2. California Essential Infrastructure (OpenStreetMap)
- **Source**: Overpass API (OpenStreetMap)
- **Coverage**: **Entire State of California** (Area-based query)
- **Format (Expected)**: GeoJSON/CSV Points of Interest (POIs).
- **Purpose**: Provides semantic classification and tags (e.g., `hospital`, `school`, `university`, `fire_station`, `police`).
- **Limitation**: OpenStreetMap coverage varies by region, but provides the best open-source semantic labels for structures.

## 3. The Unified "California Essential Buildings" Dataset
- **Methodology**: Spatial Join (`ST_Contains`).
- **Processing Engine**: Apache Spark + Apache Sedona.
- **Logic**: We cross-reference all 11,542,912 structural Microsoft polygons against a state-wide POI dataset of 79,200 points (Hospitals, Universities, Fire Stations, etc.).
- **Output**: `data/california_essential_buildings.parquet`. Produced by `scripts/spatial_join_buildings.py`. Found **52,570** buildings with semantic classifications across CA.
