import os
import sys

# Monkey-patch typing.io for PySpark 3.4.x on Python 3.13+
if sys.version_info >= (3, 13):
    import typing
    import types
    if not hasattr(typing, 'io'):
        io_module = types.ModuleType('io')
        io_module.BinaryIO = typing.BinaryIO # type: ignore
        typing.io = io_module # type: ignore
        sys.modules['typing.io'] = io_module

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sedona.spark import SedonaContext

def run_spatial_join():
    print("Initializing Sedona Context (California Scale)...")
    config = SedonaContext.builder() \
        .appName("Spatial Join CA-Wide") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,org.datasyslab:geotools-wrapper:1.7.0-28.5") \
        .getOrCreate()
        
    sedona = SedonaContext.create(config)
    
    # riverside_buildings.parquet actually contains all of CA (11.5M records)
    buildings_path = "data/riverside_buildings.parquet"
    pois_path = "data/riverside_pois.csv"
    output_path = "data/california_essential_buildings.parquet"
    
    if not os.path.exists(buildings_path) or not os.path.exists(pois_path):
        print("Missing input data files.")
        return
        
    print("Loading Microsoft geometries...")
    buildings_df = sedona.read.parquet(buildings_path)
    buildings_df.printSchema()
    buildings_df.createOrReplaceTempView("raw_buildings")
    
    # Check schema and handle geometry conversion
    if "geometry" in buildings_df.columns:
        print("Using native 'geometry' column. Applying explicit conversion to ensure Sedona compatibility.")
        # Sometimes Sedona needs an explicit cast or ST function to 'activate' the geometry type from a parquet file
        sedona.sql("SELECT ST_GeomFromWKB(geometry) AS building_geom FROM raw_buildings").createOrReplaceTempView("buildings")
    else:
        print("Using 'wkt' column fallback.")
        sedona.sql("SELECT ST_GeomFromText(wkt) AS building_geom FROM raw_buildings").createOrReplaceTempView("buildings")
    
    print("Loading OpenStreetMap POIs...")
    poi_schema = StructType([
        StructField("id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True)
    ])
    
    pois_df = sedona.read.csv(pois_path, header=True, schema=poi_schema)
    pois_df.printSchema()
    pois_df = pois_df.filter("lat IS NOT NULL AND lon IS NOT NULL")
    pois_df.createOrReplaceTempView("raw_pois")
    
    # Create spatial points from lat/lon
    sedona.sql("""
        SELECT id, type, name, ST_Point(CAST(lon AS Decimal(24,20)), CAST(lat AS Decimal(24,20))) AS poi_geom 
        FROM raw_pois
    """).createOrReplaceTempView("pois")
    
    print("Executing Spatial Join (ST_Contains)...")
    # Spatial Join: Find the building polygon that contains the POI point
    # We use INNER JOIN because the dashboard ONLY wants the essential buildings (the ones with a POI match)
    joined_df = sedona.sql("""
        SELECT b.building_geom as geometry, p.type as building_type, p.name as building_name
        FROM buildings b, pois p
        WHERE ST_Contains(b.building_geom, p.poi_geom)
    """)
    
    joined_count = joined_df.count()
    print(f"Spatial Join success! Found {joined_count} Microsoft buildings containing an OSM essential POI.")
    
    if joined_count == 0:
        print("Warning: Spatial join yielded 0 results. Perhaps coordinate reference systems are mismatched, or points are outside polygons.")
        
    if os.path.exists(output_path):
        import shutil
        shutil.rmtree(output_path)
        
    print(f"Writing to {output_path} (GeoParquet)...")
    joined_df.write.format("geoparquet").save(output_path)
    print("Done!")

if __name__ == "__main__":
    run_spatial_join()
