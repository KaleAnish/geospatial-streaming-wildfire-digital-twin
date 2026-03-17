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

import pyspark.sql.functions as F
from sedona.spark import SedonaContext

# Riverside Center
center_lat, center_lon = 33.9533, -117.3961

# Viewport approx bounds (+/- 0.04 deg)
min_lon, max_lon = center_lon - 0.04, center_lon + 0.04
min_lat, max_lat = center_lat - 0.04, center_lat + 0.04

def run_filter():
    config = SedonaContext.builder() \
        .appName("Filter Essential Buildings") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,org.datasyslab:geotools-wrapper:1.7.0-28.5") \
        .getOrCreate()
        
    sedona = SedonaContext.create(config)
    
    input_path = "data/riverside_buildings.parquet"
    output_path = "data/essential_buildings.parquet"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return
        
    print(f"Reading {input_path}")
    df = sedona.read.format("geoparquet").load(input_path)
    df.createOrReplaceTempView("buildings")
    
    # Filter using ST_Intersects
    bbox_wkt = f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    
    query = f"""
        SELECT ST_AsText(geometry) as wkt
        FROM buildings
        WHERE ST_Intersects(geometry, ST_GeomFromText('{bbox_wkt}'))
    """
    filtered_df = sedona.sql(query)
    
    count = filtered_df.count()
    print(f"Found {count} buildings in the viewport.")
    
    if os.path.exists(output_path):
        import shutil
        shutil.rmtree(output_path)
        
    filtered_df.write.parquet(output_path)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    run_filter()
