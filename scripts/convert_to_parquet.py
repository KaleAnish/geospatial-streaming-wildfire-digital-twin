"""
One-time utility to convert California.geojson to GeoParquet format.
Uses Sedona for efficient spatial data processing.
"""
import sys
import typing
import os

# Apply typing.io patch for Python 3.13+
if sys.version_info >= (3, 13) and not hasattr(typing, 'io'):
    from typing import BinaryIO
    class MockIOModule:
        BinaryIO = BinaryIO
    mock_io = MockIOModule()
    typing.io = mock_io
    sys.modules['typing.io'] = mock_io

from sedona.spark import SedonaContext

def main():
    # Environment Setup for Windows (from start_spark.ps1 logic)
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.getcwd(), "infra", "hadoop"))
        os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

    print("Initializing Sedona Context...")
    config = SedonaContext.builder() \
        .master("local[*]") \
        .appName("WildfireTwin-Conversion") \
        .config("spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,"
                "org.datasyslab:geotools-wrapper:1.7.0-28.5") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    sedona = SedonaContext.create(config)
    
    geojson_path = "data/California.ndjson"
    parquet_path = "data/riverside_buildings.parquet"
    
    if not os.path.exists(geojson_path):
        print(f"Error: {geojson_path} not found.")
        return

    print(f"Reading {geojson_path} as raw text...")
    # Read each line as a raw string to avoid complex schema issues
    raw_df = sedona.read.text(geojson_path)
    
    print("Parsing GeoJSON features...")
    # Each row has a 'value' column containing the full GeoJSON feature string
    buildings_df = raw_df.selectExpr(
        "ST_GeomFromGeoJSON(value) as geometry"
    )
    
    # Optional: Filter out any null geometries if parsing failed for some rows
    buildings_df = buildings_df.filter("geometry IS NOT NULL")
    
    print("Sample data:")
    buildings_df.show(5)
    
    print(f"Writing to {parquet_path} in GeoParquet format...")
    if os.path.exists(parquet_path):
        import shutil
        shutil.rmtree(parquet_path)
        
    buildings_df.write.format("geoparquet").save(parquet_path)
    
    print(f"Successfully converted to {parquet_path}")
    
    # Final check: record count
    count = sedona.read.format("geoparquet").load(parquet_path).count()
    print(f"Total building records: {count}")
    
    sedona.stop()

if __name__ == "__main__":
    main()
