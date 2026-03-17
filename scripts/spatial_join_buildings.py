import os
import sys
import shutil

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
from pyspark.sql.functions import expr
from sedona.spark import SedonaContext

def run_spatial_join():
    print("Setting up Windows environment variables...")
    # Get the absolute path to the project root (E:\wild-fire-twin)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hadoop_home = os.path.join(project_root, "infra", "hadoop")
    
    # Set HADOOP_HOME and add bin to PATH
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")
    
    print(f"[*] HADOOP_HOME set to: {hadoop_home}")

    print("Initializing Sedona Context (H3 Partitioning Enabled)...")
    # Rest of your existing config code...
    config = SedonaContext.builder() \
        .appName("Spatial Join CA-Wide-H3") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,org.datasyslab:geotools-wrapper:1.7.0-28.5") \
        .getOrCreate()
        
    sedona = SedonaContext.create(config)
    
    buildings_path = "data/riverside_buildings.parquet"
    pois_path = "data/riverside_pois.csv"
    # New output path to avoid confusion
    output_path = "data/california_essential_buildings_indexed.parquet"
    
    if not os.path.exists(buildings_path) or not os.path.exists(pois_path):
        print("Missing input data files.")
        return
        
    print("Loading Microsoft geometries...")
    buildings_df = sedona.read.parquet(buildings_path)
    buildings_df.createOrReplaceTempView("raw_buildings")
    
    # Standardize Geometry
    if "geometry" in buildings_df.columns:
        sedona.sql("SELECT ST_GeomFromWKB(geometry) AS building_geom FROM raw_buildings").createOrReplaceTempView("buildings")
    else:
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
    pois_df = pois_df.filter("lat IS NOT NULL AND lon IS NOT NULL")
    pois_df.createOrReplaceTempView("raw_pois")
    
    sedona.sql("""
        SELECT id, type, name, ST_Point(CAST(lon AS Decimal(24,20)), CAST(lat AS Decimal(24,20))) AS poi_geom 
        FROM raw_pois
    """).createOrReplaceTempView("pois")
    
    print("Executing Spatial Join & H3 Indexing...")
    # 1. We join to find essential buildings
    # 2. We calculate ST_H3CellIDs (Resolution 7 is the sweet spot for dashboard speed vs file count)
    joined_df = sedona.sql("""
        SELECT 
            b.building_geom as geometry, 
            p.type as building_type, 
            p.name as building_name,
            ST_H3CellIDs(ST_Centroid(b.building_geom), 7, false)[0] as h3_res7
        FROM buildings b, pois p
        WHERE ST_Contains(b.building_geom, p.poi_geom)
    """)
    
    # Clean up any nulls to ensure partition integrity
    joined_df = joined_df.filter("h3_res7 IS NOT NULL")
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
        
    print(f"Writing H3 Partitioned GeoParquet to {output_path}...")
    
    # We partitionBy h3_res7 so the dashboard can load by cell ID
    joined_df.write \
        .format("geoparquet") \
        .partitionBy("h3_res7") \
        .save(output_path)
        
    print("Done! Data is ready for the high-performance dashboard.")

if __name__ == "__main__":
    run_spatial_join()