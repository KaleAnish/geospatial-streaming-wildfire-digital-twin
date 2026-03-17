import os
import sys
import json
import typing

# Patch for Python 3.13+ compatibility with PySpark 3.4.1
# PySpark tries to import from typing.io, which was removed in Python 3.13
if sys.version_info >= (3, 13) and not hasattr(typing, 'io'):
    from typing import BinaryIO
    class MockIOModule:
        BinaryIO = BinaryIO
    
    mock_io = MockIOModule()
    typing.io = mock_io
    sys.modules['typing.io'] = mock_io

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

from sedona.spark import SedonaContext

# --- Configuration ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
OUTPUT_TOPIC = os.getenv("KAFKA_TOPIC_OUTPUT", "at_risk_assets")
# Point to the new CA Master Dataset
BUILDING_DATA_PATH = os.path.join(os.getcwd(), "data", "california_essential_buildings.parquet")
CHECKPOINT_DIR = os.path.join(os.getcwd(), "data", "spark_checkpoints", "fire_twin")

# Fix PySpark python executable detection on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Base Risk buffer in degrees (~500m base)
BASE_RISK_DEGREES = float(os.getenv("RISK_BUFFER_METERS", "500")) / 111000.0 

def create_spark_session():
    # Configure Spark for Sedona and Kafka
    config = SedonaContext.builder() \
        .appName("WildfireTwin-SpatialEngine") \
        .config("spark.jars.packages", 
                "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,"
                "org.datasyslab:geotools-wrapper:1.7.0-28.5,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    sedona = SedonaContext.create(config)
    return sedona

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Load static building data (California Master)
    print("\n[STEP 1] Loading California Master Building Assets...")
    
    if os.path.exists(BUILDING_DATA_PATH):
        print(f"Loading CA buildings from: {BUILDING_DATA_PATH}")
        # Apply ST_GeomFromWKB to ensure it's a Sedona Geometry object
        spark.read.parquet(BUILDING_DATA_PATH).createOrReplaceTempView("raw_buildings")
        spark.sql("CREATE OR REPLACE TEMP VIEW buildings AS SELECT building_type, building_name, ST_GeomFromWKB(geometry) as geometry FROM raw_buildings")
        print(f"[Done] Loaded California essential buildings into 'buildings' view.")
    else:
        print(f"Critical Error: {BUILDING_DATA_PATH} not found.")
        sys.exit(1)

    # 2. Define schema for incoming Kafka events (Enriched with Weather)
    event_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("is_fire", BooleanType(), True),
        StructField("wind_speed_mph", DoubleType(), True),
        StructField("wind_direction_deg", DoubleType(), True),
        StructField("humidity_percent", DoubleType(), True)
    ])

    # 3. Read streaming data from Kafka
    print(f"[STEP 2] Connecting to Kafka Stream: {INPUT_TOPIC}")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parse JSON and convert to structured columns
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), event_schema).alias("data")) \
        .select("data.*")
    
    parsed_stream.createOrReplaceTempView("fire_events_stream")

    # 5. Spatial Join Processing with Predictive Wind Modeling
    # We create a base circle, scale it into an ellipse based on wind speed, 
    # and rotate it based on wind direction to form a predictive risk cone.
    # Base radius is ~500m. Wind speed multiplier scales it forward.
    risk_query = """
        SELECT 
            f.event_id,
            f.event_time,
            f.latitude as fire_lat,
            f.longitude as fire_lon,
            f.temperature,
            f.wind_speed_mph,
            f.wind_direction_deg,
            f.humidity_percent,
            b.building_type,
            b.building_name,
            ST_AsText(b.geometry) as building_geom
        FROM fire_events_stream f
        JOIN buildings b
        ON ST_Intersects(
            -- Translate the ellipse so the fire origin is at the tail, then rotate it
            ST_Rotate(
                ST_Translate(
                    ST_Scale(
                        ST_Buffer(ST_Point(CAST(f.longitude AS Decimal(24,20)), CAST(f.latitude AS Decimal(24,20))), {base_buffer}),
                        1.0, -- X scale (width stays base buffer)
                        -- Y scale (length extends based on wind speed. 1 mph = +10% length)
                        1.0 + (COALESCE(f.wind_speed_mph, 0.0) * 0.1)
                    ),
                    0.0,
                    -- Shift origin so the point is at the base of the cone, not the center
                    ({base_buffer} * (1.0 + (COALESCE(f.wind_speed_mph, 0.0) * 0.1))) / 2.0
                ),
                -- Rotate based on wind direction. Radians = Degrees * PI / 180.
                -- Subtracted from 180/360 depending on orientation to point 'downwind'
                RADIANS(COALESCE(f.wind_direction_deg, 0.0)),
                ST_Point(CAST(f.longitude AS Decimal(24,20)), CAST(f.latitude AS Decimal(24,20)))
            ), 
            b.geometry
        )
        WHERE f.is_fire = true
    """.format(base_buffer=BASE_RISK_DEGREES)

    risk_assets_stream = spark.sql(risk_query)

    # 6. Write results to Output Kafka topic
    print(f"[STEP 3] Starting Asset-Risk Stream -> {OUTPUT_TOPIC}")
    kafka_output = risk_assets_stream \
        .select(to_json(struct("*")).alias("value"))

    query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
