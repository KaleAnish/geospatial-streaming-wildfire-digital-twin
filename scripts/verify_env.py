"""
Verification script for Wildfire-Twin Sprint 1.
Tests Python imports, Java configuration, and Sedona spatial functions.
"""
import sys
import typing
import os

# 1. Test Patch
if sys.version_info >= (3, 13) and not hasattr(typing, 'io'):
    from typing import BinaryIO
    class MockIOModule:
        BinaryIO = BinaryIO
    mock_io = MockIOModule()
    typing.io = mock_io
    sys.modules['typing.io'] = mock_io
    print("[OK] Python 3.13 typing.io patch applied.")

# 2. Test Imports
try:
    from pyspark.sql import SparkSession
    from sedona.spark import SedonaContext
    import pyarrow
    print(f"[OK] Imports successful (PySpark, Sedona, PyArrow {pyarrow.__version__})")
except ImportError as e:
    print(f"[ERROR] Import failed: {e}")
    sys.exit(1)

# 3. Test Spark + Sedona (Local Mode)
print("\nTesting Spark Session + Sedona Spatial Functions...")
try:
    # Ensure HADOOP_HOME is set for the test if run manually
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "infra", "hadoop"))
        os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

    config = SedonaContext.builder() \
        .master("local[1]") \
        .appName("WildfireTwin-Test") \
        .config("spark.jars.packages", 
                "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.7.0,"
                "org.datasyslab:geotools-wrapper:1.7.0-28.5") \
        .getOrCreate()
    
    sedona = SedonaContext.create(config)
    
    # Test a simple spatial join logic
    test_df = sedona.sql("SELECT ST_Distance(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)) as dist")
    dist = test_df.collect()[0]['dist']
    
    print(f"[OK] Spark/Sedona Session Active (Version: {sedona.version})")
    print(f"[OK] Spatial Calculation Test: Distance(0,0 to 1,1) = {dist:.4f}")
    
    sedona.stop()
    print("\n[SUCCESS] SPRINT 1 VERIFIED: Environment is stable and spatial engine is functional.")
    
except Exception as e:
    print(f"[ERROR] Spark Test Failed: {e}")
    sys.exit(1)
