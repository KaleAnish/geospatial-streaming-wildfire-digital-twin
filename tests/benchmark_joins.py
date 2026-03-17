"""
Sedona Spatial Join Benchmark
-----------------------------
Proves the academic claim of Big Data efficiency by comparing a standard
Spark nested-loop spatial comparison against Sedona's R-Tree partitioned Spatial Join.
"""

import sys
import os
import time
import typing

# Fix PySpark python executable detection on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if sys.version_info >= (3, 13) and not hasattr(typing, 'io'):
    from typing import BinaryIO
    class MockIOModule:
        BinaryIO = BinaryIO
    mock_io = MockIOModule()
    typing.io = mock_io
    sys.modules['typing.io'] = mock_io

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, broadcast
from sedona.spark import SedonaContext

def main():
    print("Initializing Benchmark Environment...")
    # Add project root to path so we can import the session builder
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from spark_processor.spatial_engine import create_spark_session
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # 1. Load Buildings (11.5M Master Dataset)
    BUILDING_DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "california_essential_buildings.parquet")
    print(f"Loading {BUILDING_DATA_PATH}...")
    b_raw = spark.read.parquet(BUILDING_DATA_PATH)
    b_raw.createOrReplaceTempView("raw_buildings")
    
    # Using 100,000 buildings for the benchmark to ensure it finishes in reasonable time
    spark.sql("CREATE OR REPLACE TEMP VIEW b_geom AS "
              "SELECT building_name, ST_GeomFromWKB(geometry) as geometry FROM raw_buildings LIMIT 100000")
    
    print("Building View created with 100,000 architectural polygons.")

    # 2. Simulate 1,000 dense fire perimeters
    print("Generating 1,000 simulated fire WKT polygons...")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW fires AS 
        SELECT 
            id as fire_id, 
            ST_Buffer(ST_Point(-118.2437 + (id*0.0001), 34.0522 + (id*0.0001)), 0.05) as fire_geom 
        FROM RANGE(1, 1000)
    """)

    # --- BENCHMARK 1: Standard Join (No Spatial Partitioning) ---
    print("\n" + "="*50)
    print("BENCHMARK 1: Standard Spatial Intersects (No R-Tree Join)")
    # Force spark NOT to broadcast or optimize spatially
    spark.conf.set("sedona.join.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("sedona.join.spatialPartitioningType", "none")
    
    start_time = time.time()
    res1 = spark.sql("""
        SELECT count(*) as total_intersections 
        FROM fires f 
        JOIN b_geom b 
        ON ST_Intersects(f.fire_geom, b.geometry)
    """).collect()
    duration1 = time.time() - start_time
    print(f"Result: {res1[0][0]} intersections found.")
    print(f"Time  : {duration1:.2f} seconds")


    # --- BENCHMARK 2: Sedona Spatial Partitioning (R-Tree) ---
    print("\n" + "="*50)
    print("BENCHMARK 2: Sedona Optimized Spatial Join (QuadTree/R-Tree)")
    # Enable Sedona indexing
    spark.conf.set("sedona.join.spatialPartitioningType", "quadtree")
    spark.conf.set("sedona.join.indexType", "rtree")
    
    start_time = time.time()
    res2 = spark.sql("""
        SELECT /*+ BROADCAST(f) */ count(*) as total_intersections 
        FROM fires f 
        JOIN b_geom b 
        ON ST_Intersects(f.fire_geom, b.geometry)
    """).collect()
    duration2 = time.time() - start_time
    print(f"Result: {res2[0][0]} intersections found.")
    print(f"Time  : {duration2:.2f} seconds")

    print("\n" + "="*50)
    if duration2 > 0:
        print(f"🚀 PERFORMANCE GAIN: {duration1 / duration2:.1f}x Speedup leveraging Apache Sedona!")
    print("="*50)
    
    # --- Generate Chart for PPT ---
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        labels = ['Standard Python PySpark\n(Nested Loop Filter)', 'Apache Sedona\n(R-Tree Partitioning)']
        times = [duration1, duration2]
        
        plt.figure(figsize=(9, 6))
        ax = sns.barplot(x=labels, y=times, hue=labels, palette=['#e74c3c', '#2ecc71'], legend=False)
        
        plt.title('Big Data Spatial Join Performance Benchmark', fontsize=16, pad=15)
        plt.ylabel('Execution Time (Seconds)', fontsize=12)
        
        # Add exact times on top of bars
        for i, p in enumerate(ax.patches):
            ax.annotate(f"{times[i]:.2f}s", 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='center', 
                        xytext=(0, 9), textcoords='offset points',
                        fontsize=12, fontweight='bold')
                        
        if duration2 > 0:
            speedup = duration1 / duration2
            plt.figtext(0.5, 0.02, f"🚀 {speedup:.1f}x Speedup leveraging Apache Sedona QuadTree/R-Tree", 
                       ha="center", fontsize=13, fontweight='bold', bbox={"facecolor":"#ffeaa7", "alpha":0.5, "pad":8})
            
        plt.tight_layout(rect=[0, 0.08, 1, 1])
        
        chart_path = os.path.join(os.path.dirname(__file__), 'spatial_join_benchmark.png')
        plt.savefig(chart_path, dpi=300)
        print(f"\n📊 Automatically saved chart for PowerPoint: {chart_path}")
    except ImportError:
        pass
    
    spark.stop()

if __name__ == "__main__":
    main()
