"""
End-to-End Latency Measurement Tool
-----------------------------------
Calculates the pipeline latency by comparing the original fire event creation time
with the final database ingestion time.
"""

import os
import duckdb
import pandas as pd
from datetime import datetime

# Point to the active DuckDB file
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "alerts.duckdb")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}. Is the alert_sink consumer running?")
        return

    print("Connecting to DuckDB Hot Store (Read-Only)...")
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Calculate latency difference in milliseconds
        query = """
            SELECT 
                event_id,
                building_name,
                event_time,
                ingestion_ts,
                date_diff('millisecond', CAST(event_time AS TIMESTAMP), ingestion_ts) as latency_ms
            FROM alerts_live
            ORDER BY ingestion_ts DESC
            LIMIT 500
        """
        
        df = con.execute(query).df()
        
        if df.empty:
            print("No alerts found in the database yet. Run a simulation first.")
            return
            
        avg_latency = df['latency_ms'].mean() / 1000.0
        max_latency = df['latency_ms'].max() / 1000.0
        min_latency = df['latency_ms'].min() / 1000.0
        
        print("\n" + "="*50)
        print("🚀 PIPELINE LATENCY REPORT (Last 500 Alerts)")
        print("="*50)
        print(f"Total Evaluated : {len(df)} alerts")
        print(f"Average Latency : {avg_latency:.3f} seconds")
        print(f"Minimum Latency : {min_latency:.3f} seconds")
        print(f"Maximum Latency : {max_latency:.3f} seconds")
        print("="*50)
        
        if avg_latency < 30.0:
            print("\n✅ SUCCESS: Pipeline successfully meets the < 30s latency claim!")
        else:
            print("\n⚠️ WARNING: Pipeline exceeds the 30s latency claim. Consider scaling Spark executors.")
            
        # --- Generate Chart for PPT ---
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        plt.figure(figsize=(10, 6))
        sns.histplot(df['latency_ms'] / 1000.0, bins=20, kde=True, color='purple')
        plt.axvline(avg_latency, color='red', linestyle='dashed', linewidth=2, label=f'Average: {avg_latency:.2f}s')
        plt.axvline(30.0, color='orange', linestyle='dashed', linewidth=2, label='Target: < 30s')
        
        plt.title('End-to-End Pipeline Latency Distribution', fontsize=16)
        plt.xlabel('Latency (Seconds)', fontsize=12)
        plt.ylabel('Frequency (Number of Events)', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        chart_path = os.path.join(os.path.dirname(__file__), 'latency_distribution.png')
        plt.savefig(chart_path, dpi=300)
        print(f"\n📊 Automatically saved chart for PowerPoint: {chart_path}")
            
    except Exception as e:
        print(f"Failed to query database: {e}")
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    main()
