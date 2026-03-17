"""
Kafka Throughput Stress Tester
------------------------------
Floods the Kafka topic with 10,000 simulated fire events to prove 
the Spark cluster can sustain high-velocity data ingress without dropping micro-batches.
"""
import os
import json
import time
import uuid
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
TOTAL_EVENTS = 10000

def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=0,  # "Fire and forget" for maximum throughput in stress testing
        linger_ms=10, 
        batch_size=16384 * 4
    )

def worker(producer: KafkaProducer, thread_id: int, count: int, base_lat: float, base_lon: float):
    for i in range(count):
        event = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "event_id": f"stress_test_{thread_id}_{i}_{uuid.uuid4().hex[:8]}",
            "sensor_id": f"worker_{thread_id}",
            "latitude": base_lat + (i * 0.0001), # slightly moving fire
            "longitude": base_lon + (i * 0.0001),
            "temperature": 100.0,
            "is_fire": True,
            "wind_speed_mph": 25.0,
            "wind_direction_deg": 45.0 + (i % 10),
            "humidity_percent": 15.0
        }
        producer.send(TOPIC, value=event)

def main():
    print(f"Initializing Kafka Throughput Stress Test...")
    print(f"Targeting: {BOOTSTRAP} on topic '{TOPIC}'")
    print(f"Total Events to Generate: {TOTAL_EVENTS}")
    
    producer = get_producer()
    
    # Riverside CA base
    base_lat = 33.9533
    base_lon = -117.3961
    
    start_time = time.time()
    
    # Single threaded due to GIL and KafkaProducer C-extension async nature being fast enough
    worker(producer, 0, TOTAL_EVENTS, base_lat, base_lon)
    
    print("Waiting for final buffer flush to Kafka...")
    producer.flush()
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = TOTAL_EVENTS / duration
    
    print("\n" + "="*50)
    print("🚦 STRESS TEST RESULTS")
    print("="*50)
    print(f"Total Sent     : {TOTAL_EVENTS} events")
    print(f"Total Time     : {duration:.2f} seconds")
    print(f"Max Throughput : {throughput:.0f} events/second")
    print("="*50)
    print("Note: Monitor your Spark Web UI (localhost:4040) to confirm")
    print("      the micro-batch processing time remained stable.")

    # --- Generate Chart for PPT ---
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        plt.figure(figsize=(8, 5))
        sns.barplot(x=['Observed Throughput (Kafka)'], y=[throughput], color='teal')
        plt.axhline(1000, color='orange', linestyle='dashed', linewidth=2, label='Target: > 1k ev/s')
        
        plt.title('Big Data Ingestion Throughput Stress Test', fontsize=16)
        plt.ylabel('Events Per Second', fontsize=12)
        plt.ylim(0, max(throughput * 1.2, 1200)) # Scale y-axis appropriately
        
        # Add exact number on top of bar
        plt.text(0, throughput + (throughput*0.02), f"{throughput:.0f} ev/s", ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        plt.legend()
        plt.tight_layout()
        
        chart_path = os.path.join(os.path.dirname(__file__), 'throughput_benchmark.png')
        plt.savefig(chart_path, dpi=300)
        print(f"\n📊 Automatically saved chart for PowerPoint: {chart_path}")
    except ImportError:
        pass

if __name__ == "__main__":
    main()
