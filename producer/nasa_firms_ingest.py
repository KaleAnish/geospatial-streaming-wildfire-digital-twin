"""
NASA FIRMS Live Ingestion Service
---------------------------------
Periodically pulls Near Real-Time (NRT) thermal anomalies (active fires) 
from the NASA FIRMS API, enriches them with OpenWeatherMap data, 
and publishes them to the Kafka `fire_events` topic.
"""

import os
import csv
import json
import time
import uuid
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

# We reuse the specific module from simulate_fire to avoid duplicating logic
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))
from fetch_weather_data import fetch_live_weather

API_KEY = "f731c98d36b4d091d64e4423d76a036a"
# The string provided by user had a hardcoded date. We use the dynamic generic path for NRT (Near Real Time).
# Source: VIIRS_NOAA20_NRT
# Area (California rough BBOX): -124,32,-114,42
# Day_range: 1 (last 24 hours)
FIRMS_API_URL = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{API_KEY}/VIIRS_NOAA20_NRT/-124,32,-114,42/1"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
POLL_INTERVAL_SECONDS = 300  # 5 minutes

def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

def fetch_nasa_anomalies() -> list[dict]:
    """Fetch and parse the CSV from NASA FIRMS."""
    print(f"[{datetime.now().isoformat()}] Fetching live anomalies from NASA FIRMS...")
    try:
        response = requests.get(FIRMS_API_URL, timeout=30)
        response.raise_for_status()
        
        # FIRMS returns CSV format
        csv_text = response.text.strip().split("\n")
        if len(csv_text) <= 1:
            print("No active anomalies detected in the last 24 hours for this region.")
            return []
            
        reader = csv.DictReader(csv_text)
        anomalies = list(reader)
        print(f"Found {len(anomalies)} thermal anomalies.")
        return anomalies
    except Exception as e:
        print(f"Error fetching from NASA FIRMS: {e}")
        return []

def process_and_publish(producer: KafkaProducer, anomalies: list[dict]):
    """Enrich with weather and publish to Kafka."""
    # We maintain a simple cache of 'seen' anomalies running in memory so we don't spam 
    # Kafka with the exact same satellite dots every 5 minutes.
    # FIRMS doesn't provide unique event IDs, so we use a composite (lat, lon, acq_time) hash.
    published_count = 0
    
    for row in anomalies:
        try:
            lat = float(row['latitude'])
            lon = float(row['longitude'])
            acq_date = row['acq_date']
            acq_time = row['acq_time'] # HHMM format
            confidence = row.get('confidence', 'n/a')
            
            # Filter out explicit low confidence if needed
            if confidence == 'low':
                continue
            
            # Some satellites only update multiple times a day. We create a deterministic ID 
            # so the Streamlit dashboard or Spark engine can deduplicate if needed.
            unique_str = f"{lat:.4f}_{lon:.4f}_{acq_date}_{acq_time}"
            event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))
            
            # Fetch realtime weather context at the exact fire coordinates
            weather = fetch_live_weather(lat, lon)
            
            if not weather:
                # Fallback to zero wind if OpenWeatherMap fails
                weather = {
                    "temperature_f": 85.0,
                    "humidity_percent": 30.0,
                    "wind_speed_mph": 0.0,
                    "wind_direction_deg": 0.0
                }
            
            # Build the unified JSON payload expected by Spark
            event = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event_id": event_id,
                "sensor_id": f"nasa_viirs_{confidence}",
                "latitude": lat,
                "longitude": lon,
                "temperature": float(weather["temperature_f"]),
                "is_fire": True,
                "wind_speed_mph": float(weather["wind_speed_mph"]),
                "wind_direction_deg": float(weather["wind_direction_deg"]),
                "humidity_percent": float(weather["humidity_percent"])
            }
            
            producer.send(TOPIC, value=event)
            published_count += 1
            
            # Avoid hammering OpenWeatherMap API limit
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error processing anomaly {row}: {e}")
            
    producer.flush()
    print(f"Successfully enriched and published {published_count} new fire events to Kafka.")

def main():
    print(f"Starting NASA FIRMS Ingestion Service for topic: {TOPIC}")
    producer = get_producer()
    
    # We keep track of previously seen IDs to avoid sending duplicates every 5 mins
    seen_ids = set()
    
    try:
        while True:
            anomalies = fetch_nasa_anomalies()
            
            # Filter out anomalies we've already processed this session
            new_anomalies = []
            for row in anomalies:
                unique_str = f"{float(row['latitude']):.4f}_{float(row['longitude']):.4f}_{row['acq_date']}_{row['acq_time']}"
                event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))
                if event_id not in seen_ids:
                    seen_ids.add(event_id)
                    new_anomalies.append(row)
            
            if new_anomalies:
                print(f"Processing {len(new_anomalies)} newly detected anomalies...")
                process_and_publish(producer, new_anomalies)
            else:
                print("No new anomalies since last check.")
                
            print(f"Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\nShutting down NASA FIRMS ingest service...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
