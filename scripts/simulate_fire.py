import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer

CITIES = {
    "Riverside": (33.9533, -117.3961),
    "Los Angeles": (34.0522, -118.2437),
    "San Francisco": (37.7749, -122.4194),
    "San Diego": (32.7157, -117.1611),
    "Sacramento": (38.5816, -121.4944),
    "UCR": (33.9737, -117.3281) # Precision UCR for user's specific check
}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

def main():
    parser = argparse.ArgumentParser(description="Wildfire-Twin Fire Event Simulator")
    parser.add_argument("--city", choices=list(CITIES.keys()), default="Riverside")
    parser.add_argument("--count", type=int, default=1, help="Number of fire events to send")
    parser.add_argument("--temp", type=float, default=85.0, help="Temperature in Celsius")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
    
    producer = build_producer(bootstrap)
    lat, lon = CITIES[args.city]

    print(f"Triggering {args.count} fire event(s) in {args.city} ({lat}, {lon})...")

    for i in range(args.count):
        # Add tiny jitter to simulate different spots in the city
        event_lat = lat + random.uniform(-0.005, 0.005)
        event_lon = lon + random.uniform(-0.005, 0.005)
        
        event = {
            "event_time": utc_now_iso(),
            "event_id": str(uuid.uuid4()),
            "sensor_id": f"sim_sensor_{i}",
            "latitude": float(event_lat),
            "longitude": float(event_lon),
            "temperature": float(args.temp),
            "is_fire": True
        }
        
        producer.send(topic, value=event)
        print(f"   [Sent] {event['event_id']} at ({event_lat:.4f}, {event_lon:.4f})")
        time.sleep(0.5)

    producer.flush()
    print("Done.")

if __name__ == "__main__":
    main()
