import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer

from fetch_weather_data import fetch_live_weather

CITIES = {
    "Riverside": (33.9533, -117.3961),
    "Los Angeles": (34.0522, -118.2437),
    "San Francisco": (37.7749, -122.4194),
    "San Diego": (32.7157, -117.1611),
    "Sacramento": (38.5816, -121.4944),
    "UCR": (33.9737, -117.3281), # Precision UCR for user's specific check
    "Riverside_School": (33.9825, -117.3569) # Known building exact coordinate
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
    parser.add_argument("--exact", action="store_true", help="Do not add random jitter to coordinates")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_INPUT", "fire_events")
    
    producer = build_producer(bootstrap)
    lat, lon = CITIES[args.city]

    print(f"Triggering {args.count} fire event(s) in {args.city} ({lat}, {lon})...")
    
    # Fetch live weather once for the general city coordinate
    print("Fetching live weather data for simulation context...")
    live_weather = fetch_live_weather(lat, lon)
    if live_weather:
        print(f"Weather context: {live_weather['temperature_f']}F, Wind: {live_weather['wind_speed_mph']}mph @ {live_weather['wind_direction_deg']}deg")
    else:
        print("Warning: Could not fetch weather. Using default zero-wind values fallback.")
        live_weather = {
            "temperature_f": args.temp,
            "humidity_percent": 30.0,
            "wind_speed_mph": 0.0,
            "wind_direction_deg": 0.0
        }

    for i in range(args.count):
        if args.exact:
            event_lat, event_lon = lat, lon
        else:
            # Add tiny jitter to simulate different spots in the city
            event_lat = lat + random.uniform(-0.005, 0.005)
            event_lon = lon + random.uniform(-0.005, 0.005)
        
        event = {
            "event_time": utc_now_iso(),
            "event_id": f"sim_{uuid.uuid4()}",
            "sensor_id": f"sim_sensor_{i}",
            "latitude": float(event_lat),
            "longitude": float(event_lon),
            "temperature": float(live_weather["temperature_f"]),
            "is_fire": True,
            # Weather Enrichment
            "wind_speed_mph": float(live_weather["wind_speed_mph"]),
            "wind_direction_deg": float(live_weather["wind_direction_deg"]),
            "humidity_percent": float(live_weather["humidity_percent"])
        }
        
        producer.send(topic, value=event)
        print(f"   [Sent] {event['event_id']} at ({event_lat:.4f}, {event_lon:.4f})")
        time.sleep(0.5)

    producer.flush()
    print("Done.")

if __name__ == "__main__":
    main()
