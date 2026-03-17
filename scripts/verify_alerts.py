from kafka import KafkaConsumer
import json
import time

KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'at_risk_assets'

print(f"Connecting to {TOPIC}...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000 # wait for up to 10 seconds for messages
)

print("Waiting for messages...")
count = 0
for message in consumer:
    alert = message.value
    print(f"  [ALERT] Building: {alert.get('building_name', 'Unnamed')} ({alert.get('building_type')}) - Source Event: {alert.get('event_id')}")
    count += 1

print(f"\nTotal at-risk buildings detected: {count}")
if count > 0:
    print("✅ SUCCESS: The Unified Dynamic Stream is fully operational.")
else:
    print("❌ FAILURE: No buildings were detected in the fire zones. Is the simulation running?")
