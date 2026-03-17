"""
Alert Sink Consumer — Persistent Kafka → DuckDB bridge.

Continuously consumes from the `at_risk_assets` Kafka topic using a stable
consumer group and writes alerts into the DuckDB live store.

Usage:
    python alert_sink/consumer.py
"""

import os
import sys
import json
import signal
import time

from kafka import KafkaConsumer

# Add project root to path so we can import duckdb_store
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from alert_sink.duckdb_store import init_db, insert_alerts_batch


# --- Configuration ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "at_risk_assets")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "wildfire-alert-sink")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    print(f"\n[Alert Sink] Received signal {signum}, shutting down gracefully...")
    _shutdown = True


def main():
    global _shutdown

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Initialize DuckDB schema
    print("[Alert Sink] Initializing DuckDB store...")
    init_db()

    # Create Kafka consumer with stable group ID
    print(f"[Alert Sink] Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"[Alert Sink] Topic: {ALERT_TOPIC}")
    print(f"[Alert Sink] Consumer Group: {CONSUMER_GROUP}")

    consumer = KafkaConsumer(
        ALERT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="latest",        # Only consume NEW messages
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    print("[Alert Sink] Consumer started. Waiting for alerts...\n")

    total_ingested = 0
    start_time = time.time()

    try:
        while not _shutdown:
            batch = []

            try:
                for message in consumer:
                    batch.append(message.value)

                    if len(batch) >= BATCH_SIZE:
                        break
            except StopIteration:
                # consumer_timeout_ms triggered — no new messages
                pass

            if batch:
                insert_alerts_batch(batch)
                total_ingested += len(batch)
                elapsed = time.time() - start_time
                rate = total_ingested / elapsed if elapsed > 0 else 0
                print(f"[Alert Sink] Ingested {len(batch)} alerts "
                      f"(total={total_ingested}, rate={rate:.1f}/s)")

            if _shutdown:
                break

    except Exception as e:
        print(f"[Alert Sink] Fatal error: {e}")
        raise
    finally:
        print("[Alert Sink] Closing consumer...")
        consumer.close()
        print(f"[Alert Sink] Stopped. Total ingested: {total_ingested}")


if __name__ == "__main__":
    main()
