"""Quick integration test for the DuckDB alert store."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ["DUCKDB_PATH"] = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "test_alerts.duckdb")

from alert_sink.duckdb_store import (
    init_db, insert_alert, insert_alerts_batch,
    get_latest_alerts, get_alert_count, clear_alerts
)

def test():
    init_db()
    print("1. DB initialized OK")

    insert_alert({
        "event_id": "e1", "event_time": "2026-03-17T00:00:00Z",
        "fire_lat": 33.95, "fire_lon": -117.39, "temperature": 95.0,
        "wind_speed_mph": 15.0, "wind_direction_deg": 270.0, "humidity_percent": 20.0,
        "building_name": "UCR Medical", "building_type": "Hospital",
        "building_geom": "POINT(-117.39 33.95)"
    })
    print("2. Single insert OK")

    insert_alerts_batch([
        {
            "event_id": "e2", "event_time": "2026-03-17T00:01:00Z",
            "fire_lat": 33.96, "fire_lon": -117.38, "temperature": 100.0,
            "wind_speed_mph": 20.0, "wind_direction_deg": 180.0, "humidity_percent": 15.0,
            "building_name": "Riverside School", "building_type": "School",
            "building_geom": "POINT(-117.38 33.96)"
        },
        {  # Duplicate of e1 — should be deduped
            "event_id": "e1", "event_time": "2026-03-17T00:00:00Z",
            "fire_lat": 33.95, "fire_lon": -117.39, "temperature": 95.0,
            "wind_speed_mph": 15.0, "wind_direction_deg": 270.0, "humidity_percent": 20.0,
            "building_name": "UCR Medical", "building_type": "Hospital",
            "building_geom": "POINT(-117.39 33.95)"
        }
    ])
    print("3. Batch insert with dedup OK")

    count = get_alert_count()
    assert count == 2, f"Expected 2, got {count}"
    print(f"4. Count = {count} (expected 2) OK")

    alerts = get_latest_alerts(10)
    assert len(alerts) == 2, f"Expected 2 alerts, got {len(alerts)}"
    print(f"5. Latest alerts = {len(alerts)} records OK")
    print(f"   First: {alerts[0]['building_name']}")
    print(f"   Second: {alerts[1]['building_name']}")

    clear_alerts()
    assert get_alert_count() == 0, "Expected 0 after clear"
    print("6. Clear OK")

    # Cleanup test DB
    db_path = os.environ["DUCKDB_PATH"]
    if os.path.exists(db_path):
        os.remove(db_path)

    print("")
    print("All DuckDB store tests PASSED")

if __name__ == "__main__":
    test()
