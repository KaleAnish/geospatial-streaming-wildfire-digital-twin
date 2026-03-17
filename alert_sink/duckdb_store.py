"""
DuckDB Live Alert Store for Wildfire Twin.

Provides CRUD operations for the alerts_live table.
Used by the Alert Sink consumer and the Streamlit dashboard.
"""

import os
import time
import duckdb
from datetime import datetime, timezone, timedelta

# Default DB path — overridable via env var
DB_PATH = os.getenv("DUCKDB_PATH", os.path.join(os.path.dirname(__file__), "..", "storage", "alerts.duckdb"))
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", os.path.join(os.path.dirname(__file__), "..", "storage", "archive"))

# Resolve to absolute paths
DB_PATH = os.path.abspath(DB_PATH)
ARCHIVE_DIR = os.path.abspath(ARCHIVE_DIR)


def _get_connection(read_only=False):
    """Get a DuckDB connection with retry logic for file locks."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    max_retries = 10
    retry_delay = 0.2
    
    for attempt in range(max_retries):
        try:
            return duckdb.connect(DB_PATH, read_only=read_only)
        except duckdb.IOException as e:
            if "used by another process" in str(e) or "lock" in str(e).lower():
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
            raise e


def init_db():
    """Create the alerts_live table if it doesn't exist."""
    con = _get_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS alerts_live (
            event_id        VARCHAR,
            event_time      TIMESTAMP,
            fire_lat        DOUBLE,
            fire_lon        DOUBLE,
            temperature     DOUBLE,
            wind_speed_mph  DOUBLE,
            wind_direction_deg DOUBLE,
            humidity_percent DOUBLE,
            building_name   VARCHAR,
            building_type   VARCHAR,
            building_geom   VARCHAR,
            ingestion_ts    TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (event_id, building_name)
        )
    """)
    con.close()


def insert_alert(alert: dict):
    """
    Insert an alert into the live table.
    Uses INSERT OR REPLACE for dedup on (event_id, building_name).
    """
    con = _get_connection()
    con.execute("""
        INSERT OR REPLACE INTO alerts_live (
            event_id, event_time, fire_lat, fire_lon,
            temperature, wind_speed_mph, wind_direction_deg, humidity_percent,
            building_name, building_type, building_geom, ingestion_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        alert.get("event_id"),
        alert.get("event_time"),
        alert.get("fire_lat"),
        alert.get("fire_lon"),
        alert.get("temperature"),
        alert.get("wind_speed_mph"),
        alert.get("wind_direction_deg"),
        alert.get("humidity_percent"),
        alert.get("building_name"),
        alert.get("building_type"),
        alert.get("building_geom"),
        datetime.now(timezone.utc).isoformat(),
    ])
    con.close()


def insert_alerts_batch(alerts: list[dict]):
    """Insert multiple alerts in a single transaction for better throughput."""
    if not alerts:
        return
    con = _get_connection()
    con.executemany("""
        INSERT OR REPLACE INTO alerts_live (
            event_id, event_time, fire_lat, fire_lon,
            temperature, wind_speed_mph, wind_direction_deg, humidity_percent,
            building_name, building_type, building_geom, ingestion_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            a.get("event_id"),
            a.get("event_time"),
            a.get("fire_lat"),
            a.get("fire_lon"),
            a.get("temperature"),
            a.get("wind_speed_mph"),
            a.get("wind_direction_deg"),
            a.get("humidity_percent"),
            a.get("building_name"),
            a.get("building_type"),
            a.get("building_geom"),
            datetime.now(timezone.utc).isoformat(),
        )
        for a in alerts
    ])
    con.close()


def get_latest_alerts(limit: int = 500) -> list[dict]:
    """Return the most recent alerts from the live table."""
    con = _get_connection(read_only=True)
    try:
        result = con.execute("""
            SELECT * FROM alerts_live
            ORDER BY event_time DESC
            LIMIT ?
        """, [limit]).fetchdf()
        con.close()
        return result.to_dict(orient="records")
    except duckdb.CatalogException:
        # Table doesn't exist yet — return empty
        con.close()
        return []


def get_alert_count() -> int:
    """Return total number of alerts in the live table."""
    con = _get_connection(read_only=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM alerts_live").fetchone()[0]
        con.close()
        return count
    except duckdb.CatalogException:
        con.close()
        return 0


def clear_alerts():
    """Delete all alerts from the live table. Useful for testing."""
    con = _get_connection()
    try:
        con.execute("DELETE FROM alerts_live")
    except duckdb.CatalogException:
        pass
    con.close()


def archive_old_alerts(days: int = 7) -> int:
    """
    Move alerts older than `days` into a dated Parquet file under storage/archive/.
    Returns the number of archived rows.
    """
    con = _get_connection()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    
    try:
        # Count rows to archive
        row_count = con.execute(
            "SELECT COUNT(*) FROM alerts_live WHERE ingestion_ts < ?", [cutoff]
        ).fetchone()[0]
        
        if row_count == 0:
            con.close()
            return 0
        
        # Export to Parquet
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y_%m_%d")
        archive_path = os.path.join(ARCHIVE_DIR, f"alerts_{date_str}.parquet")
        
        con.execute(f"""
            COPY (SELECT * FROM alerts_live WHERE ingestion_ts < '{cutoff}')
            TO '{archive_path}' (FORMAT PARQUET)
        """)
        
        # Delete archived rows
        con.execute("DELETE FROM alerts_live WHERE ingestion_ts < ?", [cutoff])
        con.close()
        
        return row_count
    except duckdb.CatalogException:
        con.close()
        return 0
