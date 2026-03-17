"""
Alert Retention Manager — Archives old alerts to Parquet cold storage.

Can be run standalone (cron/manual) or called from the consumer on startup.

Usage:
    python alert_sink/retention.py [--days 7]
"""

import os
import sys
import argparse

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from alert_sink.duckdb_store import init_db, archive_old_alerts, get_alert_count


def main():
    parser = argparse.ArgumentParser(description="Archive old alerts to Parquet cold storage")
    parser.add_argument("--days", type=int, default=7,
                        help="Archive alerts older than N days (default: 7)")
    args = parser.parse_args()

    init_db()

    count_before = get_alert_count()
    print(f"[Retention] Live alerts before: {count_before}")

    archived = archive_old_alerts(days=args.days)

    if archived > 0:
        count_after = get_alert_count()
        print(f"[Retention] Archived {archived} alerts (older than {args.days} days)")
        print(f"[Retention] Live alerts after: {count_after}")
    else:
        print(f"[Retention] No alerts older than {args.days} days found. Nothing to archive.")


if __name__ == "__main__":
    main()
