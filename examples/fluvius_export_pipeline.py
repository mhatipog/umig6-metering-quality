"""Offline public-data example: two informative Mijn Fluvius CSV snapshots."""

import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from umig6_metering_quality import read_fluvius_csv, process_metering


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: spark-submit examples/fluvius_export_pipeline.py older.csv newer.csv")
    spark = (SparkSession.builder.appName("fluvius-csv-quality-example")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    # Supply real export timestamps in production; these dates are illustrative.
    old = read_fluvius_csv(spark, sys.argv[1], snapshot_at_utc=datetime(2026, 1, 2, tzinfo=timezone.utc))
    new = read_fluvius_csv(spark, sys.argv[2], snapshot_at_utc=datetime(2026, 2, 2, tzinfo=timezone.utc))
    result = process_metering(old.readings.unionByName(new.readings))
    print("Adapter rejected rows:", old.rejected.count() + new.rejected.count())
    print("Core rejected rows:", result.rejected.count())
    print("Current records:", result.current.count())
    print("Changed intervals:", result.changes.count())
    # No EANs or meter IDs written to public logs in this example.
    spark.stop()


if __name__ == "__main__":
    main()
