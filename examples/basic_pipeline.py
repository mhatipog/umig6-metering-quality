"""Example job: canonical metering data -> verified outputs.

Run using an existing Spark cluster or local Spark installation. All inputs
are synthetic in this demo; no CMS endpoint, production data or secrets.
"""

from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType
from umig6_metering_quality import process_metering

spark = SparkSession.builder.appName("metering-quality-example").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

schema = StructType([
    StructField("ean", StringType()), StructField("meter_id", StringType()),
    StructField("register", StringType()),
    StructField("interval_start_utc", TimestampType()),
    StructField("interval_end_utc", TimestampType()),
    StructField("energy_kwh", DecimalType(18, 6)),
    StructField("revision_at_utc", TimestampType()),
    StructField("measurement_method", StringType()),
])
start = datetime(2026, 1, 2)
end = start + timedelta(minutes=15)
rows = [
    ("541440000000000000", "DEMO-1", "A+", start, end, Decimal("0.400000"), datetime(2026, 1, 3), "ESTIMATED"),
    ("541440000000000000", "DEMO-1", "A+", start, end, Decimal("0.350000"), datetime(2026, 1, 4), "MEASURED"),
]
bronze = spark.createDataFrame(rows, schema)
result = process_metering(bronze)

for name in ("current", "rejected", "superseded", "changes"):
    df = getattr(result, name)
    print(f"\n{name}: {df.count()} rows")
    df.show(truncate=False)

# For real pipelines, persist these as separate tables/snapshots. Do not append
# `current` blindly across batches: use an explicit idempotent MERGE or rebuild.
spark.stop()
