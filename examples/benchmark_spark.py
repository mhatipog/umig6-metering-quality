"""Repeatable synthetic Spark benchmark (NOT an Azure Databricks result).

Run on an actual cluster and record run metadata + explain plans. As it is
synthetic data, this establishes throughput characteristics only, not accuracy
or commercial MIG6 performance guarantees.
"""
from __future__ import annotations

import argparse
import time
from pyspark.sql import SparkSession, functions as F
from umig6_metering_quality import evaluate_readiness


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=16)
    args = parser.parse_args()
    if args.rows <= 0 or args.partitions <= 0:
        parser.error("--rows and --partitions must be positive")
    spark = SparkSession.builder.appName("umig6-readiness-benchmark").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    start = F.to_timestamp(F.lit("2026-01-01 00:00:00"))
    df = (spark.range(args.rows, numPartitions=args.partitions)
          .select(F.lit("541234567890123456").alias("ean"),
                  F.concat(F.lit("M-"), (F.col("id") % 10000).cast("string")).alias("meter_id"),
                  F.lit("A+").alias("register"),
                  F.expr("timestampadd(MINUTE, cast(id % 35040 as int) * 15, timestamp'2026-01-01 00:00:00')").alias("interval_start_utc"),
                  F.lit(0.25).cast("decimal(18,6)").alias("energy_kwh"))
          .withColumn("interval_end_utc", F.expr("interval_start_utc + INTERVAL 15 MINUTES")))
    t0 = time.perf_counter()
    result = evaluate_readiness(df, "METERING_REPLAY")
    metrics = result.summary.collect()[0]
    elapsed = time.perf_counter() - t0
    print({"spark": spark.version, "rows": args.rows, "partitions": args.partitions,
           "seconds": round(elapsed, 3), "rows_per_second": round(args.rows / elapsed),
           "ready": metrics.rows_ready, "blocked": metrics.rows_blocked})
    spark.stop()


if __name__ == "__main__":
    main()
