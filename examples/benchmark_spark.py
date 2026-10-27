"""Reproducible Spark workload benchmark; does not imply MIG6 certification.

Two modes: readiness profiling (no shuffle of measurement history) and the
full correction-aware pipeline (shuffle/window workloads). All data synthetic.
No Python UDF, per-record Python loops or driver-side meter records.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
from umig6_metering_quality import (evaluate_readiness, process_metering,
                                    screen_source_provenance, screen_metering_rows)
from umig6_metering_quality.rulebook import screen_continuous_measures


def sample_metering(spark, rows: int, partitions: int, revisions_every: int = 0):
    """Synthetic uniquely keyed quarter-hour records, distributed in Spark."""
    base = spark.range(rows, numPartitions=partitions)
    # At 4096 supply points, id / 4096 gives a natural per-point quarter index.
    # All EANs and physical meter identifiers here are synthetic and unallocated.
    point = F.pmod(F.col("id"), F.lit(4096))
    sequence = F.floor(F.col("id") / F.lit(4096)).cast("long")
    if revisions_every:
        # One revised quarter every N source slots, carrying a later
        # source-authored revision timestamp and changed kWh.
        revised = (sequence > 0) & ((sequence % F.lit(revisions_every)) == 0)
        interval_index = sequence - F.floor(sequence / F.lit(revisions_every)).cast("long")
        source_revision = F.when(revised, F.lit("2026-01-02 00:00:00")).otherwise(
            F.lit("2026-01-01 00:00:00"))
        source_kwh = F.when(revised, F.lit(0.30)).otherwise(F.lit(0.25))
    else:
        interval_index = sequence
        source_revision = F.lit("2026-01-01 00:00:00")
        source_kwh = F.lit(0.25)
    start = F.timestamp_seconds(F.lit(1704067200) + interval_index * F.lit(900))
    return (base.select(
        F.concat(F.lit("54"), F.lpad(point.cast("string"), 16, "0")).alias("ean"),
        F.concat(F.lit("TEST-METER-"), point.cast("string")).alias("meter_id"),
        F.lit("A+").alias("register"),
        start.alias("interval_start_utc"),
        (start + F.expr("INTERVAL 15 MINUTES")).alias("interval_end_utc"),
        source_kwh.cast("decimal(18,6)").alias("energy_kwh"),
        F.to_timestamp(source_revision).alias("revision_at_utc"),
        F.lit("MEASURED").alias("measurement_method"),
    ))


def run(spark, rows: int, partitions: int, workload: str, revisions_every: int = 0):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = sample_metering(spark, rows, partitions, revisions_every=revisions_every).persist(StorageLevel.MEMORY_AND_DISK)
    t0 = time.perf_counter()
    actual = df.count()  # Materialise source before timing the processing stage.
    loading_seconds = time.perf_counter() - t0
    if actual != rows:
        raise RuntimeError(f"Materialized {actual} source rows, expected {rows}")
    t0 = time.perf_counter()
    if workload == "readiness":
        result = evaluate_readiness(df, "METERING_REPLAY")
        metric = result.summary.first()
        accepted, rejected = metric.rows_ready, metric.rows_blocked
        plan = result.summary._jdf.queryExecution().optimizedPlan().toString()
    elif workload == "rulebook":
        result = screen_continuous_measures(df)
        metric = result.summary.first()
        accepted, rejected = metric.passed + metric.incomplete, metric.failed
        plan = result.summary._jdf.queryExecution().optimizedPlan().toString()
    elif workload == "bronze":
        result = screen_metering_rows(df)
        metric = result.summary.first()
        accepted, rejected = metric.accepted_rows, metric.quarantined_rows
        plan = result.summary._jdf.queryExecution().optimizedPlan().toString()
    elif workload == "provenance":
        evidenced = (df
            .withColumn("source_system", F.lit("SYNTHETIC_MDM"))
            .withColumn("source_record_id", F.concat_ws(":",
                F.col("ean"), F.col("meter_id"),
                F.date_format("interval_start_utc", "yyyy-MM-dd HH:mm:ss")))
            .withColumn("source_evidence_ref", F.lit("synthetic-source-contract-v1"))
            .withColumn("timestamp_semantics", F.lit("UTC_VERIFIED"))
            .withColumn("revision_basis", F.lit("SOURCE_REVISION")))
        result = screen_source_provenance(evidenced)
        metric = result.summary.first()
        accepted, rejected = metric.analytics_ready_rows, metric.review_rows + metric.quarantine_rows
        plan = result.summary._jdf.queryExecution().optimizedPlan().toString()
    elif workload == "corrections":
        result = process_metering(df)
        # Legacy benchmark: only materialises the current and rejected lanes.
        accepted, rejected = result.current.count(), result.rejected.count()
        plan = result.current._jdf.queryExecution().optimizedPlan().toString()
    elif workload in ("corrections-full", "corrections-full-cached",
                      "corrections-full-cached-evidenced"):
        cached = workload != "corrections-full"
        # The evidenced mode measures the additional declared-provenance
        # predicate, NOT an independent check of an upstream source contract.
        input_df = df
        if workload == "corrections-full-cached-evidenced":
            input_df = (df.withColumn("timestamp_semantics", F.lit("UTC_VERIFIED"))
                       .withColumn("revision_basis", F.lit("SOURCE_REVISION")))
        # All four sinks, including explicitly releasing cached history.
        with process_metering(input_df, cache_shared=cached) as result:
            accepted, rejected = result.current.count(), result.rejected.count()
            superseded, changed = result.superseded.count(), result.changes.count()
            plan = result.current._jdf.queryExecution().optimizedPlan().toString()
    else:
        raise ValueError(workload)
    seconds = time.perf_counter() - t0
    out = {
        "benchmark": "SYNTHETIC_NOT_FLUVIUS", "workload": workload,
        "spark_version": spark.version,
        "rows": actual, "partitions": partitions, "revisions_every": revisions_every,
        "source_materialization_seconds": round(loading_seconds, 3),
        "processing_seconds": round(seconds, 3),
        "rows_per_second": round(actual / seconds) if seconds else None,
        "accepted": int(accepted), "rejected": int(rejected),
        "optimized_plan_sha256": hashlib.sha256(plan.encode()).hexdigest(),
        "spark_adaptive_execution": spark.conf.get("spark.sql.adaptive.enabled"),
        "environment": os.environ.get("BENCH_ENVIRONMENT", "unspecified"),
    }
    if workload in ("corrections-full", "corrections-full-cached", "corrections-full-cached-evidenced"):
        out["superseded"] = int(superseded)
        out["changed"] = int(changed)
        out["materialized_outputs"] = ["current", "rejected", "superseded", "changes"]
    elif workload == "corrections":
        out["materialized_outputs"] = ["current", "rejected"]
    if workload == "corrections-full-cached-evidenced":
        out["scope"] = "DECLARED_PROVENANCE_GUARD_ONLY_NOT_EXTERNAL_EVIDENCE_AUDIT"
    df.unpersist()
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--revisions-every", type=int, default=0,
                        help="Every Nth observation per point is a later changed revision; 0 means unique")
    parser.add_argument("--shuffle-partitions", type=int, help="Optional explicit benchmark shuffle partition count")
    parser.add_argument("--workload", choices=("readiness", "rulebook", "bronze", "provenance", "corrections", "corrections-full", "corrections-full-cached", "corrections-full-cached-evidenced", "all"), default="all")
    parser.add_argument("--output-json", help="Write structured metrics to a JSON file")
    args = parser.parse_args()
    if args.revisions_every < 0 or args.revisions_every == 1:
        parser.error("--revisions-every must be 0 or >= 2")
    if args.rows <= 0 or args.partitions <= 0:
        parser.error("--rows and --partitions must be positive")
    if args.shuffle_partitions is not None and args.shuffle_partitions <= 0:
        parser.error("--shuffle-partitions must be positive")
    builder = SparkSession.builder.appName("umig6-metering-benchmark")
    if args.shuffle_partitions:
        builder = builder.config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    spark = builder.getOrCreate()
    try:
        workloads = ("readiness", "rulebook", "bronze", "provenance", "corrections") if args.workload == "all" else (args.workload,)
        records = [run(spark, args.rows, args.partitions, work, revisions_every=args.revisions_every) for work in workloads]
        for record in records:
            print(json.dumps(record, sort_keys=True))
        if args.output_json:
            with open(args.output_json, "w", encoding="utf-8") as stream:
                json.dump(records, stream, indent=2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
