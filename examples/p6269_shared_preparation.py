"""One-source-scan reuse for original P6269 quality and coverage workloads.

The baseline scans and checks a CSV separately for every action. The
alternative prepares a minimal set of checked Spark-native columns *once*,
persists that projection, then reuses it across three exact aggregates.

Unlike a misleading 'warm cache' benchmark, setup/materialization time is
included, including eager Databricks serverless snapshot creation. Unpersist
one category before starting the next; never cache all 84.3M rows at once.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import time

from umig6_metering_quality.public_profiles import (
    read_public_profiles, screen_public_profiles, PublicProfilesResult,
    public_profile_metrics, profile_coverage,
)
from umig6_metering_quality.local_clock import profile_local_clock_coverage
from pyspark.sql import functions as F

NEEDED = ("profile_id", "quality_errors", "interval_start_utc",
          "Datum", "Datum_Startuur", "import_kwh", "injection_kwh")


def compact_checked(source):
    """Keep exactly the columns consumed by the three acceptance checks."""
    checked = screen_public_profiles(source)
    minimized = checked.all_checked.select(*NEEDED)
    return PublicProfilesResult(valid=minimized.where(F.size("quality_errors") == 0),
                                quarantine=minimized.where(F.size("quality_errors") > 0),
                                all_checked=minimized)


def aggregate(checked):
    """Execute the same three independent checks, returning aggregate-only proof."""
    times = {}
    start = time.perf_counter()
    quality = public_profile_metrics(checked).first().asDict()
    times["row_screen_seconds"] = round(time.perf_counter() - start, 3)
    start = time.perf_counter()
    strict = profile_coverage(checked).agg(
        F.count("*").alias("profiles"),
        F.sum(F.when(F.col("coverage_status") == "INCOMPLETE", 1)
              .otherwise(0)).alias("incomplete"),
    ).first().asDict()
    times["utc_coverage_seconds"] = round(time.perf_counter() - start, 3)
    start = time.perf_counter()
    local = profile_local_clock_coverage(checked).agg(
        F.count("*").alias("profiles"),
        F.sum(F.when(F.col("local_clock_coverage_status") == "COMPLETE_LOCAL_CLOCK", 1)
              .otherwise(0)).alias("complete"),
        F.sum("invalid_clock_slots").alias("invalid_slots"),
    ).first().asDict()
    times["local_coverage_seconds"] = round(time.perf_counter() - start, 3)
    # Spark Decimal values become strings for a deterministic JSON contract.
    canonical = {
        "quality": {k: str(v) if v is not None else None for k,v in quality.items()},
        "strict": {k: str(v) if v is not None else None for k,v in strict.items()},
        "local": {k: str(v) if v is not None else None for k,v in local.items()},
    }
    return canonical, times


def compare(spark, csv_path):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    baseline = compact_checked(read_public_profiles(spark, csv_path))
    reference, ref_times = aggregate(baseline)
    cached = compact_checked(read_public_profiles(spark, csv_path))
    began = time.perf_counter()
    # Supported by Fabric classic Spark; Databricks serverless requires
    # the 2026 DataFrame caching preview. If unavailable, fail explicitly.
    stored = cached.all_checked.cache()
    checked = PublicProfilesResult(valid=stored.where(F.size("quality_errors") == 0),
                                   quarantine=stored.where(F.size("quality_errors") > 0),
                                   all_checked=stored)
    created_seconds = round(time.perf_counter() - began, 3)
    try:
        actual, times = aggregate(checked)
    finally:
        stored.unpersist()
    if actual != reference:
        raise AssertionError("Caching changed quality or profile-clock aggregate")
    baseline_seconds = round(sum(ref_times.values()), 3)
    cached_total = round(created_seconds + sum(times.values()), 3)
    return {
        "format": "umig6-p6269-shared-preparation-v1",
        "source": "one_original_category_csv",
        "spark_version": spark.version,
        "result_equal": True,
        "category_rows": int(reference["quality"]["input_rows"]),
        "cold_scan_three_actions_seconds": baseline_seconds,
        "cached_setup_seconds_included": created_seconds,
        "cached_three_actions_seconds": round(sum(times.values()), 3),
        "cache_total_seconds": cached_total,
        "relative_speedup_for_this_single_execution": round(
            baseline_seconds / cached_total, 3) if cached_total > 0 else None,
        "timings": {"baseline": ref_times, "cache_actions": times},
        "caveat": "order, warmup, preview and capacity confound one run; verify repetitions and billable cost",
    }
