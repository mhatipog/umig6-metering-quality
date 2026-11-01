"""Read caller-staged official Fluvius P6269 category CSVs with managed Spark.

This does not download archives on the driver or assume a local filesystem.
The caller must verify source archive checksums before staging. Public
profiles are NOT official UMIG6 messages or trusted UTC-instant evidence.
"""
import argparse
import hashlib
import json
import os
import time
from pyspark.sql import SparkSession, functions as F
from umig6_metering_quality.public_profiles import (
    DATASET_ID, read_public_profiles, screen_public_profiles,
    public_profile_metrics, profile_coverage,
)
from umig6_metering_quality.local_clock import profile_local_clock_coverage


def run(spark, paths, *, coverage=False, full_corpus=False):
    if not paths or len(paths) > 8 or len(set(paths)) != len(paths):
        raise ValueError("Supply 1-8 distinct original P6269 category CSV paths")
    if full_corpus and len(paths) != 8:
        raise ValueError("Full-corpus benchmark requires eight CSV inputs")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    categories = []
    for path in paths:
        started = time.perf_counter()
        checked = screen_public_profiles(read_public_profiles(spark, path))
        metrics = public_profile_metrics(checked).first().asDict()
        result = {
            "source_path_sha256": hashlib.sha256(path.encode("utf-8")).hexdigest(),
            "metrics": {k: str(v) if v is not None else None for k, v in metrics.items()},
            "row_screen_seconds": round(time.perf_counter() - started, 3),
        }
        if coverage:
            started = time.perf_counter()
            utc = profile_coverage(checked).agg(
                F.count("*").alias("profiles"),
                F.sum(F.when(F.col("coverage_status") == "INCOMPLETE", 1).otherwise(0)).alias("incomplete"),
            ).first()
            result["utc_label_coverage"] = {
                "profiles": int(utc["profiles"]),
                "incomplete": int(utc["incomplete"] or 0),
                "seconds": round(time.perf_counter() - started, 3),
                "utc_instant_mapping_verified": False,
            }
            started = time.perf_counter()
            local = profile_local_clock_coverage(checked).agg(
                F.count("*").alias("profiles"),
                F.sum(F.when(F.col("local_clock_coverage_status") == "COMPLETE_LOCAL_CLOCK", 1).otherwise(0)).alias("complete"),
                F.sum("invalid_clock_slots").alias("invalid_slots"),
            ).first()
            result["local_clock_coverage"] = {
                "profiles": int(local["profiles"]),
                "complete": int(local["complete"] or 0),
                "invalid_slots": int(local["invalid_slots"] or 0),
                "seconds": round(time.perf_counter() - started, 3),
            }
        categories.append(result)
    total = sum(int(c["metrics"]["input_rows"]) for c in categories)
    complete = len(categories) == 8 and total == 84_326_400
    if full_corpus and not complete:
        raise AssertionError("Expected eight original CSVs totalling 84,326,400 rows")
    if full_corpus and coverage and (
        sum(c["local_clock_coverage"]["complete"] for c in categories) != 2400
        or sum(c["utc_label_coverage"]["incomplete"] for c in categories) != 2400
    ):
        raise AssertionError("Fluvius source clock semantics changed from pinned baseline")
    return {
        "format": "umig6-managed-spark-p6269-v1",
        "dataset": DATASET_ID, "source_files": len(categories),
        "total_rows": total, "full_corpus_count_verified": complete,
        "archive_checksum_verified_by_runner": False,
        "categories": categories, "spark_version": spark.version,
        "runtime": os.getenv("UMIG6_RUNTIME", "unclassified"),
        "caveat": "P6269 is not a MIG6 source revision feed; apparent Z offsets are unverified",
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", nargs="+", required=True)
    p.add_argument("--coverage", action="store_true")
    p.add_argument("--full-corpus", action="store_true")
    p.add_argument("--output", default="p6269-managed.json")
    args = p.parse_args()
    active = globals().get("spark") or SparkSession.builder.appName("umig6-public-profiles").getOrCreate()
    report = run(active, args.csv, coverage=args.coverage, full_corpus=args.full_corpus)
    with open(args.output, "w", encoding="utf-8") as stream:
        json.dump(report, stream, sort_keys=True, indent=2)
    print("UMIG6_FLUVIUS=" + json.dumps({
        "total_rows": report["total_rows"], "source_files": report["source_files"],
        "full_corpus_count_verified": report["full_corpus_count_verified"]}, sort_keys=True))


if __name__ == "__main__":
    main()
