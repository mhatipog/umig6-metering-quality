"""Partition-size sensitivity for the complete public Fluvius P6269 checks.

A configuration is eligible for ranking only if *every* row-screen, strict
UTC-label and local-clock aggregate matches the reference. No customer
records, profile identifiers or raw CSV paths leave the engine.

Experimental unit: four original category CSVs in one official archive, or
eight for the complete dataset. Timings are Spark actions, not wall-clock
cost or cold-start measurements. Use independent runs to assess warmup.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
import os
import time

from cloud_fluvius import run as check_public

SUITE = "umig6-p6269-read-partition-sensitivity-v1"
EXPECTED_ROWS_PER_CATEGORY = 10_540_800
INPUT_MIB = (64, 128, 256)
MEASUREMENTS = ("row_screen_seconds", "utc_label_coverage", "local_clock_coverage")


def semantic_signature(result):
    """Exclude runtime and timing; require all substantive aggregates equal."""
    fields = []
    for item in result["categories"]:
        fields.append({
            "row_metrics": item["metrics"],
            "utc_labels": {k: v for k, v in item["utc_label_coverage"].items()
                           if k != "seconds"},
            "local_clock": {k: v for k, v in item["local_clock_coverage"].items()
                            if k != "seconds"},
        })
    return fields


def stage_totals(report):
    return {
        "row_screen_seconds": round(sum(x["row_screen_seconds"] for x in report["categories"]), 3),
        "strict_utc_coverage_seconds": round(
            sum(x["utc_label_coverage"]["seconds"] for x in report["categories"]), 3),
        "local_clock_coverage_seconds": round(
            sum(x["local_clock_coverage"]["seconds"] for x in report["categories"]), 3),
    }


def aggregate_source(report):
    items = report["categories"]
    return {
        "rows": sum(int(z["metrics"]["input_rows"]) for z in items),
        "passed": sum(int(z["metrics"]["accepted_rows"]) for z in items),
        "quarantined": sum(int(z["metrics"]["quarantined_rows"]) for z in items),
        "profiles_local_complete": sum(z["local_clock_coverage"]["complete"] for z in items),
        "profiles_utc_labels_incomplete": sum(z["utc_label_coverage"]["incomplete"] for z in items),
        "import_kwh": str(sum((Decimal(z["metrics"]["accepted_import_kwh"])
                               for z in items), Decimal(0))),
        "injection_kwh": str(sum((Decimal(z["metrics"]["accepted_injection_kwh"])
                                  for z in items), Decimal(0))),
    }


def run_matrix(spark, paths, *, archive_index, sizes=INPUT_MIB, repeats=1):
    if archive_index not in (0, 1, "full"):
        raise ValueError("Unrecognized Fluvius 2024 source partition")
    expected_files = 8 if archive_index == "full" else 4
    if len(paths) != expected_files or len(set(paths)) != len(paths):
        raise ValueError("Require all four original category files per source ZIP (eight for full)")
    if repeats not in (1, 2, 3):
        raise ValueError("Use one to three measurements per setting")
    if not sizes or any(n not in INPUT_MIB for n in sizes) or len(set(sizes)) != len(sizes):
        raise ValueError("Unsupported or duplicate maxPartitionBytes setting")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rows_expected = len(paths) * EXPECTED_ROWS_PER_CATEGORY
    run = {
        "format": SUITE, "source": "OFFICIAL_FLUVIUS_P6269_ORIGINAL_2024",
        "archive_index": archive_index, "source_files": len(paths),
        "spark_version": spark.version,
        "engine_label": os.environ.get("UMIG6_RUNTIME", "unclassified"),
        "configuration_scope": "spark.sql.files.maxPartitionBytes",
        "source_archive_sha256_verified_before_run": True,
        "repetitions_requested": repeats, "runs": [],
        "workload": "row-screen + strict UTC-label coverage + exact local-clock coverage",
        "cold_start_included": False,
        "source_bytes_staged_in_managed_storage": True,
    }
    original = spark.conf.get("spark.sql.files.maxPartitionBytes", "134217728")
    baseline = None
    baseline_values = None
    try:
        # Fixed order; repetitions guard against treating a warm result as
        # a general optimizer win. Do not turn ratios into pricing claims.
        for repetition in range(repeats):
            for mib in sizes:
                bytes_per_split = mib * 1024 * 1024
                spark.conf.set("spark.sql.files.maxPartitionBytes", str(bytes_per_split))
                effective = spark.conf.get("spark.sql.files.maxPartitionBytes")
                if str(effective) not in (str(bytes_per_split), str(mib) + "m"):
                    raise ValueError("Engine did not confirm requested partition setting")
                started = time.perf_counter()
                checked = check_public(spark, paths, coverage=True, full_corpus=(archive_index == "full"))
                measured = stage_totals(checked)
                totals = aggregate_source(checked)
                if (totals["rows"] != rows_expected or totals["passed"] != rows_expected
                        or totals["quarantined"] != 0
                        or totals["profiles_local_complete"] != 300 * len(paths)
                        or totals["profiles_utc_labels_incomplete"] != 300 * len(paths)):
                    raise AssertionError("Measured original source differs from expected dataset")
                semantic = semantic_signature(checked)
                if baseline is None:
                    baseline, baseline_values = semantic, totals
                if semantic != baseline or totals != baseline_values:
                    raise AssertionError("Read tuning modified measured source semantics")
                run["runs"].append({
                    "repetition": repetition + 1,
                    "file_partition_mib": mib,
                    "setting_effective": str(effective),
                    "source_rows": totals["rows"],
                    "quality_and_coverage_invariants_match": True,
                    "workload_action_seconds": measured,
                    "sum_three_action_groups_seconds": round(sum(measured.values()), 3),
                    "total_invocation_seconds": round(time.perf_counter() - started, 3),
                })
    finally:
        spark.conf.set("spark.sql.files.maxPartitionBytes", original)
    run["aggregate_evidence"] = baseline_values
    # Include an aggregate digest, not underlying profile rows or storage URLs.
    run["invariant_sha256"] = hashlib.sha256(
        json.dumps(baseline, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    run["verified"] = (len(run["runs"]) == repeats * len(sizes)
                       and all(x["quality_and_coverage_invariants_match"] for x in run["runs"]))
    return run


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, nargs="+")
    p.add_argument("--archive", required=True, choices=("0", "1", "full"))
    p.add_argument("--sizes", nargs="+", type=int, default=list(INPUT_MIB))
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--output", default="p6269-tuning.json")
    args = p.parse_args()
    from pyspark.sql import SparkSession
    active = globals().get("spark") or SparkSession.builder.appName("umig6-p6269-tune").getOrCreate()
    result = run_matrix(active, args.csv, archive_index=(
        args.archive if args.archive == "full" else int(args.archive)),
        sizes=tuple(args.sizes), repeats=args.repeats)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    print(json.dumps({"verified": result["verified"],
                      "runs": result["runs"], "invariant_sha256": result["invariant_sha256"]},
                     sort_keys=True))


if __name__ == "__main__":
    main()
