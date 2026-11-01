"""Managed Spark portability and correction-parity experiment.

Use with a pre-existing Spark session in Fabric / Databricks, or execute as
a Python script in local Spark. This module does not create cloud resources.
Results contain only synthetic aggregates and runtime metadata.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType,
)
from umig6_metering_quality import process_metering
from umig6_metering_quality.evidence import lane_manifest

OUTPUTS = ("current", "rejected", "superseded", "changes")
EXPECTED = {"current": 2, "rejected": 3, "superseded": 2, "changes": 1}


def fixture(spark):
    """Eight rows exercising redelivery, late revision, latest tie, bad energy."""
    schema = StructType([
        StructField("ean", StringType()),
        StructField("meter_id", StringType()),
        StructField("register", StringType()),
        StructField("interval_start_utc", TimestampType()),
        StructField("interval_end_utc", TimestampType()),
        StructField("energy_kwh", DecimalType(18, 6)),
        StructField("revision_at_utc", TimestampType()),
        StructField("measurement_method", StringType()),
    ])
    anchor = datetime(2024, 10, 27, 0, 0)
    older = datetime(2024, 11, 1)
    newer = older + timedelta(days=1)

    def row(point, quarter, kwh, revision):
        start = anchor + timedelta(minutes=15 * quarter)
        return ("54" + str(point).zfill(16), "SYNTH-METER-" + str(point),
                "A+", start, start + timedelta(minutes=15),
                __import__("decimal").Decimal(kwh), revision, "MEASURED")

    source = [
        row(0, 0, "0.250000", older),
        row(0, 0, "0.250000", older),  # identical redelivery
        row(0, 0, "0.350000", newer),  # legitimate correction
        row(1, 1, "0.500000", older),
        row(1, 1, "0.600000", newer),  # contradictory tied revisions
        row(1, 1, "0.700000", newer),
        row(2, 2, "-0.100000", older), # reject negative volume
        row(3, 3, "0.200000", older),
    ]
    return spark.createDataFrame(source, schema)


def run(spark, *, bucket_count=64, delta_root=None):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    started = time.perf_counter()
    with process_metering(fixture(spark), cache_shared=False) as output:
        lanes = {name: getattr(output, name) for name in OUTPUTS}
        manifests = {name: lane_manifest(lanes[name], buckets=bucket_count)
                     for name in OUTPUTS}
        counts = {name: manifests[name]["rows"] for name in OUTPUTS}
        if counts != EXPECTED:
            raise AssertionError("Correction semantics changed: expected "
                                 + str(EXPECTED) + ", observed " + str(counts))
        delta = None
        if delta_root:
            # A caller MUST deliberately choose a disposable, owned path.
            # Deliberately no automated deletion or blanket cleanup.
            if "umig6-evidence-" not in delta_root or delta_root.rstrip("/") in ("/", ""):
                raise ValueError("Use an isolated owned path containing 'umig6-evidence-'")
            delta = {}
            for name in OUTPUTS:
                path = delta_root.rstrip("/") + "/" + name
                lanes[name].write.format("delta").mode("overwrite").save(path)
                observed = lane_manifest(spark.read.format("delta").load(path), buckets=bucket_count)
                if observed != manifests[name]:
                    raise AssertionError("Delta round-trip failed for " + name)
                delta[name] = {"round_trip": "matched", "path_redacted": True}
            # Repeat writes to test deterministic overwrite, NOT multi-table
            # atomicity. Never mark a batch published without a separate gate.
            for name in OUTPUTS:
                path = delta_root.rstrip("/") + "/" + name
                lanes[name].write.format("delta").mode("overwrite").save(path)
                observed = lane_manifest(spark.read.format("delta").load(path), buckets=bucket_count)
                if observed != manifests[name]:
                    raise AssertionError("Delta retry changed " + name)
                delta[name]["retry"] = "matched"
    output = {
        "format": "umig6-cross-runtime-fixture-v1",
        "dataset": "SYNTHETIC_ONLY",
        "input_rows": 8,
        "expected": EXPECTED,
        "counts": counts,
        "lanes": manifests,
        "delta": delta,
        "runtime": {
            "spark_version": spark.version,
            "python_version": platform.python_version(),
            "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
            "environment": os.environ.get("UMIG6_RUNTIME", "unclassified"),
        },
        "elapsed_seconds_including_actions": round(time.perf_counter() - started, 3),
    }
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="umig6-evidence.json")
    parser.add_argument("--delta-root", help="Owned isolated disposable Delta path")
    args = parser.parse_args()
    active = globals().get("spark") or SparkSession.builder.appName("umig6-parity").getOrCreate()
    result = run(active, delta_root=args.delta_root)
    with open(args.output, "w", encoding="utf-8") as stream:
        json.dump(result, stream, sort_keys=True, indent=2)
    print("UMIG6_RESULT=" + json.dumps({
        "format": result["format"], "counts": result["counts"],
        "spark_version": result["runtime"]["spark_version"],
        "fingerprints": {k: v["fingerprint_sha256"] for k,v in result["lanes"].items()},
    }, sort_keys=True))


if __name__ == "__main__":
    main()
