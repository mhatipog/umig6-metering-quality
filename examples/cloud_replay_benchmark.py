"""Four-output synthetic correction benchmark for existing Spark sessions."""
import argparse
import json
import os
import time
from pyspark.sql import SparkSession
from umig6_metering_quality import process_metering
from umig6_metering_quality.evidence import lane_manifest
from benchmark_spark import sample_metering


def run(spark, *, rows=100_000, partitions=8, revisions_every=10,
        cache_shared=False, shuffle_partitions=None, buckets=64):
    if rows < 100 or partitions < 1 or revisions_every < 2:
        raise ValueError("Need rows >= 100, partitions >= 1, revisions_every >= 2")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if shuffle_partitions is not None:
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    source = sample_metering(spark, rows, partitions, revisions_every)
    start = time.perf_counter()
    count = source.count()
    load_seconds = time.perf_counter() - start
    if count != rows:
        raise AssertionError("Unexpected synthetic source count")
    start = time.perf_counter()
    with process_metering(source, cache_shared=cache_shared) as output:
        lanes = {name: lane_manifest(getattr(output, name), buckets=buckets)
                 for name in ("current", "rejected", "superseded", "changes")}
    processing_seconds = time.perf_counter() - start
    counts = {name: manifest["rows"] for name, manifest in lanes.items()}
    if counts["current"] + counts["superseded"] + counts["rejected"] > rows:
        raise AssertionError("Output reconciliation exceeds input")
    if counts["changes"] != counts["superseded"]:
        raise AssertionError("Unexpected synthetic revision history")
    return {
        "format": "umig6-four-lane-synthetic-benchmark-v1",
        "input": {"synthetic": True, "rows": rows, "partitions": partitions,
                  "revisions_every": revisions_every},
        "config": {"cache_shared": cache_shared, "shuffle_partitions": shuffle_partitions,
                   "session_timezone": spark.conf.get("spark.sql.session.timeZone")},
        "engine": {"spark_version": spark.version,
                   "runtime": os.getenv("UMIG6_RUNTIME", "unclassified")},
        "source_count_seconds": round(load_seconds, 3),
        "four_lane_processing_seconds": round(processing_seconds, 3),
        "counts": counts, "manifests": lanes,
        "timing_scope": "four Spark aggregate actions; excludes Delta persistence, network, cluster startup",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--revisions-every", type=int, default=10)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--cache-shared", action="store_true")
    parser.add_argument("--output", default="cloud-replay.json")
    args = parser.parse_args()
    active = globals().get("spark") or SparkSession.builder.appName("umig6-replay-benchmark").getOrCreate()
    result = run(active, rows=args.rows, partitions=args.partitions,
                 revisions_every=args.revisions_every, cache_shared=args.cache_shared,
                 shuffle_partitions=args.shuffle_partitions)
    with open(args.output, "w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
    print("UMIG6_BENCHMARK=" + json.dumps({
        "counts": result["counts"], "seconds": result["four_lane_processing_seconds"],
        "spark_version": result["engine"]["spark_version"]}, sort_keys=True))


if __name__ == "__main__":
    main()
