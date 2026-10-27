#!/usr/bin/env python3
"""Benchmark the COMPLETE official Fluvius P6269 2024 corpus, one CSV at a time.

Sources are the two ZIP attachments from the public dataset's own metadata.
Archive data is used only locally for the benchmark; never committed or uploaded.
Output contains aggregate counts, durations and checksums but no meter profiles.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import urllib.request
import zipfile

from pyspark.sql import SparkSession
from umig6_metering_quality.public_profiles import (
    DATASET_ID, read_public_profiles, screen_public_profiles, public_profile_metrics, profile_coverage,
)

BASE = "https://opendata.fluvius.be"
HEADERS = {"User-Agent": "umig6-metering-quality-public-benchmark/0.5"}


def dataset_attachments():
    url = f"{BASE}/api/explore/v2.1/catalog/datasets/{DATASET_ID}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=35) as response:
        metadata = json.load(response)
    archives = [x for x in metadata.get("attachments", []) if x.get("mimetype") == "application/zip"]
    if len(archives) != 2:
        raise RuntimeError(f"Expected exactly two official ZIP attachments; found {len(archives)}")
    for x in archives:
        if not x.get("url", "").startswith(BASE + "/api/explore/v2.1/"):
            raise RuntimeError("Unexpected attachment URL: official endpoint required")
    return archives


def download(attachment, dest: Path):
    h = hashlib.sha256()
    size = 0
    with urllib.request.urlopen(urllib.request.Request(attachment["url"], headers=HEADERS), timeout=90) as src, dest.open("wb") as target:
        while True:
            block = src.read(2 * 1024 * 1024)
            if not block: break
            size += len(block)
            if size > 1_600_000_000: raise RuntimeError("Archive exceeded 1.6GB safety limit")
            h.update(block)
            target.write(block)
    return {"archive": attachment["title"], "bytes": size, "sha256": h.hexdigest()}


def benchmark(spark, output_path: str, *, max_files: int = 8, check_coverage: bool = False):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    results = []
    archive_checksums = []
    with tempfile.TemporaryDirectory(prefix="fluvius-p6269-") as temporary:
        home = Path(temporary)
        for index, attachment in enumerate(dataset_attachments()):
            zip_path = home / f"archive-{index}.zip"
            print("Downloading official archive", attachment["title"], flush=True)
            details = download(attachment, zip_path)
            archive_checksums.append(details)
            print("Verified downloaded bytes / sha256", details, flush=True)
            with zipfile.ZipFile(zip_path) as archive:
                members = sorted((m for m in archive.infolist() if m.filename.lower().endswith(".csv")), key=lambda m: m.filename)
                if len(members) != 4:
                    raise RuntimeError(f"Expected four category CSV files per archive, found {len(members)}")
                for member in members:
                    if len(results) >= max_files: break
                    if member.file_size > 1_000_000_000:
                        raise RuntimeError("CSV member exceeds safety cap")
                    # Stream only one CSV to disk at a time; no unsafe ZIP path extraction.
                    csv_path = home / "profile.csv"
                    print("Extracting category", member.filename, "bytes", member.file_size, flush=True)
                    with archive.open(member) as src, csv_path.open("wb") as dst:
                        shutil.copyfileobj(src, dst, length=2*1024*1024)
                    started = time.perf_counter()
                    checked = screen_public_profiles(read_public_profiles(spark, str(csv_path)))
                    stats = public_profile_metrics(checked).first().asDict()
                    elapsed = time.perf_counter() - started
                    row = {
                        "category_file": member.filename,
                        "source_size_bytes": member.file_size,
                        "rows": int(stats["input_rows"]),
                        "accepted": int(stats["accepted_rows"]),
                        "quarantined": int(stats["quarantined_rows"]),
                        "ambiguous_dst": int(stats["ambiguous_dst_rows"]),
                        "nonexistent_dst": int(stats["nonexistent_dst_rows"]),
                        "invalid_volumes": int(stats["invalid_volume_rows"]),
                        "accepted_import_kwh": str(stats["accepted_import_kwh"]),
                        "accepted_injection_kwh": str(stats["accepted_injection_kwh"]),
                        "seconds": round(elapsed, 3),
                        "rows_per_second": round(stats["input_rows"] / elapsed) if elapsed else None,
                    }
                    if check_coverage:
                        coverage_started = time.perf_counter()
                        # This second action is deliberately accounted for separately:
                        # it performs an exact per-profile distinct-interval check.
                        profiles = profile_coverage(checked)
                        from pyspark.sql import functions as F
                        coverage_summary = profiles.agg(
                            F.count("*").alias("profiles"),
                            F.sum(F.when(F.col("coverage_status") == "INCOMPLETE", 1).otherwise(0)).alias("incomplete_profiles"),
                            F.sum(F.col("observed_rows") - F.col("unique_utc_quarters")).alias("duplicate_quarters"),
                        ).first()
                        row["profile_coverage"] = {
                            "profiles": int(coverage_summary.profiles),
                            "incomplete_profiles": int(coverage_summary.incomplete_profiles),
                            "duplicate_quarters": int(coverage_summary.duplicate_quarters),
                            "seconds": round(time.perf_counter() - coverage_started, 3),
                        }
                    results.append(row)
                    print("RESULT", json.dumps(row, sort_keys=True), flush=True)
                    csv_path.unlink()
            zip_path.unlink()
            if len(results) >= max_files: break
    final = {
        "dataset": DATASET_ID,
        "scope": "COMPLETE_8_FILES" if len(results) == 8 else "PARTIAL",
        "spark_version": spark.version,
        "environment": os.environ.get("BENCH_ENVIRONMENT", "unknown"),
        "archive_checksums": archive_checksums,
        "categories": results,
        "total_rows": sum(x["rows"] for x in results),
        "total_accepted": sum(x["accepted"] for x in results),
        "total_quarantined": sum(x["quarantined"] for x in results),
        "total_processing_seconds": round(sum(x["seconds"] for x in results), 3),
    }
    Path(output_path).write_text(json.dumps(final, indent=2) + "\n")
    print("FINAL", json.dumps({k:v for k,v in final.items() if k not in ("categories","archive_checksums")}), flush=True)
    return final


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", default="public-benchmark.json")
    p.add_argument("--max-files", type=int, default=8)
    p.add_argument("--master", default="local[2]")
    p.add_argument("--check-coverage", action="store_true", help="Run exact distinct-quarter coverage per profile (separate shuffled workload)")
    args = p.parse_args()
    if not 1 <= args.max_files <= 8: p.error("--max-files must be between 1 and 8")
    spark = (SparkSession.builder.appName("fluvius-p6269-real-data-quality-benchmark")
        .master(args.master).config("spark.sql.session.timeZone","UTC")
        .config("spark.sql.shuffle.partitions","8").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try: benchmark(spark, args.output, max_files=args.max_files, check_coverage=args.check_coverage)
    finally: spark.stop()


if __name__ == "__main__": main()
