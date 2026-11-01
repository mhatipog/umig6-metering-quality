"""Regressions in manifest semantics and corrections must fail CI."""
import copy

import pytest
from pyspark.sql import functions as F
from umig6_metering_quality.evidence import lane_manifest, compare_manifests

def test_partition_and_row_order_independence(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    base = spark.createDataFrame([(1, "alpha"), (1, "alpha"), (2, "beta"), (3, None)], ["id", "label"])
    a = lane_manifest(base.repartition(1), buckets=8)
    b = lane_manifest(base.orderBy(F.desc("id")).repartition(3), buckets=8)
    assert a == b
    assert a["rows"] == 4

def test_detects_single_value_change_and_missing_duplicate(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    base = spark.createDataFrame([(1, "a"), (2, "b"), (2, "b")], ["id", "tag"])
    original = lane_manifest(base, buckets=8)
    changed = lane_manifest(base.withColumn("tag", F.when(F.col("id") == 1, "c").otherwise(F.col("tag"))), buckets=8)
    assert not compare_manifests(original, changed)["matches"]
    removed = lane_manifest(base.dropDuplicates(), buckets=8)
    assert not compare_manifests(original, removed)["matches"]

def test_reject_unknown_evidence_and_non_utc(spark):
    spark.conf.set("spark.sql.session.timeZone", "Europe/Brussels")
    try:
        with pytest.raises(ValueError):
            lane_manifest(spark.range(2))
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    valid = lane_manifest(spark.range(2))
    wrong = copy.deepcopy(valid)
    wrong["format"] = "nonstandard"
    with pytest.raises(ValueError):
        compare_manifests(valid, wrong)

def test_fixture_correction_counts(spark):
    # Import from the runnable public example; don't duplicate the fixtures.
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
    from cloud_validation import run
    observed = run(spark, bucket_count=8)
    assert observed["counts"] == {"current": 2, "rejected": 3, "superseded": 2, "changes": 1}
    assert all(v["fingerprint_sha256"] for v in observed["lanes"].values())
