"""Deterministic, bounded Spark evidence for data migration comparisons.

Produces aggregates only. These checksums are high-confidence regression
signals, NOT a cryptographic proof of full data equivalence. Independent
systems can compare results without exporting underlying metering records.
"""
from __future__ import annotations

import hashlib
import json
from typing import Mapping

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType

FORMAT = "umig6-spark-lane-evidence-v1"


def lane_manifest(frame: DataFrame, *, buckets: int = 64) -> dict:
    """Compute partition-invariant fingerprints without collecting source rows.

    For each SHA-256 row digest, retain two nonoverlapping 60-bit numeric
    fragments. Their decimal sums and count are grouped into fixed buckets.
    The aggregates retain duplicates but cannot be inverted to recover rows.
    They are not collision-proof; use independent row-level comparisons when
    a formal regulatory or financial acceptance guarantee is required.
    """
    if not 1 <= buckets <= 4096 or (buckets & (buckets - 1)):
        raise ValueError("buckets must be a power of two between 1 and 4096")
    zone = frame.sparkSession.conf.get("spark.sql.session.timeZone")
    if zone.upper() != "UTC":
        raise ValueError("Evidence requires spark.sql.session.timeZone=UTC")
    fields = sorted(frame.schema.fields, key=lambda f: f.name)
    if not fields or len({f.name for f in fields}) != len(fields):
        raise ValueError("Evidence requires distinct, nonempty column names")
    # Explicit ISO timestamp representation and null keys. The schema is
    # included separately: a string/decimal type change must not pass.
    payload = F.to_json(
        F.struct(*[F.col(f.name).alias(f.name) for f in fields]),
        options={"ignoreNullFields": "false",
                 "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"},
    )
    hashes = frame.select(F.sha2(payload, 256).alias("_digest"))
    part_a = F.conv(F.substring("_digest", 1, 15), 16, 10).cast(DecimalType(38, 0))
    part_b = F.conv(F.substring("_digest", 16, 15), 16, 10).cast(DecimalType(38, 0))
    grouped = (hashes
               .withColumn("_bucket", F.pmod(F.xxhash64("_digest"), F.lit(buckets)))
               .groupBy("_bucket")
               .agg(F.count("*").alias("rows"),
                    F.sum(part_a).alias("sum_a"),
                    F.sum(part_b).alias("sum_b")))
    # This collects at most 'buckets' *aggregate* rows, never source data.
    observed = sorted(grouped.collect(), key=lambda r: r["_bucket"])
    partitions = [
        {"bucket": int(r["_bucket"]), "rows": int(r["rows"]),
         "sum_a": str(r["sum_a"]), "sum_b": str(r["sum_b"])}
        for r in observed
    ]
    schema = [{"name": f.name, "type": f.dataType.simpleString()} for f in fields]
    total = sum(item["rows"] for item in partitions)
    envelope = {"format": FORMAT, "columns": schema, "rows": total,
                "bucket_count": buckets, "buckets": partitions}
    envelope["fingerprint_sha256"] = hashlib.sha256(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return envelope


def compare_manifests(reference: Mapping, candidate: Mapping) -> dict:
    """Return explicit mismatches; never silently deem absent evidence equal."""
    if reference.get("format") != FORMAT or candidate.get("format") != FORMAT:
        raise ValueError("Unknown evidence format")
    fields = ("columns", "rows", "bucket_count", "buckets", "fingerprint_sha256")
    mismatches = [key for key in fields if reference.get(key) != candidate.get(key)]
    return {"matches": not mismatches, "mismatched_fields": mismatches}
