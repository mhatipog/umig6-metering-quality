"""Fault-injected Delta recovery check for a caller-owned managed Spark catalog.

Creates uniquely named scratch managed Delta tables; does not touch any
existing customer tables. Tables are dropped after validation unless cleanup
fails, in which case only their generated names are reported. The publication
marker is a *demonstration gate*, not a cross-table Delta transaction.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from pyspark.sql import functions as F
from umig6_metering_quality import process_metering
from umig6_metering_quality.evidence import lane_manifest
from cloud_validation import fixture, OUTPUTS, EXPECTED

def run(spark, *, prefix=None, buckets=64):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    slug = prefix or uuid.uuid4().hex[:12]
    if not re.fullmatch(r"[a-z0-9]{8,24}", slug):
        raise ValueError("Scratch identifier must contain 8-24 lowercase alphanumerics")
    names = {lane: "umig6_recovery_" + slug + "_" + lane for lane in OUTPUTS}
    marker = "umig6_recovery_" + slug + "_publication"
    allocated = []
    phase = []
    cleanup = {}
    beginning = time.perf_counter()
    outcome = {"format": "umig6-delta-recovery-v1", "synthetic_only": True,
               "id_redacted": True, "phases": phase, "cleanup": cleanup}
    try:
        with process_metering(fixture(spark), cache_shared=False) as resolved:
            lanes = {lane: getattr(resolved, lane) for lane in OUTPUTS}
            baseline = {lane: lane_manifest(lanes[lane], buckets=buckets)
                        for lane in OUTPUTS}
            if {lane: baseline[lane]["rows"] for lane in OUTPUTS} != EXPECTED:
                raise AssertionError("Source fixture correctness precondition failed")

            def write(lane, *, first=False, override=None):
                df = override if override is not None else lanes[lane]
                writer = df.write.format("delta")
                if first:
                    # Spark Connect on managed serverless does not consistently
                    # support DataFrameWriter mode=errorifexists.
                    existing = spark.sql("SHOW TABLES LIKE '" + names[lane] + "'").count()
                    if existing:
                        raise RuntimeError("Scratch table name already exists")
                    writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(names[lane])
                    allocated.append(names[lane])
                else:
                    writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(names[lane])

            def verify_all():
                mismatched = []
                for lane in OUTPUTS:
                    try:
                        actual = lane_manifest(spark.table(names[lane]), buckets=buckets)
                        if actual != baseline[lane]:
                            mismatched.append(lane)
                    except Exception:
                        mismatched.append(lane)
                return mismatched

            def write_marker(state):
                frame = spark.createDataFrame(
                    [(state, "SYNTHETIC_8_ROWS", json.dumps(
                        {lane: baseline[lane]["fingerprint_sha256"] for lane in OUTPUTS},
                        sort_keys=True))],
                    "state string, input_scope string, lane_fingerprints_json string",
                )
                if marker not in allocated:
                    if spark.sql("SHOW TABLES LIKE '" + marker + "'").count():
                        raise RuntimeError("Scratch publication marker exists")
                    frame.write.format("delta").mode("overwrite").saveAsTable(marker)
                    allocated.append(marker)
                else:
                    frame.write.format("delta").mode("overwrite").saveAsTable(marker)

            def published():
                if marker not in allocated:
                    return False
                rows = spark.table(marker).select("state").collect()
                return len(rows) == 1 and rows[0]["state"] == "PUBLISHED"

            # Emulate a crash after writing one of four independent sinks.
            write("current", first=True)
            assert not published()
            phase.append({"phase": "interrupted_after_first_sink", "publication_blocked": True})
            # Recover by writing all remaining sinks and comparing every lane.
            for lane in OUTPUTS:
                if lane != "current":
                    write(lane, first=True)
            discrepancy = verify_all()
            if discrepancy:
                raise AssertionError("Initial recovery differs in lanes: " + ",".join(discrepancy))
            write_marker("PUBLISHED")
            if not published():
                raise AssertionError("Completion marker not visible")
            phase.append({"phase": "first_reconciliation", "four_lanes_match": True,
                          "publication_visible": True})

            # Explicitly unpublish BEFORE retries, because independently
            # replaced Delta tables do not share one atomic commit boundary.
            write_marker("UNPUBLISHED")
            if published():
                raise AssertionError("Marker was not retracted")
            # Simulate a bad intermediate write; fail closed rather than
            # silently claiming that the old published batch remains valid.
            write("current", override=lanes["current"].limit(0))
            disagreement = verify_all()
            if disagreement != ["current"] or published():
                raise AssertionError("Fault injection failed to invalidate publication")
            phase.append({"phase": "damaged_sink_detected", "mismatched_lanes": disagreement,
                          "publication_blocked": True})

            # Repeat all outputs using the *same* source. Retries should not
            # append duplicates or alter any fingerprint.
            for lane in OUTPUTS:
                write(lane)
            disagreement = verify_all()
            if disagreement:
                raise AssertionError("Replay differs in lanes: " + ",".join(disagreement))
            write_marker("PUBLISHED")
            if not published():
                raise AssertionError("Final publication marker absent")
            phase.append({"phase": "idempotent_replay", "four_lanes_match": True,
                          "publication_visible": True})
            outcome.update({
                "verified": True, "counts": EXPECTED,
                "lane_fingerprints": {
                    lane: baseline[lane]["fingerprint_sha256"] for lane in OUTPUTS},
                "publication_contract": "orchestrator-controlled marker; not atomic cross-table Delta",
            })
    finally:
        # Explicitly drop only scratch tables that this invocation created.
        for table in reversed(allocated):
            try:
                spark.sql("DROP TABLE IF EXISTS " + table).collect()
                cleanup[table.split("_")[-1]] = "dropped"
            except Exception as e:
                cleanup[table.split("_")[-1]] = "drop_failed_" + type(e).__name__
        outcome["elapsed_seconds"] = round(time.perf_counter() - beginning, 3)
        outcome["cleanup_success"] = (len(cleanup) == len(allocated)
                                      and all(v == "dropped" for v in cleanup.values()))
    if not outcome.get("verified") or not outcome["cleanup_success"]:
        raise AssertionError("Delta recovery/cleanup did not complete")
    return outcome
