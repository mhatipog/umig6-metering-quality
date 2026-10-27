"""Illustrative Databricks Bronze -> quarantine/review -> Silver batch.

Requires a canonical metering adapter with explicit origin metadata and an
audited source contract. Replace tables, retention, access controls and batch
IDs before deployment. Delta writes to different tables are NOT atomic.
"""
from pyspark.sql import functions as F
from umig6_metering_quality import (
    screen_metering_rows, screen_source_provenance, process_metering,
    validate_asset_history, reconcile_metering_assets, evaluate_readiness,
    envelope_quarantine, write_delta_batch,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN = "backfill_20261027"  # Existing immutable source batch

bronze = spark.read.table("energy_bronze.metering").where(F.col("load_id") == RUN)
assets = spark.read.table("energy_bronze.assets").where(F.col("load_id") == RUN)

# Stage 1: cheap row-grain screening. No duplicates or corrections removed.
screen = screen_metering_rows(bronze)

# Stage 2: operator-provided source evidence; never interpret a Z suffix
# or a newer export file as independent proof of UTC/correction precedence.
origin = screen_source_provenance(screen.accepted)

# Stage 3: version handling ONLY for genuinely source-authored revisions.
# Snapshots with no revision metadata remain analytically useful but must
# not pass through the official-looking correction replay.
reading_dq = process_metering(origin.replay_ready, require_source_revision=True)
asset_dq = validate_asset_history(assets)
joined = reconcile_metering_assets(reading_dq.current, asset_dq.accepted)
reference_review = joined.where(F.size("asset_reference_issues") > 0)

# Distinct review and rejection lanes. No metadata uncertainty disappears.
bronze_bad = envelope_quarantine(
    screen.quarantine, dataset="metering_bronze", batch_id=RUN,
)
origin_bad = envelope_quarantine(
    origin.quarantine, dataset="metering_provenance", batch_id=RUN,
    errors_column="provenance_failures",
)
origin_review = envelope_quarantine(
    origin.review, dataset="metering_provenance_review", batch_id=RUN,
    errors_column="provenance_review_reasons",
)
revision_bad = envelope_quarantine(
    reading_dq.rejected, dataset="metering_revisions", batch_id=RUN,
)
asset_bad = envelope_quarantine(
    asset_dq.rejected, dataset="asset_history", batch_id=RUN,
)

# These are five INDEPENDENT Delta sinks. Provision tables beforehand and
# restrict access to raw readings/EANs. Replace each specific batch on retry.
sinks = (
    (bronze_bad.quarantined, "energy_ops.metering_bronze_quarantine"),
    (origin_bad.quarantined, "energy_ops.metering_provenance_quarantine"),
    (origin_review.quarantined, "energy_ops.metering_provenance_review"),
    (asset_bad.quarantined, "energy_ops.asset_quarantine"),
)
for records, table in sinks:
    (records.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"batch_id = '{RUN}'").saveAsTable(table))

write_delta_batch(
    reading_dq.current.withColumn("batch_id", F.lit(RUN)),
    revision_bad.quarantined,
    batch_id=RUN,
    current_table="energy_silver.metering_by_batch",
    quarantine_table="energy_ops.metering_revision_quarantine",
)

# Run ASSET_HISTORY and METERING_REPLAY readiness independently on the right
# source contracts; the status means technical readiness, not MIG6 certification.
readiness = evaluate_readiness(assets, "ASSET_HISTORY")

# A separate orchestrator MUST verify row counts/all required sinks and then
# commit a batch-completion marker. No Gold table may read incomplete batches.
readiness.summary.show(truncate=False)
origin.summary.show(truncate=False)
screen.summary.show(truncate=False)
