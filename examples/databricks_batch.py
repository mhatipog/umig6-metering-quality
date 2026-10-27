"""Illustrative Databricks notebook/job: three independent data-quality lanes.

Replace all paths and tables before use. Designed for Databricks Delta, not
unit tests. Bronze is immutable and should be scoped by a unique load/run ID.
Per-output Delta replaceWhere is retryable; multiple outputs aren't atomic.
"""
from umig6_metering_quality import (
    process_metering, validate_asset_history, reconcile_metering_assets,
    evaluate_readiness, envelope_quarantine, write_delta_batch,
)
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN = "backfill_20261027" # unique immutable source batch

# Input already normalized to the package's explicit canonical contract.
bronze = spark.read.table("energy_bronze.metering").where(F.col("load_id") == RUN)
assets = spark.read.table("energy_bronze.assets").where(F.col("load_id") == RUN)

asset_dq = validate_asset_history(assets)
reading_dq = process_metering(bronze)

# Optional range join: use it only if point/meter history is relevant. Do not
# broadcast a large asset dimension unless its in-memory size is known.
joined = reconcile_metering_assets(reading_dq.current, asset_dq.accepted)
asset_reference_issues = joined.filter(F.size("asset_reference_issues") > 0)

readiness = evaluate_readiness(assets, "ASSET_HISTORY")
# Repeat for METERING_REPLAY or SUPPLIER_CHANGE on appropriately shaped inputs.

meter_quarantine = envelope_quarantine(reading_dq.rejected, dataset="metering", batch_id=RUN)
asset_quarantine = envelope_quarantine(asset_dq.rejected, dataset="asset_history", batch_id=RUN)
# Keep access restricted: these frames can contain identifiers and readings.

# Provision schemas and target Delta tables first. Accepted + quarantine
# persistence are separate transactions; orchestrate a batch completion marker
# only after all writes, and rebuild downstream Gold on successful completion.
write_delta_batch(
    reading_dq.current.withColumn("batch_id", F.lit(RUN)),
    meter_quarantine.quarantined,
    batch_id=RUN,
    current_table="energy_silver.metering_by_batch",
    quarantine_table="energy_ops.metering_quarantine",
)

# Useful reporting tables, keyed by batch; choose retention and service-level
# objectives in the organization, never log raw customer rows.
readiness.summary.show(truncate=False)
meter_quarantine.reason_counts.show(truncate=False)
asset_quarantine.reason_counts.show(truncate=False)
