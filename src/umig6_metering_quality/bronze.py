"""One-pass Bronze screening for bulk metering ingestion.

This is intentionally *not* revision resolution or interval completeness.
Unlike process_metering(), it preserves all input rows (including duplicates)
so an ingestion pipeline can quarantine malformed values before expensive
correction-aware shuffles. Native Spark expressions only.
"""
from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import DataFrame, functions as F
from .pipeline import REQUIRED, _check_columns, _check_types, _checks


@dataclass(frozen=True)
class BronzeScreenResult:
    evaluated: DataFrame
    accepted: DataFrame
    quarantine: DataFrame
    summary: DataFrame


def screen_metering_rows(
    readings: DataFrame, *, interval_minutes: int = 15,
    validate_ean_format: bool = True,
) -> BronzeScreenResult:
    """Validate each observation without aggregation, deduplication or joins.

    Passing does not establish whether the row was superseded, is a conflicting
    correction, has valid source timestamp provenance, or fills an active scope.
    Run provenance screening and process_metering() afterwards when needed.
    """
    if interval_minutes <= 0 or 1440 % interval_minutes:
        raise ValueError("interval_minutes must be a positive divisor of 1440")
    _check_columns(readings, REQUIRED, "readings")
    _check_types(readings)
    checked = _checks(readings, interval_minutes, validate_ean_format)
    passed = F.size("quality_errors") == 0
    result = checked.withColumn("bronze_screen_status", F.when(passed, "PASS").otherwise("QUARANTINE"))
    summary = result.agg(
        F.count("*").alias("rows_screened"),
        F.sum(F.when(F.col("bronze_screen_status") == "PASS", 1).otherwise(0)).alias("accepted_rows"),
        F.sum(F.when(F.col("bronze_screen_status") == "QUARANTINE", 1).otherwise(0)).alias("quarantined_rows"),
    )
    return BronzeScreenResult(
        evaluated=result,
        accepted=result.where("bronze_screen_status = 'PASS'"),
        quarantine=result.where("bronze_screen_status = 'QUARANTINE'"),
        summary=summary,
    )
