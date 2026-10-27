"""Technical source-provenance gate for historical metering feeds.

This is a *local engineering control*. It is not an Atrias validation code, and
a caller asserting UTC_VERIFIED does not amount to independent certification.
An operator must provide a documented source contract and evidence reference.

Use before deduplicating revisions or joining legacy readings by UTC instants.
No Python UDFs, driver collection or additional Spark shuffle is required.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, TimestampType

PROFILE_ID = "TECH_SOURCE_PROVENANCE_V1"
TIMESTAMP_STATES = (
    "UTC_VERIFIED", "OFFSET_VERIFIED", "UTC_LABEL_UNVERIFIED",
    "LOCAL_UNRESOLVED", "UNKNOWN",
)
REVISION_BASES = (
    "SOURCE_REVISION", "EXPORT_SNAPSHOT", "INGESTION_TIME", "UNKNOWN",
)
REQUIRED = (
    "source_system", "source_record_id", "source_evidence_ref",
    "timestamp_semantics", "revision_basis",
)


@dataclass(frozen=True)
class SourceProvenanceResult:
    evaluated: DataFrame
    analytics_ready: DataFrame
    replay_ready: DataFrame
    review: DataFrame
    quarantine: DataFrame
    summary: DataFrame


def screen_source_provenance(readings: DataFrame) -> SourceProvenanceResult:
    """Separate eligible UTC analytics from unsupported revision assumptions.

    Source fields are supplied explicitly by an adapter/operator:
    - source_system: stable name of the origin system;
    - source_record_id: stable original identifier (not row_number);
    - source_evidence_ref: internal document/catalogue reference for timestamp
      semantics and source version precedence (not necessarily a URL);
    - timestamp_semantics: one of TIMESTAMP_STATES;
    - revision_basis: one of REVISION_BASES;
    - revision_at_utc: optional Spark timestamp, only meaningful for replay
      with SOURCE_REVISION.

    PASS means sufficient *asserted evidence* for technical processing, not
    proof that Atrias or any grid operator validated the values.
    """
    missing = sorted(set(REQUIRED) - set(readings.columns))
    # No guesswork: missing fields become INCOMPLETE at row level, allowing
    # legacy extracts to be profiled rather than failing an entire Spark job.
    wrong_types = sorted(
        c for c in REQUIRED if c in readings.columns
        and not isinstance(readings.schema[c].dataType, StringType)
    )
    revision_wrong_type = (
        "revision_at_utc" in readings.columns
        and not isinstance(readings.schema["revision_at_utc"].dataType, TimestampType)
    )

    def value(name: str):
        return F.col(name) if name in readings.columns and name not in wrong_types else F.lit(None).cast("string")

    def nonempty(name: str):
        return value(name).isNotNull() & (F.length(F.trim(value(name))) > 0)

    ts = value("timestamp_semantics")
    basis = value("revision_basis")
    errors = F.filter(F.array(
        F.when(nonempty("timestamp_semantics") & ~ts.isin(*TIMESTAMP_STATES),
               F.lit("UNKNOWN_TIMESTAMP_SEMANTICS")),
        F.when(nonempty("revision_basis") & ~basis.isin(*REVISION_BASES),
               F.lit("UNKNOWN_REVISION_BASIS")),
        *[F.lit("WRONG_TYPE_" + name.upper()) for name in wrong_types],
        *([F.lit("WRONG_TYPE_REVISION_AT_UTC")] if revision_wrong_type else []),
    ), lambda x: x.isNotNull())

    review_reasons = F.filter(F.array(
        *[F.lit("MISSING_COLUMN_" + name.upper()) for name in missing],
        F.when(~nonempty("source_system"), F.lit("MISSING_SOURCE_SYSTEM")),
        F.when(~nonempty("source_record_id"), F.lit("MISSING_SOURCE_RECORD_ID")),
        F.when(~nonempty("source_evidence_ref"), F.lit("MISSING_SOURCE_EVIDENCE")),
        F.when(~nonempty("timestamp_semantics")
               | ts.isin("UTC_LABEL_UNVERIFIED", "LOCAL_UNRESOLVED", "UNKNOWN"),
               F.lit("UTC_INSTANT_NOT_VERIFIED")),
        F.when(~nonempty("revision_basis") | (basis == "UNKNOWN"),
               F.lit("REVISION_BASIS_NOT_ESTABLISHED")),
    ), lambda x: x.isNotNull())

    revision = (F.col("revision_at_utc") if "revision_at_utc" in readings.columns
                and not revision_wrong_type else F.lit(None).cast("timestamp"))
    evaluated = (
        readings.withColumn("provenance_profile_id", F.lit(PROFILE_ID))
        .withColumn("provenance_failures", errors)
        .withColumn("provenance_review_reasons", review_reasons)
        .withColumn(
            "provenance_status",
            F.when(F.size("provenance_failures") > 0, F.lit("FAIL"))
             .when(F.size("provenance_review_reasons") > 0, F.lit("INCOMPLETE"))
             .otherwise(F.lit("PASS")),
        )
        .withColumn(
            "source_revision_replay_ready",
            (F.col("provenance_status") == "PASS")
            & (basis == "SOURCE_REVISION")
            & revision.isNotNull(),
        )
    )

    # Missing SOURCE_REVISION timestamps are never replay ready. They remain
    # analytics candidates (not an excuse to order by file ingestion time).
    approved = evaluated.where(F.col("provenance_status") == "PASS")
    summary = evaluated.groupBy("provenance_profile_id").agg(
        F.count("*").alias("rows_checked"),
        F.sum(F.when(F.col("provenance_status") == "PASS", 1).otherwise(0)).alias("analytics_ready_rows"),
        F.sum(F.when("source_revision_replay_ready", 1).otherwise(0)).alias("replay_ready_rows"),
        F.sum(F.when(F.col("provenance_status") == "INCOMPLETE", 1).otherwise(0)).alias("review_rows"),
        F.sum(F.when(F.col("provenance_status") == "FAIL", 1).otherwise(0)).alias("quarantine_rows"),
    )
    return SourceProvenanceResult(
        evaluated=evaluated,
        analytics_ready=approved,
        replay_ready=approved.where("source_revision_replay_ready"),
        review=evaluated.where(F.col("provenance_status") == "INCOMPLETE"),
        quarantine=evaluated.where(F.col("provenance_status") == "FAIL"),
        summary=summary,
    )
