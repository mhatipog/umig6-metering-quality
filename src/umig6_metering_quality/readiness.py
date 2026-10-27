"""Conservative, columnar migration-readiness profiling for legacy energy data.

These checks are *prerequisites* for downstream UMIG6-related processing, not
UMIG6 conformance or proof that a market process can be executed.

The caller supplies a normalized DataFrame. An absent column is deliberately
reported as a blocker, not fabricated from another field or silently defaulted.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, TimestampType, StringType


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    required: tuple[str, ...]
    dates: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    explanation: str = ""


SCENARIOS: dict[str, ScenarioSpec] = {
    "METERING_REPLAY": ScenarioSpec(
        "METERING_REPLAY",
        ("ean", "meter_id", "register", "interval_start_utc", "interval_end_utc", "energy_kwh"),
        dates=("interval_start_utc", "interval_end_utc"),
        optional=("revision_at_utc", "measurement_method"),
        explanation="Can historical interval observations be replayed with stable keys and valid time windows?",
    ),
    "ASSET_HISTORY": ScenarioSpec(
        "ASSET_HISTORY",
        ("ean", "asset_id", "meter_id", "valid_from_utc", "valid_to_utc"),
        dates=("valid_from_utc", "valid_to_utc"),
        optional=("commodity", "register"),
        explanation="Can the historical association between access points and physical meters be reconstructed?",
    ),
    "SUPPLIER_CHANGE": ScenarioSpec(
        "SUPPLIER_CHANGE",
        ("ean", "supplier_id", "contract_start_utc", "contract_end_utc"),
        dates=("contract_start_utc", "contract_end_utc"),
        optional=("commodity", "market_role"),
        explanation="Is the legacy supplier assignment timeline structurally adequate for later process mapping?",
    ),
}


@dataclass(frozen=True)
class ReadinessResult:
    evaluated: DataFrame
    ready: DataFrame
    quarantine: DataFrame
    summary: DataFrame


def _nonempty(df: DataFrame, column: str) -> F.Column:
    if column not in df.columns:
        return F.lit(False)
    return F.col(column).isNotNull() & (F.length(F.trim(F.col(column).cast("string"))) > 0)


def _time(df: DataFrame, name: str) -> F.Column:
    if name not in df.columns or not isinstance(df.schema[name].dataType, TimestampType):
        return F.lit(None).cast("timestamp")
    return F.col(name)


def evaluate_readiness(data: DataFrame, scenario: str, *, validate_ean_format: bool = True) -> ReadinessResult:
    """Profile *all rows* for a migration scenario; missing columns are blockers.

    Output is lazy and fully distributed. ``summary`` executes a Spark groupBy
    and must only be materialized if needed. It measures *structural* readiness,
    not market-message certification, historical truth, or a migration forecast.
    """
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown scenario {scenario!r}; choose from {', '.join(SCENARIOS)}")
    spec = SCENARIOS[scenario]
    reasons: list[F.Column] = []
    warnings: list[F.Column] = []
    for column in spec.required:
        if column not in data.columns:
            reasons.append(F.lit(f"MISSING_COLUMN_{column.upper()}"))
        else:
            # Open-ended asset and supplier assignments are legitimate legacy data.
            # The end column must exist and be typed, but may contain NULL.
            if not (column == spec.dates[-1] and scenario in ("ASSET_HISTORY", "SUPPLIER_CHANGE")):
                reasons.append(F.when(~_nonempty(data, column), F.lit(f"MISSING_{column.upper()}")))
    # Numeric identifiers are not safe to canonicalize after ingestion: leading
    # zeros may already have been destroyed in legacy ETL or spreadsheet paths.
    for identifier in ("ean", "meter_id", "asset_id", "supplier_id", "register"):
        if identifier in spec.required and identifier in data.columns:
            if not isinstance(data.schema[identifier].dataType, StringType):
                reasons.append(F.lit(f"WRONG_TYPE_{identifier.upper()}"))
    if validate_ean_format and "ean" in data.columns:
        # Format screening only: not an EAN allocation or checksum authority.
        reasons.append(F.when(_nonempty(data, "ean") & ~F.col("ean").cast("string").rlike(r"^54[0-9]{16}$"),
                              F.lit("INVALID_BELGIAN_EAN_FORMAT")))
    for column in spec.dates:
        if column in data.columns and not isinstance(data.schema[column].dataType, TimestampType):
            reasons.append(F.lit(f"WRONG_TYPE_{column.upper()}"))
    if len(spec.dates) == 2 and all(c in data.columns for c in spec.dates):
        start, end = spec.dates
        if isinstance(data.schema[start].dataType, TimestampType) and isinstance(data.schema[end].dataType, TimestampType):
            reasons.append(F.when(_time(data, end) <= _time(data, start), F.lit("INVALID_TIME_WINDOW")))
    if scenario == "METERING_REPLAY" and "energy_kwh" in data.columns:
        if isinstance(data.schema["energy_kwh"].dataType, NumericType):
            reasons.append(F.when((F.col("energy_kwh") < F.lit(0)) |
                                  F.isnan(F.col("energy_kwh").cast("double")), F.lit("INVALID_ENERGY")))
        else:
            reasons.append(F.lit("WRONG_TYPE_ENERGY_KWH"))
    if scenario == "METERING_REPLAY" and all(c in data.columns for c in spec.dates):
        if all(isinstance(data.schema[c].dataType, TimestampType) for c in spec.dates):
            diff = F.unix_timestamp("interval_end_utc") - F.unix_timestamp("interval_start_utc")
            reasons.append(F.when((diff > 0) & (diff != 900), F.lit("NOT_QUARTER_HOURLY")))
    for column in spec.optional:
        if column not in data.columns:
            warnings.append(F.lit(f"UNAVAILABLE_{column.upper()}"))
        else:
            warnings.append(F.when(~_nonempty(data, column), F.lit(f"MISSING_OPTIONAL_{column.upper()}")))

    # Schema may be near-empty; make array types consistent without Python UDFs.
    def array_of(items: list[F.Column]) -> F.Column:
        return F.filter(F.array(*items) if items else F.array().cast("array<string>"), lambda item: item.isNotNull())

    evaluated = (data.withColumn("scenario", F.lit(scenario))
                 .withColumn("readiness_blockers", array_of(reasons))
                 .withColumn("readiness_warnings", array_of(warnings))
                 .withColumn("is_ready", F.size("readiness_blockers") == 0))
    ready = evaluated.filter("is_ready")
    quarantine = evaluated.filter("NOT is_ready")
    summary = (evaluated.groupBy("scenario")
               .agg(F.count(F.lit(1)).alias("rows_checked"),
                    F.sum(F.when(F.col("is_ready"), F.lit(1)).otherwise(F.lit(0))).alias("rows_ready"),
                    F.sum(F.when(~F.col("is_ready"), F.lit(1)).otherwise(F.lit(0))).alias("rows_blocked"),
                    F.sum(F.when(F.size("readiness_warnings") > 0, F.lit(1)).otherwise(F.lit(0))).alias("rows_with_warnings"))
               .withColumn("structural_readiness_pct", F.when(F.col("rows_checked") > 0,
                   F.round(F.col("rows_ready") * 100.0 / F.col("rows_checked"), 2)).otherwise(F.lit(None).cast("double"))))
    return ReadinessResult(evaluated, ready, quarantine, summary)
