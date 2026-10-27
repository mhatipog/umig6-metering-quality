"""Pinned, publicly documented metering prerequisites (not MIG6 certification).

The historic UMIG 6.5.2.2 Measure Validation Rules handbook, §5.1.2,
describes two minimum V1 checks for continuous values: presence of a measure
and digits compatible with the readout. It does *not* establish that passing
these checks awards V1 or any downstream market/billing validation level.

Readout digit counts must be supplied from authoritative device metadata.
Never infer them from rendered kWh decimals or a warehouse's numeric width.
"""
from __future__ import annotations

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType


@dataclass(frozen=True)
class RuleProfile:
    profile_id: str
    source_version: str
    document: str
    section: str
    url: str
    checked_rules: tuple[str, ...]
    notes: str


UMIG_ME_6522_PREREQUISITES = RuleProfile(
    profile_id="UMIG_ME_6_5_2_2_V1_PREREQUISITES",
    source_version="6.5.2.2",  # Historic pinned document, NOT the current market baseline.
    document="UMIG - HB - ME - 03 - Validation Rules",
    section="5.1.2 (Form and completeness of continuous values)",
    url=("https://www.atrias.be/current-documentation"),
    checked_rules=("ME_CONTINUOUS_VALUE_PRESENT", "ME_READOUT_DIGITS_MATCH"),
    notes=("Local pre-screen only; required digit-count metadata is external. "
           "Never interpret PASS as a market validation level."),
)
PROFILES = {UMIG_ME_6522_PREREQUISITES.profile_id: UMIG_ME_6522_PREREQUISITES}


@dataclass(frozen=True)
class ContinuousScreenResult:
    evaluated: DataFrame
    quarantine: DataFrame
    review: DataFrame
    summary: DataFrame


def screen_continuous_measures(
    readings: DataFrame,
    *,
    profile_id: str = UMIG_ME_6522_PREREQUISITES.profile_id,
    value_column: str = "energy_kwh",
    observed_digits_column: str | None = None,
    expected_digits_column: str | None = None,
) -> ContinuousScreenResult:
    """Distributed evidence screening with PASS, FAIL and NOT_EVALUATED.

    A missing value is quarantined. The digits check is performed only when
    both counts exist, are positive integers and were supplied by the caller.
    Unknown/missing metadata leads to REVIEW, never to a PASS. Output frames
    remain lazy, and source columns are untouched (no Python UDFs).

    ``observed_digits_column`` and ``expected_digits_column`` are integer
    *readout digit counts*, not textual kWh string lengths. It is the upstream
    adapter's responsibility to obtain them from metering system metadata.
    """
    if profile_id not in PROFILES:
        raise ValueError(f"Unknown pinned profile: {profile_id}")
    if value_column not in readings.columns:
        raise ValueError(f"Missing required measure column: {value_column}")
    if not isinstance(readings.schema[value_column].dataType, NumericType):
        raise TypeError(f"{value_column} must be a numeric Spark type")
    if (observed_digits_column is None) != (expected_digits_column is None):
        raise ValueError("Supply both digit-count columns or neither")
    metadata = observed_digits_column is not None
    if metadata:
        absent = {observed_digits_column, expected_digits_column} - set(readings.columns)
        if absent:
            raise ValueError(f"Missing readout metadata: {', '.join(sorted(absent))}")
    missing_measure = F.col(value_column).isNull() | F.isnan(F.col(value_column).cast("double"))
    digit_result = F.lit("NOT_EVALUATED")
    if metadata:
        # try_cast, rather than cast, is important on ANSI-enabled Databricks:
        # old warehouse extracts sometimes contain nonnumeric readout metadata.
        def safe_number(name: str, target: str):
            identifier = "`" + name.replace("`", "``") + "`"
            return F.expr(f"try_cast({identifier} AS {target})")

        observed = safe_number(observed_digits_column, "BIGINT")
        expected = safe_number(expected_digits_column, "BIGINT")
        observed_decimal = safe_number(observed_digits_column, "DOUBLE")
        expected_decimal = safe_number(expected_digits_column, "DOUBLE")
        # Unknown, malformed and non-positive source metadata is not evidence
        # that an observation passes or fails an official format check.
        usable = (observed.isNotNull() & expected.isNotNull()
                  & (observed > 0) & (expected > 0)
                  & (observed_decimal == observed.cast("double"))
                  & (expected_decimal == expected.cast("double")))
        digit_result = (F.when(~usable, F.lit("NOT_EVALUATED"))
                        .when(observed != expected, F.lit("FAIL"))
                        .otherwise(F.lit("PASS")))
    evaluated = (readings
        .withColumn("rule_profile_id", F.lit(profile_id))
        .withColumn("ME_CONTINUOUS_VALUE_PRESENT", F.when(missing_measure, F.lit("FAIL")).otherwise(F.lit("PASS")))
        .withColumn("ME_READOUT_DIGITS_MATCH", digit_result)
        .withColumn("rule_failures", F.filter(F.array(
            F.when(F.col("ME_CONTINUOUS_VALUE_PRESENT") == "FAIL", F.lit("ME_CONTINUOUS_VALUE_PRESENT")),
            F.when(F.col("ME_READOUT_DIGITS_MATCH") == "FAIL", F.lit("ME_READOUT_DIGITS_MATCH")),
        ), lambda x: x.isNotNull()))
        .withColumn("rules_not_evaluated", F.filter(F.array(
            F.when(F.col("ME_READOUT_DIGITS_MATCH") == "NOT_EVALUATED", F.lit("ME_READOUT_DIGITS_MATCH")),
        ), lambda x: x.isNotNull()))
        .withColumn("screen_status", F.when(F.size("rule_failures") > 0, F.lit("FAIL"))
                    .when(F.size("rules_not_evaluated") > 0, F.lit("INCOMPLETE"))
                    .otherwise(F.lit("PASS"))))
    summary = (evaluated.groupBy("rule_profile_id")
        .agg(F.count(F.lit(1)).alias("rows_screened"),
             F.sum(F.when(F.col("screen_status") == "PASS", 1).otherwise(0)).alias("passed"),
             F.sum(F.when(F.col("screen_status") == "FAIL", 1).otherwise(0)).alias("failed"),
             F.sum(F.when(F.col("screen_status") == "INCOMPLETE", 1).otherwise(0)).alias("incomplete")))
    return ContinuousScreenResult(
        evaluated=evaluated,
        quarantine=evaluated.where(F.col("screen_status") == "FAIL"),
        review=evaluated.where(F.col("screen_status") == "INCOMPLETE"),
        summary=summary,
    )
