"""Data-quality screening of point-to-meter history from legacy asset systems.

An overlapping physical meter assignment to two EANs is raised for *review*;
there are legitimate migration and multi-meter situations. Do not infer real
market asset status or start/terminate contracts from these observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

REQUIRED = ("ean", "asset_id", "meter_id", "valid_from_utc", "valid_to_utc")


@dataclass(frozen=True)
class AssetQualityResult:
    accepted: DataFrame
    rejected: DataFrame
    overlaps_for_review: DataFrame


def validate_asset_history(assets: DataFrame, *, validate_ean_format: bool = True) -> AssetQualityResult:
    """Validate asset keys and validity windows; report possible assignment overlaps.

    The overlap check is a sorting/window pass; no self-join, Python UDF or
    driver-side row expansion. Null end means an open-ended assignment.
    """
    missing = set(REQUIRED) - set(assets.columns)
    if missing:
        raise ValueError(f"assets missing required columns: {', '.join(sorted(missing))}")
    for name in ("valid_from_utc", "valid_to_utc"):
        if not isinstance(assets.schema[name].dataType, TimestampType):
            raise TypeError(f"{name} must be Spark TimestampType")
    errors = []
    for c in ("ean", "asset_id", "meter_id"):
        errors.append(F.when(F.col(c).isNull() | (F.length(F.trim(F.col(c).cast("string"))) == 0),
                             F.lit(f"MISSING_{c.upper()}")))
    errors.append(F.when(F.col("valid_from_utc").isNull(), F.lit("MISSING_VALID_FROM_UTC")))
    errors.append(F.when(F.col("valid_from_utc").isNotNull() & F.col("valid_to_utc").isNotNull()
                         & (F.col("valid_to_utc") <= F.col("valid_from_utc")), F.lit("INVALID_ASSET_PERIOD")))
    if validate_ean_format:
        errors.append(F.when(F.col("ean").isNotNull() & ~F.col("ean").cast("string").rlike(r"^54[0-9]{16}$"),
                             F.lit("INVALID_BELGIAN_EAN_FORMAT")))
    checked = assets.withColumn("quality_errors", F.filter(F.array(*errors), lambda x: x.isNotNull()))
    accepted = checked.where(F.size("quality_errors") == 0)
    rejected = checked.where(F.size("quality_errors") > 0)
    # Two assignments to the same *physical* meter may overlap. This is not
    # always wrong, so it is a review signal rather than a quarantine reason.
    w = (Window.partitionBy("meter_id").orderBy("valid_from_utc", "ean", "asset_id")
         .rowsBetween(Window.unboundedPreceding, -1))
    past_end = F.max(F.coalesce(F.col("valid_to_utc"), F.to_timestamp(F.lit("9999-12-31 00:00:00")))).over(w)
    found = (accepted.withColumn("_previous_max_end", past_end)
             .withColumn("_assignment_overlap", F.col("_previous_max_end").isNotNull()
                         & (F.col("valid_from_utc") < F.col("_previous_max_end"))))
    review = found.filter("_assignment_overlap").drop("_assignment_overlap")
    return AssetQualityResult(accepted, rejected, review)


def reconcile_metering_assets(readings: DataFrame, assets: DataFrame, *, broadcast_assets: bool = False) -> DataFrame:
    """Annotate canonical metering rows with temporal point/meter reference coverage.

    Zero matches means missing master-data evidence; multiple means ambiguous
    history. Does NOT prove a meter value is wrong, nor infer a missing meter.
    Keep this optional: it is a potentially expensive range join on legacy data.
    Input asset history should first pass ``validate_asset_history``.
    Only a caller that knows its dimension is small should enable broadcast.
    """
    readings_required = ("ean", "meter_id", "interval_start_utc", "interval_end_utc")
    for frame, cols, name in ((readings, readings_required, "readings"), (assets, REQUIRED, "assets")):
        missing = set(cols) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} missing required columns: {', '.join(sorted(missing))}")
    keys = readings.select(*readings_required).dropDuplicates()
    # An exact repeated source asset assignment is one match, not ambiguity.
    history = assets.select(*REQUIRED).dropDuplicates()
    if broadcast_assets:
        history = F.broadcast(history)
    matched = keys.alias("r").join(history.alias("a"),
        (F.col("r.ean") == F.col("a.ean")) &
        (F.col("r.meter_id") == F.col("a.meter_id")) &
        (F.col("r.interval_start_utc") >= F.col("a.valid_from_utc")) &
        (F.col("a.valid_to_utc").isNull() | (F.col("r.interval_end_utc") <= F.col("a.valid_to_utc"))),
        "left")
    counts = (matched.groupBy(*[F.col(f"r.{c}") for c in readings_required])
              .agg(F.count(F.col("a.asset_id")).alias("asset_assignment_matches")))
    return (readings.join(counts, list(readings_required), "left")
            .withColumn("asset_assignment_matches", F.coalesce(F.col("asset_assignment_matches"), F.lit(0)))
            .withColumn("asset_reference_issues", F.filter(F.array(
                F.when(F.col("asset_assignment_matches") == 0, F.lit("NO_MATCHING_ASSET_ASSIGNMENT")),
                F.when(F.col("asset_assignment_matches") > 1, F.lit("MULTIPLE_ASSET_ASSIGNMENTS"))),
                lambda x: x.isNotNull())))
