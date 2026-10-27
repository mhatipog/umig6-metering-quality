"""Columnar, Spark-native quality controls for versioned electricity interval data.

Input is a deliberately small canonical contract. Adapters for specific exports
should be implemented outside this package: neither consumer exports nor internal
market messages are assumed to share this schema.

All timestamps must represent UTC instants; set the Spark session timezone to UTC.
No Python UDFs, driver-side collections or implicit billing decisions are used.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, NumericType, StringType, TimestampType

KEY = ("ean", "meter_id", "register", "interval_start_utc", "interval_end_utc")
IDENTITY = ("ean", "meter_id", "register")
REQUIRED = (*KEY, "energy_kwh", "revision_at_utc", "measurement_method")
SCOPE_REQUIRED = (*IDENTITY, "scope_id", "valid_from_utc", "valid_to_utc")


@dataclass(frozen=True)
class MeteringQualityResult:
    """Separate trustworthy and exceptional outputs; caller owns persistence."""

    current: DataFrame
    rejected: DataFrame
    superseded: DataFrame
    changes: DataFrame
    coverage: DataFrame | None


def _check_columns(df: DataFrame, names: tuple[str, ...], frame: str) -> None:
    absent = set(names) - set(df.columns)
    if absent:
        raise ValueError(f"{frame}: missing required columns: {', '.join(sorted(absent))}")


def _check_types(df: DataFrame) -> None:
    for name in ("interval_start_utc", "interval_end_utc", "revision_at_utc"):
        if not isinstance(df.schema[name].dataType, TimestampType):
            raise TypeError(f"{name} must be Spark TimestampType (UTC)")
    if not isinstance(df.schema["energy_kwh"].dataType, NumericType):
        raise TypeError("energy_kwh must be a Spark numeric type")
    for name in (*IDENTITY, "measurement_method"):
        if not isinstance(df.schema[name].dataType, StringType):
            raise TypeError(f"{name} must be a Spark string")


def _checks(df: DataFrame, interval_minutes: int, validate_ean_format: bool) -> DataFrame:
    missing_id = F.lit(False)
    for name in IDENTITY:
        missing_id = missing_id | F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)
    # 'EAN' is a free-form identifier here; national code validation belongs in adapters.
    invalid_time = (
        F.col("interval_start_utc").isNull()
        | F.col("interval_end_utc").isNull()
        | (F.col("interval_end_utc") <= F.col("interval_start_utc"))
    )
    seconds = F.unix_timestamp("interval_end_utc") - F.unix_timestamp("interval_start_utc")
    unexpected_duration = seconds != F.lit(interval_minutes * 60)
    bad_energy = F.col("energy_kwh").isNull() | F.isnan(F.col("energy_kwh").cast("double")) | (F.col("energy_kwh") < 0)
    missing_method = F.col("measurement_method").isNull() | (F.length(F.trim("measurement_method")) == 0)
    invalid_ean = (~F.col("ean").rlike(r"^54[0-9]{16}$")) & F.col("ean").isNotNull()
    reasons = F.array(
        F.when(F.lit(validate_ean_format) & invalid_ean, F.lit("INVALID_BELGIAN_EAN_FORMAT")),
        F.when(missing_id, F.lit("MISSING_POINT_IDENTITY")),
        F.when(invalid_time, F.lit("INVALID_INTERVAL")),
        F.when(~invalid_time & unexpected_duration, F.lit("UNEXPECTED_INTERVAL_DURATION")),
        F.when(bad_energy, F.lit("INVALID_ENERGY_VALUE")),
        F.when(missing_method, F.lit("MISSING_MEASUREMENT_METHOD")),
    )
    return df.withColumn("quality_errors", F.filter(reasons, lambda x: x.isNotNull()))


def _payload_hash() -> F.Column:
    # Hash only business fields: repeated delivery of an identical observation is idempotent.
    return F.sha2(
        F.to_json(F.struct(
            F.col("energy_kwh").cast("string").alias("kwh"),
            F.col("measurement_method").alias("method"),
        ), options={"ignoreNullFields": "false"}),
        256,
    )


def process_metering(
    readings: DataFrame, scopes: DataFrame | None = None, *,
    interval_minutes: int = 15, validate_ean_format: bool = True
) -> MeteringQualityResult:
    """Validate versioned interval readings without discarding contradictory evidence.

    Every distinct revision is retained in ``superseded`` or ``current``.
    For a given interval, the highest revision timestamp wins only if its content
    is unambiguous. Conflicting observations tied at that timestamp are rejected.
    A repeated, byte-equivalent business observation is treated as a re-delivery.
    ``changes`` lists observed revision-to-revision payload changes; not invoices.

    ``scopes`` is an optional caller-provided point/register coverage contract.
    Gaps are never inferred for dates without explicit coverage scope.
    """
    if interval_minutes <= 0 or 1440 % interval_minutes:
        raise ValueError("interval_minutes must be a positive divisor of 1440")
    _check_columns(readings, REQUIRED, "readings")
    _check_types(readings)

    checked = _checks(readings, interval_minutes, validate_ean_format).withColumn("_payload_hash", _payload_hash())
    invalid = checked.where(F.size("quality_errors") > 0)
    valid = checked.where(F.size("quality_errors") == 0)

    # Re-deliveries with the same revision and values are harmless. We remove
    # only exact business duplicates, leaving conflicting values visible.
    versions = valid.dropDuplicates([*KEY, "revision_at_utc", "_payload_hash"])
    # A revision key makes null-safe equality practical for Spark joins.
    # Preserve microseconds and ensure the source session uses UTC.
    versions = versions.withColumn(
        "_revision_key",
        F.coalesce(F.date_format("revision_at_utc", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), F.lit("__null__")),
    )
    conflicted_versions = versions.groupBy(*KEY, "_revision_key").agg(
        F.countDistinct("_payload_hash").alias("_variants")
    ).where(F.col("_variants") > 1).select(*KEY, "_revision_key")
    conflict_rows = versions.join(conflicted_versions, [*KEY, "_revision_key"], "inner").withColumn(
        "quality_errors", F.array(F.lit("CONFLICTING_REVISION"))
    )
    safe_versions = versions.join(conflicted_versions, [*KEY, "_revision_key"], "left_anti")

    # Determine the winning revision from ALL version claims. If the newest
    # claim is conflicted, do not silently fall back to an older value.
    latest_revision = versions.groupBy(*KEY).agg(F.max("revision_at_utc").alias("_max_revision"))
    annotated = safe_versions.join(latest_revision, list(KEY), "inner")
    is_latest = F.col("revision_at_utc").eqNullSafe(F.col("_max_revision"))
    current = annotated.where(is_latest)
    superseded = annotated.where(~is_latest)

    # Capture changes from one valid revision to the next; tied conflicting
    # revisions are deliberately not treated as a deterministic sequence.
    order = Window.partitionBy(*KEY).orderBy(F.col("revision_at_utc").asc_nulls_first())
    prior = annotated.withColumn("previous_kwh", F.lag("energy_kwh").over(order))
    prior = prior.withColumn("previous_method", F.lag("measurement_method").over(order))
    changes = prior.where(
        F.col("previous_kwh").isNotNull()
        & ((F.col("previous_kwh") != F.col("energy_kwh"))
           | (F.col("previous_method") != F.col("measurement_method")))
    ).select(*KEY, "revision_at_utc", "previous_kwh", "energy_kwh", "previous_method", "measurement_method")

    # Remove working columns from published datasets.
    hidden = ("_payload_hash", "_max_revision", "_variants", "_revision_key")
    def visible(df: DataFrame) -> DataFrame:
        return df.drop(*hidden)

    rejected = invalid.unionByName(conflict_rows, allowMissingColumns=True)
    coverage = coverage_issues(current, scopes) if scopes is not None else None
    return MeteringQualityResult(
        current=_enrich(visible(current), interval_minutes),
        rejected=visible(rejected),
        superseded=_enrich(visible(superseded), interval_minutes),
        changes=changes, coverage=coverage,
    )


def coverage_issues(current: DataFrame, scopes: DataFrame) -> DataFrame:
    """Find missing/overlapping time windows strictly within declared scopes.

    `valid_from_utc` is inclusive, `valid_to_utc` exclusive. No row generation:
    uses window functions over existing readings plus two boundary comparisons.
    Multiple scopes for the same point/register are supported via scope_id;
    overlapping scope definitions are the caller's responsibility.
    """
    _check_columns(scopes, SCOPE_REQUIRED, "scopes")
    for c in ("valid_from_utc", "valid_to_utc"):
        if not isinstance(scopes.schema[c].dataType, TimestampType):
            raise TypeError(f"{c} must be Spark TimestampType (UTC)")
    point = current.select(*KEY)
    # Only readings completely inside the declared period are considered.
    within = scopes.alias("s").join(
        point.alias("r"),
        (F.col("s.ean") == F.col("r.ean"))
        & (F.col("s.meter_id") == F.col("r.meter_id"))
        & (F.col("s.register") == F.col("r.register"))
        & (F.col("r.interval_start_utc") >= F.col("s.valid_from_utc"))
        & (F.col("r.interval_end_utc") <= F.col("s.valid_to_utc")), "inner",
    ).select(
        F.col("s.scope_id"), *[F.col(f"s.{c}") for c in IDENTITY],
        F.col("s.valid_from_utc"), F.col("s.valid_to_utc"),
        F.col("r.interval_start_utc"), F.col("r.interval_end_utc"),
    )
    w = Window.partitionBy("scope_id", *IDENTITY).orderBy("interval_start_utc", "interval_end_utc")
    previous_end = F.max("interval_end_utc").over(w.rowsBetween(Window.unboundedPreceding, -1))
    interior = within.withColumn("_previous_end", previous_end).where(F.col("_previous_end").isNotNull())
    gaps = interior.where(F.col("interval_start_utc") > F.col("_previous_end")).select(
        "scope_id", *IDENTITY,
        F.col("_previous_end").alias("window_from_utc"),
        F.col("interval_start_utc").alias("window_to_utc"),
        F.lit("GAP").alias("issue"),
    )
    overlaps = interior.where(F.col("interval_start_utc") < F.col("_previous_end")).select(
        "scope_id", *IDENTITY,
        F.col("interval_start_utc").alias("window_from_utc"),
        F.col("_previous_end").alias("window_to_utc"),
        F.lit("OVERLAP").alias("issue"),
    )
    edge = within.groupBy("scope_id", *IDENTITY).agg(
        F.min("interval_start_utc").alias("first_start"),
        F.max("interval_end_utc").alias("last_end"),
    )
    boundary = scopes.join(edge, ["scope_id", *IDENTITY], "left")
    first = boundary.where(F.col("first_start").isNotNull() & (F.col("first_start") > F.col("valid_from_utc"))).select(
        "scope_id", *IDENTITY, F.col("valid_from_utc").alias("window_from_utc"),
        F.col("first_start").alias("window_to_utc"), F.lit("GAP_AT_START").alias("issue"),
    )
    last = boundary.where(F.col("last_end").isNotNull() & (F.col("last_end") < F.col("valid_to_utc"))).select(
        "scope_id", *IDENTITY, F.col("last_end").alias("window_from_utc"),
        F.col("valid_to_utc").alias("window_to_utc"), F.lit("GAP_AT_END").alias("issue"),
    )
    empty = boundary.where(F.col("first_start").isNull()).select(
        "scope_id", *IDENTITY, F.col("valid_from_utc").alias("window_from_utc"),
        F.col("valid_to_utc").alias("window_to_utc"), F.lit("NO_READINGS").alias("issue"),
    )
    return gaps.unionByName(overlaps).unionByName(first).unionByName(last).unionByName(empty)


def _enrich(df: DataFrame, interval_minutes: int) -> DataFrame:
    """Derive analytic attributes; UTC keys remain authoritative across DST."""
    start_local = F.from_utc_timestamp("interval_start_utc", "Europe/Brussels")
    return (
        df.withColumn("local_date_be", F.to_date(start_local))
        .withColumn("local_month_be", F.date_format(start_local, "yyyy-MM"))
        .withColumn(
            "avg_power_kw",
            (F.col("energy_kwh") * F.lit(60.0 / interval_minutes)).cast(DecimalType(24, 6)),
        )
    )
