"""Fluvius P6269 public 2024 profiles: read-only, consumer-data benchmark adapter.

This is NOT an official UMIG/MIG6 message format. EAN_ID is an anonymised
profile label, not proof of a real supply point or a physical meter ID.

Prefer the source's ISO-8601 UTC timestamps (Datum_Startuur with Z). For
local-time-only variants, the repeated fall-back 02:00 hour is ambiguous and
quarantined rather than silently shifted.
"""

from __future__ import annotations

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DATASET_ID = "1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar"
PUBLIC_COLUMNS = (
    "EAN_ID", "Datum", "Datum_Startuur", "Volume_Afname_KWh",
    "Volume_Injectie_KWh", "Warmtepomp_Indicator",
    "Elektrisch_Voertuig_Indicator", "PV_Installatie_Indicator", "Contract_Categorie",
)


@dataclass(frozen=True)
class PublicProfilesResult:
    valid: DataFrame
    quarantine: DataFrame
    all_checked: DataFrame


def read_public_profiles(spark, path: str) -> DataFrame:
    """Load official P6269 CSV, preserving text until checks have run."""
    df = (spark.read.option("header", "true").option("sep", ",")
          .option("encoding", "UTF-8").option("mode", "FAILFAST")
          .csv(path))
    missing = sorted(set(PUBLIC_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError("Unexpected Fluvius P6269 CSV layout; missing " + ", ".join(missing))
    return df.select(*PUBLIC_COLUMNS)


def _local_timestamp(df: DataFrame):
    s = F.concat(F.trim(F.col("Datum")), F.lit(" "), F.trim(F.col("Datum_Startuur")))
    return F.coalesce(*(
        F.try_to_timestamp(s, F.lit(fmt)) for fmt in (
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy HH:mm", "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy HH:mm",
            "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm",
        )
    ))


def screen_public_profiles(df: DataFrame) -> PublicProfilesResult:
    """Apply deterministic native-Spark checks in one source-grain projection.

    Separates unexpected data from non-resolvable DST local times and avoids
    materialising the two directional measures as twice as many rows.
    """
    missing = sorted(set(PUBLIC_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError("Missing P6269 columns: " + ", ".join(missing))
    raw_start = F.trim(F.col("Datum_Startuur"))
    is_explicit = raw_start.rlike(r"(?i)(Z|[+-][0-9]{2}:[0-9]{2})$")
    # Original P6269 files use UTC ISO-8601 timestamps, e.g.
    # 2024-01-01T00:15:00.000Z. Do NOT reinterpret those as Belgian wall time.
    explicit_utc = F.try_to_timestamp(raw_start)
    local_from_date = _local_timestamp(df)
    interval_utc = F.when(is_explicit, explicit_utc).otherwise(
        F.to_utc_timestamp(local_from_date, "Europe/Brussels")
    )
    local = F.when(is_explicit, F.from_utc_timestamp(explicit_utc, "Europe/Brussels")).otherwise(local_from_date)
    # EAN_ID is an anonymised profile ID; never validate it as a Belgian EAN.
    value_import = F.try_cast(F.regexp_replace(F.trim(F.col("Volume_Afname_KWh")), ",", "."), "decimal(24,9)") if hasattr(F, 'try_cast') else F.expr("try_cast(replace(trim(Volume_Afname_KWh), ',', '.') as decimal(24,9))")
    value_export = F.expr("try_cast(replace(trim(Volume_Injectie_KWh), ',', '.') as decimal(24,9))")
    last_march = F.last_day(F.make_date(F.year(local), F.lit(3), F.lit(1)))
    last_october = F.last_day(F.make_date(F.year(local), F.lit(10), F.lit(1)))
    spring_sunday = F.date_sub(last_march, F.dayofweek(last_march) - F.lit(1))
    autumn_sunday = F.date_sub(last_october, F.dayofweek(last_october) - F.lit(1))
    spring_gap = (~is_explicit) & (F.to_date(local) == spring_sunday) & (F.hour(local) == 2)
    autumn_repeat = (~is_explicit) & (F.to_date(local) == autumn_sunday) & (F.hour(local) == 2)
    invalid_step = (F.minute(interval_utc) % F.lit(15) != 0) | (F.second(interval_utc) != 0)
    errors = F.filter(F.array(
        F.when(F.col("EAN_ID").isNull() | (F.length(F.trim("EAN_ID")) == 0), F.lit("MISSING_PUBLIC_PROFILE_ID")),
        F.when(interval_utc.isNull(), F.lit("INVALID_SOURCE_DATETIME")),
        F.when(spring_gap, F.lit("NONEXISTENT_BELGIAN_LOCAL_HOUR")),
        F.when(autumn_repeat, F.lit("AMBIGUOUS_BELGIAN_LOCAL_HOUR")),
        F.when(invalid_step, F.lit("UNALIGNED_QUARTER_HOUR")),
        F.when(value_import.isNull() | (value_import < 0), F.lit("INVALID_IMPORT_KWH")),
        F.when(value_export.isNull() | (value_export < 0), F.lit("INVALID_INJECTION_KWH")),
    ), lambda x: x.isNotNull())
    checked = (df.withColumn("profile_id", F.col("EAN_ID"))
        .withColumn("local_start_be", local)
        .withColumn("import_kwh", value_import)
        .withColumn("injection_kwh", value_export)
        .withColumn("quality_errors", errors)
        .withColumn("interval_start_utc", interval_utc)
        .withColumn("interval_end_utc", F.col("interval_start_utc") + F.expr("INTERVAL 15 MINUTES")))
    valid = checked.where(F.size("quality_errors") == 0)
    quarantine = checked.where(F.size("quality_errors") > 0)
    return PublicProfilesResult(valid, quarantine, checked)


def public_profile_metrics(result: PublicProfilesResult) -> DataFrame:
    """One aggregate action for large raw data; avoids expensive distinct/window jobs."""
    df = result.all_checked
    ok = F.size("quality_errors") == 0
    return df.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.when(ok, 1).otherwise(0)).alias("accepted_rows"),
        F.sum(F.when(~ok, 1).otherwise(0)).alias("quarantined_rows"),
        F.sum(F.when(ok, F.col("import_kwh")).otherwise(F.lit(0))).alias("accepted_import_kwh"),
        F.sum(F.when(ok, F.col("injection_kwh")).otherwise(F.lit(0))).alias("accepted_injection_kwh"),
        F.sum(F.when(F.array_contains("quality_errors", "AMBIGUOUS_BELGIAN_LOCAL_HOUR"), 1).otherwise(0)).alias("ambiguous_dst_rows"),
        F.sum(F.when(F.array_contains("quality_errors", "NONEXISTENT_BELGIAN_LOCAL_HOUR"), 1).otherwise(0)).alias("nonexistent_dst_rows"),
        F.sum(F.when(F.array_contains("quality_errors", "INVALID_IMPORT_KWH") | F.array_contains("quality_errors", "INVALID_INJECTION_KWH"), 1).otherwise(0)).alias("invalid_volume_rows"),
    )


def profile_coverage(
    checked: PublicProfilesResult, *, expected_intervals: int = 35136,
    expected_start_utc: str = "2024-01-01 00:00:00",
    expected_end_utc: str = "2024-12-31 23:45:00",
) -> DataFrame:
    """Exact year-level coverage per anonymous profile, including duplicates.

    The default scope is the official 2024 leap year (366 * 96 UTC quarters).
    Counting distinct intervals prevents a duplicate from hiding a missing slot.
    This is a shuffled analysis and should be benchmarked separately from
    the single-pass source-row screening.
    """
    if expected_intervals <= 0:
        raise ValueError("expected_intervals must be positive")
    df = checked.all_checked
    counts = df.groupBy("profile_id").agg(
        F.count("*").alias("observed_rows"),
        F.countDistinct("interval_start_utc").alias("unique_utc_quarters"),
        F.sum(F.when(F.size("quality_errors") > 0, 1).otherwise(0)).alias("quarantined_rows"),
        F.min("interval_start_utc").alias("first_utc"),
        F.max("interval_start_utc").alias("last_utc"),
    )
    start = F.to_timestamp(F.lit(expected_start_utc))
    end = F.to_timestamp(F.lit(expected_end_utc))
    return counts.withColumn("coverage_status", F.when(
        (F.col("observed_rows") == expected_intervals)
        & (F.col("unique_utc_quarters") == expected_intervals)
        & (F.col("quarantined_rows") == 0)
        & (F.col("first_utc") == start)
        & (F.col("last_utc") == end),
        F.lit("COMPLETE")
    ).otherwise(F.lit("INCOMPLETE")))
