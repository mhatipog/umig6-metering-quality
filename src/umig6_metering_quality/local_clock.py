"""Exact local-clock coverage for Fluvius P6269 profiles.

Some public profiles have UTC-looking Z labels but follow Brussels DST
wall-clock rules. Verify the raw local labels without inventing UTC offsets.
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta

from pyspark.sql import DataFrame, functions as F
from .public_profiles import PublicProfilesResult


def _last_sunday(year: int, month: int) -> date:
    last = date(year, month, calendar.monthrange(year, month)[1])
    return last - timedelta(days=(last.weekday() + 1) % 7)


def profile_local_clock_coverage(checked: PublicProfilesResult, *, year: int = 2024) -> DataFrame:
    """Verify every clock label; autumn 02:xx appears twice, spring 02:xx never.

    Deliberately separate from strict UTC coverage. A complete local calendar
    does not establish a unique UTC instant for the repeated October hour.
    Runs entirely with Spark aggregates and without Python UDFs.
    """
    if not 1900 <= year <= 2100:
        raise ValueError("year must be between 1900 and 2100")
    spring, autumn = (_last_sunday(year, month).isoformat() for month in (3, 10))
    expected_days = 366 if calendar.isleap(year) else 365

    source = checked.all_checked
    raw_time = F.trim("Datum_Startuur")
    label_day = F.substring(raw_time, 1, 10)
    clock_slot = F.regexp_extract(
        raw_time, r"^\\d{4}-\\d{2}-\\d{2}T(\\d{2}:\\d{2}):\\d{2}(?:\\.\\d+)?Z$", 1
    )
    clock_pattern = r"^(?:[01]\\d|2[0-3]):(?:00|15|30|45)$"
    day = F.to_date("Datum", "yyyy-MM-dd")
    input_rows = source.select(
        F.col("profile_id"),
        day.alias("source_day"),
        clock_slot.alias("clock_slot"),
        F.when(
            (~clock_slot.rlike(clock_pattern))
            | (label_day != F.col("Datum"))
            | (F.size("quality_errors") > 0)
            | day.isNull(),
            1,
        ).otherwise(0).alias("bad_source"),
    )

    # Exact multiplicity per local clock label: catches a duplicate that
    # masks a missing quarter despite the expected total daily row count.
    slots = input_rows.groupBy("profile_id", "source_day", "clock_slot").agg(
        F.count("*").alias("observed"),
        F.sum("bad_source").alias("bad_source_rows"),
    )
    spring_day = F.lit(spring).cast("date")
    autumn_day = F.lit(autumn).cast("date")
    is_spring_2 = (F.col("source_day") == spring_day) & F.col("clock_slot").startswith("02:")
    is_autumn_2 = (F.col("source_day") == autumn_day) & F.col("clock_slot").startswith("02:")
    expected_repeats = F.when(is_spring_2, 0).when(is_autumn_2, 2).otherwise(1)
    bad_slot = (
        (~F.col("clock_slot").rlike(clock_pattern))
        | (F.year("source_day") != year)
        | F.col("source_day").isNull()
        | (F.col("observed") != expected_repeats)
        | (F.col("bad_source_rows") > 0)
    )
    days = slots.groupBy("profile_id", "source_day").agg(
        F.sum("observed").alias("observed_rows"),
        F.count("*").alias("observed_clock_slots"),
        F.sum(F.when(bad_slot, 1).otherwise(0)).alias("invalid_clock_slots"),
        F.sum("bad_source_rows").alias("invalid_source_rows"),
    )
    expected_rows = (
        F.when(F.col("source_day") == spring_day, 92)
        .when(F.col("source_day") == autumn_day, 100).otherwise(96)
    )
    expected_slots = F.when(F.col("source_day") == spring_day, 92).otherwise(96)
    days = days.withColumn(
        "incomplete_day",
        (F.col("observed_rows") != expected_rows)
        | (F.col("observed_clock_slots") != expected_slots)
        | (F.col("invalid_clock_slots") > 0),
    )
    profiles = days.groupBy("profile_id").agg(
        F.count("*").alias("observed_days"),
        F.sum("observed_rows").alias("observed_rows"),
        F.sum(F.when("incomplete_day", 1).otherwise(0)).alias("incomplete_days"),
        F.sum("invalid_clock_slots").alias("invalid_clock_slots"),
        F.sum("invalid_source_rows").alias("invalid_source_rows"),
        F.min("source_day").alias("first_local_day"),
        F.max("source_day").alias("last_local_day"),
    )
    complete = (
        (F.col("observed_days") == expected_days)
        & (F.col("observed_rows") == expected_days * 96)
        & (F.col("incomplete_days") == 0)
        & (F.col("first_local_day") == F.lit(f"{year}-01-01").cast("date"))
        & (F.col("last_local_day") == F.lit(f"{year}-12-31").cast("date"))
    )
    return (
        profiles.withColumn(
            "local_clock_coverage_status",
            F.when(complete, F.lit("COMPLETE_LOCAL_CLOCK"))
            .otherwise(F.lit("INCOMPLETE_LOCAL_CLOCK")),
        )
        .withColumn("utc_instant_status", F.lit("NOT_ESTABLISHED_BY_LOCAL_CLOCK_CHECK"))
    )
