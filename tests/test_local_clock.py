"""DST clock-contract regression tests (synthetic observations only)."""
import calendar
from datetime import date, timedelta

import pytest
from pyspark.sql.types import StructField, StructType, StringType
from umig6_metering_quality.public_profiles import screen_public_profiles
from umig6_metering_quality.local_clock import _last_sunday, profile_local_clock_coverage

COLUMNS = ("EAN_ID", "Datum", "Datum_Startuur", "Volume_Afname_KWh",
           "Volume_Injectie_KWh", "Warmtepomp_Indicator",
           "Elektrisch_Voertuig_Indicator", "PV_Installatie_Indicator", "Contract_Categorie")


def _checked(spark, rows):
    values = [(point, date_str, instant, "0.4", "0", "0", "0", "0", "residential")
              for point, date_str, instant in rows]
    df = spark.createDataFrame(values, StructType([StructField(c, StringType(), True) for c in COLUMNS]))
    return screen_public_profiles(df)


def _year(year, *, missing=None, extra=()):
    spring, autumn = _last_sunday(year, 3), _last_sunday(year, 10)
    start = date(year, 1, 1)
    output = []
    for i in range(366 if calendar.isleap(year) else 365):
        day = start + timedelta(days=i)
        for quarter in range(96):
            hour, minute = divmod(quarter * 15, 60)
            if day == spring and hour == 2:
                continue
            repeats = 2 if day == autumn and hour == 2 else 1
            for _ in range(repeats):
                key = (day.isoformat(), f"{hour:02d}:{minute:02d}")
                if key == missing:
                    continue
                output.append(("profile", day.isoformat(), f"{day}T{hour:02d}:{minute:02d}:00.000Z"))
    return output + list(extra)


def test_2024_dst_boundaries():
    assert _last_sunday(2024, 3).isoformat() == "2024-03-31"
    assert _last_sunday(2024, 10).isoformat() == "2024-10-27"


def test_complete_wall_clock_2024_is_not_proven_utc(spark):
    result = profile_local_clock_coverage(_checked(spark, _year(2024))).first()
    assert result.observed_days == 366
    assert result.observed_rows == 35136
    assert result.local_clock_coverage_status == "COMPLETE_LOCAL_CLOCK"
    assert result.utc_instant_status == "NOT_ESTABLISHED_BY_LOCAL_CLOCK_CHECK"


def test_missing_quarter_is_not_complete(spark):
    result = profile_local_clock_coverage(
        _checked(spark, _year(2024, missing=("2024-07-01", "03:15")))
    ).first()
    assert result.local_clock_coverage_status == "INCOMPLETE_LOCAL_CLOCK"


def test_duplicate_does_not_mask_missing_quarter(spark):
    extra = [("profile", "2024-07-01", "2024-07-01T03:30:00.000Z")]
    result = profile_local_clock_coverage(
        _checked(spark, _year(2024, missing=("2024-07-01", "03:15"), extra=extra))
    ).first()
    assert result.observed_rows == 35136
    assert result.local_clock_coverage_status == "INCOMPLETE_LOCAL_CLOCK"


def test_uneven_autumn_repeats_are_invalid(spark):
    rows = [("profile", "2024-10-27", f"2024-10-27T02:{m:02d}:00.000Z")
            for m in (0, 0, 0, 15, 30, 30, 45, 45)]
    result = profile_local_clock_coverage(_checked(spark, rows)).first()
    assert result.incomplete_days > 0
    assert result.local_clock_coverage_status == "INCOMPLETE_LOCAL_CLOCK"


def test_invalid_year_is_rejected(spark):
    with pytest.raises(ValueError, match="year"):
        profile_local_clock_coverage(_checked(spark, _year(2024)), year=1800)
