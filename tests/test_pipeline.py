from datetime import timedelta

from umig6_metering_quality import process_metering
from conftest import EAN, METER


def _count(result):
    return (result.current.count(), result.rejected.count(), result.superseded.count(), result.changes.count())


def test_valid_zero_interval_is_not_missing(readings, make_reading, utc):
    result = process_metering(readings([make_reading(utc(), "0.000000")]))
    assert _count(result) == (1, 0, 0, 0)


def test_negative_and_null_values_are_rejected(readings, make_reading, utc):
    t = utc()
    result = process_metering(readings([
        make_reading(t, "-1.000000"), make_reading(t + timedelta(minutes=15), None),
    ]))
    assert result.current.count() == 0
    assert result.rejected.count() == 2
    assert all("INVALID_ENERGY_VALUE" in row.quality_errors for row in result.rejected.collect())


def test_invalid_period_and_wrong_granularity_are_rejected(readings, make_reading, utc):
    t = utc()
    result = process_metering(readings([
        make_reading(t, end=t - timedelta(minutes=15)),
        make_reading(t + timedelta(hours=1), end=t + timedelta(hours=2)),
    ]))
    assert result.current.count() == 0
    errors = [row.quality_errors for row in result.rejected.collect()]
    assert any("INVALID_INTERVAL" in x for x in errors)
    assert any("UNEXPECTED_INTERVAL_DURATION" in x for x in errors)


def test_identical_redelivery_is_idempotent(readings, make_reading, utc):
    row = make_reading(utc(), "0.240000", revision=utc(2026, 1, 3))
    result = process_metering(readings([row, row]))
    assert _count(result) == (1, 0, 0, 0)


def test_newest_revision_wins_and_delta_is_recorded(readings, make_reading, utc):
    t = utc()
    result = process_metering(readings([
        make_reading(t, "0.200000", revision=utc(2026, 1, 3), method="ESTIMATED"),
        make_reading(t, "0.700000", revision=utc(2026, 1, 4), method="MEASURED"),
    ]))
    assert _count(result) == (1, 0, 1, 1)
    assert str(result.current.first().energy_kwh) == "0.700000"
    change = result.changes.first()
    assert str(change.previous_kwh) == "0.200000"
    assert change.previous_method == "ESTIMATED"


def test_conflicting_latest_revision_is_quarantined(readings, make_reading, utc):
    t = utc()
    at = utc(2026, 1, 3)
    result = process_metering(readings([
        make_reading(t, "0.100000", revision=at),
        make_reading(t, "0.900000", revision=at),
    ]))
    assert result.current.count() == 0
    assert result.rejected.count() == 2
    assert all("CONFLICTING_REVISION" in row.quality_errors for row in result.rejected.collect())


def test_null_revision_is_lower_priority(readings, make_reading, utc):
    t = utc()
    result = process_metering(readings([
        make_reading(t, "0.100000"),
        make_reading(t, "0.200000", revision=utc(2026, 1, 3)),
    ]))
    assert str(result.current.first().energy_kwh) == "0.200000"


def test_gap_only_with_declared_scope(readings, make_reading, scopes, utc):
    t = utc()
    data = readings([
        make_reading(t), make_reading(t + timedelta(minutes=30)),
    ])
    r = process_metering(data)
    assert r.coverage is None
    coverage = scopes([("active", EAN, METER, "A+", t, t + timedelta(minutes=45))])
    issues = process_metering(data, coverage).coverage.collect()
    assert len(issues) == 1
    assert issues[0].issue == "GAP"
    assert issues[0].window_from_utc == t + timedelta(minutes=15)


def test_scope_edges_and_empty_scopes(readings, make_reading, scopes, utc):
    t = utc()
    s = scopes([
        ("partial", EAN, METER, "A+", t, t + timedelta(hours=1)),
        ("empty", EAN, METER, "A-", t, t + timedelta(hours=1)),
    ])
    got = process_metering(readings([make_reading(t + timedelta(minutes=15))]), s).coverage
    assert set(row.issue for row in got.collect()) == {"GAP_AT_START", "GAP_AT_END", "NO_READINGS"}


def test_overlapping_intervals_are_reported(readings, make_reading, scopes, utc):
    t = utc()
    # Different register periods overlap while each period has valid length.
    data = readings([
        make_reading(t), make_reading(t + timedelta(minutes=10)),
    ])
    scope = scopes([("active", EAN, METER, "A+", t, t + timedelta(minutes=30))])
    assert "OVERLAP" in [x.issue for x in process_metering(data, scope).coverage.collect()]


def test_dst_spring_day_has_92_utc_quarters(readings, make_reading, scopes, utc):
    # Europe/Brussels: 2026-03-29 is only 23 hours long.
    start = utc(2026, 3, 28, 23)
    data = readings([make_reading(start + timedelta(minutes=15*i)) for i in range(92)])
    scope = scopes([("spring", EAN, METER, "A+", start, start + timedelta(hours=23))])
    result = process_metering(data, scope)
    assert result.current.count() == 92
    assert result.coverage.count() == 0


def test_dst_autumn_day_has_100_utc_quarters(readings, make_reading, scopes, utc):
    # Europe/Brussels: 2026-10-25 is 25 hours long.
    start = utc(2026, 10, 24, 22)
    data = readings([make_reading(start + timedelta(minutes=15*i)) for i in range(100)])
    scope = scopes([("autumn", EAN, METER, "A+", start, start + timedelta(hours=25))])
    result = process_metering(data, scope)
    assert result.current.count() == 100
    assert result.coverage.count() == 0


def test_schema_failure_is_explicit(spark):
    try:
        process_metering(spark.createDataFrame([(1,)], ["a"]))
    except ValueError as exc:
        assert "missing required columns" in str(exc)
    else:
        raise AssertionError("Expected schema contract failure")


def test_ambiguous_historical_version_does_not_invent_change(readings, make_reading, utc):
    t = utc()
    early, late = utc(2026, 1, 3), utc(2026, 1, 4)
    result = process_metering(readings([
        make_reading(t, "0.100000", revision=early),
        make_reading(t, "0.200000", revision=early),
        make_reading(t, "0.300000", revision=late),
    ]))
    assert result.rejected.count() == 2
    assert str(result.current.first().energy_kwh) == "0.300000"
    assert result.changes.count() == 0


def test_conflicting_latest_does_not_fall_back_to_old_revision(readings, make_reading, utc):
    t = utc()
    result = process_metering(readings([
        make_reading(t, "0.100000", revision=utc(2026, 1, 3)),
        make_reading(t, "0.200000", revision=utc(2026, 1, 4)),
        make_reading(t, "0.300000", revision=utc(2026, 1, 4)),
    ]))
    assert result.current.count() == 0
    assert result.rejected.count() == 2
    assert result.superseded.count() == 1


def test_belgian_ean_format_is_checked_but_can_be_disabled(readings, make_reading, utc):
    data = readings([make_reading(utc(), ean="INVALID")])
    assert "INVALID_BELGIAN_EAN_FORMAT" in process_metering(data).rejected.first().quality_errors
    assert process_metering(data, validate_ean_format=False).current.count() == 1


def test_silver_enrichment_preserves_utc_and_derives_local_month(readings, make_reading, utc):
    result = process_metering(readings([make_reading(utc(), "0.250000")]))
    record = result.current.first()
    assert record.local_month_be == "2026-01"
    assert str(record.avg_power_kw) == "1.000000"
