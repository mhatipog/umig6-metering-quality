"""Row-wise Bronze screening must not silently discard duplicated events."""
from datetime import timedelta
from umig6_metering_quality import screen_metering_rows


def test_bronze_preserves_source_duplicates(readings, make_reading, utc):
    row = make_reading(utc())
    output = screen_metering_rows(readings([row, row]))
    assert output.accepted.count() == 2
    assert output.quarantine.count() == 0
    assert output.summary.first().rows_screened == 2


def test_bronze_quarantines_invalid_interval(readings, make_reading, utc):
    now = utc()
    bad = make_reading(now, end=now + timedelta(minutes=15, microseconds=1))
    result = screen_metering_rows(readings([make_reading(now), bad]))
    assert result.accepted.count() == 1
    assert result.quarantine.count() == 1
    assert "UNEXPECTED_INTERVAL_DURATION" in result.quarantine.first().quality_errors


def test_bronze_checks_ean_before_enrichment(readings, make_reading, utc):
    result = screen_metering_rows(readings([make_reading(utc(), ean="bad")]))
    assert result.accepted.count() == 0
    assert "INVALID_BELGIAN_EAN_FORMAT" in result.quarantine.first().quality_errors


def test_bronze_does_not_enrich_unverified_utc_claim(readings, make_reading, utc):
    # Local month and power outputs belong after the provenance gate.
    data = screen_metering_rows(readings([make_reading(utc())]))
    assert "local_date_be" not in data.accepted.columns
    assert "avg_power_kw" not in data.accepted.columns


def test_legacy_without_source_revision_can_still_be_screened(spark, readings, make_reading, utc):
    legacy = readings([make_reading(utc())]).drop("revision_at_utc")
    result = screen_metering_rows(legacy)
    assert result.accepted.count() == 1
    assert "revision_at_utc" in result.accepted.columns
    assert result.accepted.first().revision_at_utc is None
