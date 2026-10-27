"""Synthetic consumer-export fixtures. No private metering information."""

from datetime import datetime, timezone

import pytest

from umig6_metering_quality import process_metering, read_fluvius_csv
from conftest import EAN, METER

HEADER = "Van Datum;Van Tijdstip;Tot Datum;Tot Tijdstip;EAN;Meter;Metertype;Register;Volume;Eenheid;Validatiestatus"


def row(start_date="02-01-26", start="00:00:00", end_date="02-01-26", end="00:15:00", *,
        register="Afname Totaal", volume="0,250000", status="VAL", ean=EAN):
    return f"{start_date};{start};{end_date};{end};{ean};{METER};DIGITAL;{register};{volume};kWh;{status}"


def export(tmp_path, *rows, header=HEADER):
    p = tmp_path / "export.csv"
    p.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="latin-1")
    return str(p)


def snapshot(day=3):
    return datetime(2026, 1, day, tzinfo=timezone.utc)


def test_fluvius_import_preserves_status_and_provenance(spark, tmp_path):
    result = read_fluvius_csv(spark, export(tmp_path, row()), snapshot_at_utc=snapshot())
    assert result.rejected.count() == 0
    reading = result.readings.first()
    assert reading.register == "A+"
    assert str(reading.energy_kwh) == "0.250000"
    assert reading.interval_start_utc == datetime(2026, 1, 1, 23, 0)
    assert reading.interval_end_utc == datetime(2026, 1, 1, 23, 15)
    assert reading.revision_basis == "EXPORT_SNAPSHOT"
    assert reading.measurement_method == "VAL"
    assert reading.source_register == "Afname Totaal"
    assert reading.source_file.endswith("export.csv")
    assert process_metering(result.readings).current.count() == 1


def test_fluvius_import_quarantines_bad_units_registers_and_volume(spark, tmp_path):
    result = read_fluvius_csv(spark, export(tmp_path,
        row(volume="1.234,56"), row(register="Reactive Register"),
        row().replace(";kWh;", ";kW;")), snapshot_at_utc=snapshot())
    assert result.readings.count() == 0
    codes = [set(r.quality_errors) for r in result.rejected.select("quality_errors").collect()]
    assert any("INVALID_VOLUME" in x for x in codes)
    assert any("UNSUPPORTED_REGISTER" in x for x in codes)
    assert any("UNSUPPORTED_UNIT" in x for x in codes)


def test_fluvius_import_quarantines_autumn_repeated_hour(spark, tmp_path):
    result = read_fluvius_csv(spark, export(tmp_path,
        row("25-10-26", "02:00:00", "25-10-26", "02:15:00")), snapshot_at_utc=snapshot())
    assert result.readings.count() == 0
    assert "AMBIGUOUS_OR_NONEXISTENT_LOCAL_TIMESTAMP" in result.rejected.first().quality_errors


def test_fluvius_import_quarantines_spring_nonexistent_hour(spark, tmp_path):
    result = read_fluvius_csv(spark, export(tmp_path,
        row("29-03-26", "02:15:00", "29-03-26", "02:30:00")), snapshot_at_utc=snapshot())
    assert result.readings.count() == 0
    assert "AMBIGUOUS_OR_NONEXISTENT_LOCAL_TIMESTAMP" in result.rejected.first().quality_errors


def test_fluvius_import_valid_transition_across_spring_gap(spark, tmp_path):
    result = read_fluvius_csv(spark, export(tmp_path,
        row("29-03-26", "01:45:00", "29-03-26", "03:00:00")), snapshot_at_utc=snapshot())
    assert result.rejected.count() == 0
    assert process_metering(result.readings).current.count() == 1


def test_headers_fail_closed(spark, tmp_path):
    path = export(tmp_path, row(), header=HEADER.replace("Volume", "Unknown"))
    with pytest.raises(ValueError, match="missing column for volume"):
        read_fluvius_csv(spark, path, snapshot_at_utc=snapshot())


def test_snapshot_must_be_utc(spark, tmp_path):
    path = export(tmp_path, row())
    with pytest.raises(ValueError, match="UTC timezone"):
        read_fluvius_csv(spark, path, snapshot_at_utc=datetime(2026, 1, 2))


def test_export_snapshot_changes_not_equated_to_official_revisions(spark, tmp_path):
    first = read_fluvius_csv(spark, export(tmp_path, row(volume="0.300000")), snapshot_at_utc=snapshot(3))
    second = read_fluvius_csv(spark, export(tmp_path, row(volume="0.400000")), snapshot_at_utc=snapshot(4))
    result = process_metering(first.readings.unionByName(second.readings))
    assert result.current.count() == 1
    assert result.superseded.count() == 1
    assert result.changes.count() == 1
    assert {r.revision_basis for r in result.current.collect()} == {"EXPORT_SNAPSHOT"}
