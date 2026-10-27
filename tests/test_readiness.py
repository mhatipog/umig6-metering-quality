"""Legacy readiness rules use entirely synthetic, non-customer data."""
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

from umig6_metering_quality import evaluate_readiness, SCENARIOS
from conftest import EAN


def _dataset(spark, columns, rows):
    schema = StructType([StructField(n,t,True) for n,t in columns])
    return spark.createDataFrame(rows,schema)


def test_scenarios_are_explicit():
    assert set(SCENARIOS) == {"METERING_REPLAY", "ASSET_HISTORY", "SUPPLIER_CHANGE"}


def test_legacy_missing_schema_becomes_blocker(spark):
    df = _dataset(spark, [("ean", StringType())], [(EAN,), ("541234567890123456",)])
    res = evaluate_readiness(df, "SUPPLIER_CHANGE")
    assert res.quarantine.count() == 2
    assert "MISSING_COLUMN_SUPPLIER_ID" in res.quarantine.first().readiness_blockers
    assert res.summary.first().rows_blocked == 2


def test_invalid_legacy_dates_are_not_silently_cast(spark):
    df = _dataset(spark, [("ean",StringType()),("asset_id",StringType()),("meter_id",StringType()),
          ("valid_from_utc",StringType()),("valid_to_utc",StringType())],
          [(EAN,"asset-a","meter-a","2026-01-01","2026-01-02")])
    result = evaluate_readiness(df,"ASSET_HISTORY")
    assert "WRONG_TYPE_VALID_FROM_UTC" in result.quarantine.first().readiness_blockers


def test_metering_replay_structural_readiness(spark, utc):
    from datetime import timedelta
    t = utc()
    df = _dataset(spark,[("ean",StringType()),("meter_id",StringType()),("register",StringType()),
                  ("interval_start_utc",TimestampType()),("interval_end_utc",TimestampType()),
                  ("energy_kwh",DecimalType(18,6))],
            [(EAN,"M1","A+",t,t+timedelta(minutes=15),Decimal("1.000000")),
             (EAN,"M1","A+",t,t+timedelta(minutes=60),Decimal("1.000000"))])
    result = evaluate_readiness(df, "METERING_REPLAY")
    assert result.ready.count() == 1
    assert result.quarantine.count() == 1
    assert "NOT_QUARTER_HOURLY" in result.quarantine.first().readiness_blockers
    assert result.summary.first().structural_readiness_pct == 50.0
    assert "UNAVAILABLE_REVISION_AT_UTC" in result.ready.first().readiness_warnings


def test_unknown_scenario_fails_explicitly(spark):
    import pytest
    with pytest.raises(ValueError,match="Unknown scenario"):
        evaluate_readiness(spark.range(2), "CERTIFIED_MIG6")


def test_current_supplier_contract_allows_open_end(spark, utc):
    df = _dataset(spark, [("ean",StringType()),("supplier_id",StringType()),
             ("contract_start_utc",TimestampType()),("contract_end_utc",TimestampType())],
          [(EAN,"SUP01",utc(),None)])
    result = evaluate_readiness(df,"SUPPLIER_CHANGE")
    assert result.ready.count() == 1


def test_numeric_legacy_ean_is_blocked_even_if_digits_look_correct(spark, utc):
    # Legacy numeric keys lose leading zeros and cannot safely be treated as EAN strings.
    from pyspark.sql.types import LongType
    df = _dataset(spark, [("ean",LongType()), ("supplier_id",StringType()),
          ("contract_start_utc",TimestampType()),("contract_end_utc",TimestampType())],
          [(541234567890123456, "SUP01", utc(), None)])
    res = evaluate_readiness(df, "SUPPLIER_CHANGE")
    assert "WRONG_TYPE_EAN" in res.quarantine.first().readiness_blockers
