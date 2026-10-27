"""Reference-history validation and temporal join behaviour."""
from datetime import timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from umig6_metering_quality import validate_asset_history, reconcile_metering_assets
from conftest import EAN, METER


def _assets(spark, rows):
    names = ["ean", "asset_id", "meter_id", "valid_from_utc", "valid_to_utc"]
    types = [StringType(),StringType(),StringType(),TimestampType(),TimestampType()]
    return spark.createDataFrame(rows,StructType([StructField(n,t,True) for n,t in zip(names,types)]))


def test_asset_invalid_period_goes_to_quarantine(spark, utc):
    t = utc()
    result = validate_asset_history(_assets(spark, [(EAN,"A",METER,t,t), (EAN,"A2",METER,t,None)]))
    assert result.accepted.count() == 1
    assert "INVALID_ASSET_PERIOD" in result.rejected.first().quality_errors


def test_overlapping_meter_assignments_are_review_not_hard_rejection(spark, utc):
    t=utc()
    data=_assets(spark,[(EAN,"a",METER,t,None),(EAN,"b",METER,t+timedelta(days=1),None)])
    result=validate_asset_history(data)
    assert result.accepted.count()==2
    assert result.rejected.count()==0
    assert result.overlaps_for_review.count()==1


def test_cross_dataset_missing_and_ambiguous_asset_matches(spark,readings,make_reading,utc):
    t=utc()
    observed=readings([make_reading(t),make_reading(t+timedelta(hours=1))])
    data=_assets(spark,[(EAN,"asset1",METER,t,t+timedelta(minutes=15)),
                        (EAN,"asset2",METER,t,t+timedelta(minutes=15))])
    got=reconcile_metering_assets(observed,data)
    issues={x.interval_start_utc:x.asset_reference_issues for x in got.collect()}
    assert issues[t]==["MULTIPLE_ASSET_ASSIGNMENTS"]
    assert issues[t+timedelta(hours=1)]==["NO_MATCHING_ASSET_ASSIGNMENT"]


def test_asset_validation_schema_error(spark):
    import pytest
    with pytest.raises(ValueError,match="missing required columns"):
        validate_asset_history(spark.range(1))
