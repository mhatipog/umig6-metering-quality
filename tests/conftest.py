from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

EAN = "541440000000000000"  # Synthetic identifier, not a real supply point
METER = "TEST-METER-0001"

READING_SCHEMA = StructType([
    StructField("ean", StringType(), True),
    StructField("meter_id", StringType(), True),
    StructField("register", StringType(), True),
    StructField("interval_start_utc", TimestampType(), True),
    StructField("interval_end_utc", TimestampType(), True),
    StructField("energy_kwh", DecimalType(18, 6), True),
    StructField("revision_at_utc", TimestampType(), True),
    StructField("measurement_method", StringType(), True),
])

SCOPE_SCHEMA = StructType([
    StructField("scope_id", StringType(), True),
    StructField("ean", StringType(), True),
    StructField("meter_id", StringType(), True),
    StructField("register", StringType(), True),
    StructField("valid_from_utc", TimestampType(), True),
    StructField("valid_to_utc", TimestampType(), True),
])

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("umig6-metering-quality-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

@pytest.fixture
def make_reading(spark):
    from decimal import Decimal

    def make_reading(start, kwh="0.250000", *, end=None, revision=None, ean=EAN, meter=METER, reg="A+", method="MEASURED"):
        return (ean, meter, reg, start, end or start + timedelta(minutes=15),
                Decimal(kwh) if kwh is not None else None, revision, method)
    return make_reading

@pytest.fixture
def readings(spark):
    def create(rows):
        return spark.createDataFrame(rows, READING_SCHEMA)
    return create

@pytest.fixture
def scopes(spark):
    def create(rows):
        return spark.createDataFrame(rows, SCOPE_SCHEMA)
    return create

@pytest.fixture
def utc():
    def create(y=2026, m=1, d=2, hour=0, minute=0):
        return datetime(y, m, d, hour, minute, tzinfo=timezone.utc).replace(tzinfo=None)
    return create
