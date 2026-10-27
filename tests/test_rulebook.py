from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, LongType, StructField, StructType

from umig6_metering_quality.rulebook import (
    PROFILES, UMIG_ME_6522_PREREQUISITES, screen_continuous_measures,
)


def test_rulebook_is_pinned_and_explicit():
    p = PROFILES[UMIG_ME_6522_PREREQUISITES.profile_id]
    assert p.source_version == "6.5.2.2"
    assert "5.1.2" in p.section


def _frame(spark, rows):
    schema = StructType([
        StructField("energy_kwh", DecimalType(18, 6), True),
        StructField("observed_digits", LongType(), True),
        StructField("expected_digits", LongType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def test_present_zero_is_valid_but_missing_digit_evidence_is_not_pass(spark):
    df = _frame(spark, [(Decimal("0.000000"), None, None), (None, None, None)])
    r = screen_continuous_measures(df)
    assert [x.screen_status for x in r.evaluated.orderBy("ME_CONTINUOUS_VALUE_PRESENT").collect()] == ["FAIL", "INCOMPLETE"]
    assert r.quarantine.count() == 1
    assert r.review.count() == 1


def test_comparable_digit_counts_and_disagreement(spark):
    df = _frame(spark, [
        (Decimal("0.250000"), 6, 6),
        (Decimal("0.500000"), 5, 6),
        (Decimal("0.700000"), None, 6),
    ])
    r = screen_continuous_measures(df, observed_digits_column="observed_digits", expected_digits_column="expected_digits")
    assert sorted(x.screen_status for x in r.evaluated.collect()) == ["FAIL", "INCOMPLETE", "PASS"]
    row = r.summary.first()
    assert (row.rows_screened, row.passed, row.failed, row.incomplete) == (3, 1, 1, 1)
    assert r.quarantine.first().rule_failures == ["ME_READOUT_DIGITS_MATCH"]


def test_invalid_digit_metadata_is_unevaluated_not_a_false_failure(spark):
    r = screen_continuous_measures(
        _frame(spark, [(Decimal("0.250000"), 0, 5)]),
        observed_digits_column="observed_digits", expected_digits_column="expected_digits",
    )
    assert r.review.count() == 1
    assert r.quarantine.count() == 0


def test_wrong_profile_and_partial_metadata_fail_fast(spark):
    df = _frame(spark, [(Decimal("0.250000"), 5, 5)])
    with pytest.raises(ValueError, match="Unknown pinned profile"):
        screen_continuous_measures(df, profile_id="LATEST")
    with pytest.raises(ValueError, match="Supply both"):
        screen_continuous_measures(df, observed_digits_column="observed_digits")
    with pytest.raises(ValueError, match="Missing readout"):
        screen_continuous_measures(df, observed_digits_column="bad", expected_digits_column="expected_digits")
