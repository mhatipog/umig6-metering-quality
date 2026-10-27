"""Evidence semantics must be explicit before merging legacy meter revisions."""
from datetime import datetime

from pyspark.sql import functions as F
from umig6_metering_quality.provenance import screen_source_provenance


def rows(spark, records):
    columns = ("source_system", "source_record_id", "source_evidence_ref",
               "timestamp_semantics", "revision_basis")
    return spark.createDataFrame(records, columns)


def test_verified_source_revision_permits_replay(spark):
    data = rows(spark, [("mdm", "record-1", "catalogue/r-2026-01",
                         "UTC_VERIFIED", "SOURCE_REVISION")])
    data = data.withColumn("revision_at_utc", F.to_timestamp(F.lit("2026-10-27 12:00:00")))
    result = screen_source_provenance(data)
    assert result.replay_ready.count() == 1
    assert result.quarantine.count() == 0
    assert result.summary.first().replay_ready_rows == 1


def test_flavius_public_p6269_clock_labels_are_not_utc_proven(spark):
    data = rows(spark, [("P6269-public", "anonymous-row-001", "P6269/raw-csv",
                        "UTC_LABEL_UNVERIFIED", "EXPORT_SNAPSHOT")])
    result = screen_source_provenance(data)
    assert result.review.count() == 1
    assert result.analytics_ready.count() == 0
    assert "UTC_INSTANT_NOT_VERIFIED" in result.review.first().provenance_review_reasons


def test_export_snapshot_may_be_analytic_but_not_market_revision(spark):
    data = rows(spark, [("consumer-export", "raw-1", "manifest/v2",
                         "OFFSET_VERIFIED", "EXPORT_SNAPSHOT")])
    result = screen_source_provenance(data)
    assert result.analytics_ready.count() == 1
    assert result.replay_ready.count() == 0


def test_unrecognised_timestamp_claim_is_quarantined(spark):
    data = rows(spark, [("legacy", "row-1", "contract/1",
                         "TRUST_ME", "SOURCE_REVISION")])
    result = screen_source_provenance(data)
    assert result.quarantine.count() == 1
    assert "UNKNOWN_TIMESTAMP_SEMANTICS" in result.quarantine.first().provenance_failures


def test_missing_source_evidence_must_not_pass(spark):
    data = rows(spark, [("legacy", "row-1", "",
                         "UTC_VERIFIED", "SOURCE_REVISION")])
    result = screen_source_provenance(data)
    assert result.review.count() == 1
    assert result.analytics_ready.count() == 0


def test_missing_optional_provenance_columns_yields_review(spark):
    data = spark.createDataFrame([("legacy",)], ["source_system"])
    result = screen_source_provenance(data)
    assert result.review.count() == 1
    assert result.replay_ready.count() == 0
    assert "MISSING_COLUMN_SOURCE_RECORD_ID" in result.review.first().provenance_review_reasons


def test_wrong_type_does_not_silently_coerce_identity(spark):
    data = spark.createDataFrame([("legacy", 123, "doc", "UTC_VERIFIED", "SOURCE_REVISION")],
                                 ["source_system", "source_record_id", "source_evidence_ref",
                                  "timestamp_semantics", "revision_basis"])
    result = screen_source_provenance(data)
    assert result.quarantine.count() == 1
    assert "WRONG_TYPE_SOURCE_RECORD_ID" in result.quarantine.first().provenance_failures


def test_unsupported_revision_timestamp_type_is_rejected(spark):
    data = rows(spark, [("mdm", "a", "doc", "UTC_VERIFIED", "SOURCE_REVISION")])
    data = data.withColumn("revision_at_utc", F.lit("yesterday"))
    result = screen_source_provenance(data)
    assert result.quarantine.count() == 1
    assert result.replay_ready.count() == 0


def test_null_source_revision_does_not_claim_replay(spark):
    data = rows(spark, [("mdm", "a", "doc", "UTC_VERIFIED", "SOURCE_REVISION")])
    data = data.withColumn("revision_at_utc", F.lit(None).cast("timestamp"))
    result = screen_source_provenance(data)
    assert result.analytics_ready.count() == 1
    assert result.replay_ready.count() == 0
