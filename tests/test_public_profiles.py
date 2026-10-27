from umig6_metering_quality.public_profiles import screen_public_profiles, public_profile_metrics
from pyspark.sql.types import StructType, StructField, StringType

HEADERS=("EAN_ID", "Datum", "Datum_Startuur", "Volume_Afname_KWh", "Volume_Injectie_KWh", "Warmtepomp_Indicator", "Elektrisch_Voertuig_Indicator", "PV_Installatie_Indicator", "Contract_Categorie")


def sample(spark, *rows):
    return spark.createDataFrame([(point,date,hour,used,sent,"0","0","0","residentieel") for point,date,hour,used,sent in rows],StructType([StructField(c, StringType(), True) for c in HEADERS]))


def test_valid_quarter_and_belgian_utc_conversion(spark):
    data=screen_public_profiles(sample(spark,("profile-1","2024-01-01","00:15:00","0.251","0")))
    assert data.quarantine.count()==0
    assert data.valid.first().interval_start_utc.isoformat()=="2023-12-31T23:15:00"
    assert public_profile_metrics(data).first().accepted_rows==1


def test_valid_summer_time(spark):
    data=screen_public_profiles(sample(spark,("profile-1","2024-07-01","00:15:00","0.251","0")))
    assert data.valid.first().interval_start_utc.isoformat()=="2024-06-30T22:15:00"


def test_ambiguous_autumn_hour_quarantined(spark):
    x=screen_public_profiles(sample(spark,("profile-1","2024-10-27","02:15:00","0.5","0")))
    assert x.valid.count()==0
    assert "AMBIGUOUS_BELGIAN_LOCAL_HOUR" in x.quarantine.first().quality_errors


def test_nonexistent_spring_hour_quarantined(spark):
    x=screen_public_profiles(sample(spark,("profile-1","2024-03-31","02:15:00","0.5","0")))
    assert "NONEXISTENT_BELGIAN_LOCAL_HOUR" in x.quarantine.first().quality_errors


def test_both_negative_and_missing_values_quarantined(spark):
    x=screen_public_profiles(sample(spark,("profile-2","2024-01-01","00:30:00","-0.4",None)))
    errors=x.quarantine.first().quality_errors
    assert "INVALID_IMPORT_KWH" in errors and "INVALID_INJECTION_KWH" in errors


def test_bad_format_does_not_silently_parse(spark):
    x=screen_public_profiles(sample(spark,("p","nonsense","05:00:00","1.0","0")))
    assert "INVALID_SOURCE_DATETIME" in x.quarantine.first().quality_errors


def test_unaligned_quarter(spark):
    x=screen_public_profiles(sample(spark,("p","2024-01-01","00:17:00","1.0","0")))
    assert "UNALIGNED_QUARTER_HOUR" in x.quarantine.first().quality_errors


def test_date_with_slash(spark):
    x=screen_public_profiles(sample(spark,("p","01/02/2024","00:15:00","1.0","0")))
    assert x.valid.count()==1


def test_official_iso_utc_timestamps_are_not_shifted(spark):
    x=screen_public_profiles(sample(spark,("p","2024-01-01","2024-01-01T00:15:00.000Z","0.3","0")))
    assert x.valid.first().interval_start_utc.isoformat()=="2024-01-01T00:15:00"


def test_official_utc_fall_repeated_hour_is_unambiguous(spark):
    x=screen_public_profiles(sample(spark,
      ("p","2024-10-27","2024-10-27T00:15:00.000Z","0.3","0"),
      ("p","2024-10-27","2024-10-27T01:15:00.000Z","0.4","0")))
    assert x.quarantine.count()==0
    assert x.valid.count()==2


def test_profile_coverage_catches_duplicate_masking_missing_interval(spark):
    from umig6_metering_quality.public_profiles import profile_coverage
    data=screen_public_profiles(sample(spark,
        ("p","2024-01-01","2024-01-01T00:00:00.000Z","0.3","0"),
        ("p","2024-01-01","2024-01-01T00:00:00.000Z","0.3","0"),
    ))
    row=profile_coverage(data, expected_intervals=2,
        expected_start_utc="2024-01-01 00:00:00", expected_end_utc="2024-01-01 00:15:00").first()
    assert row.observed_rows == 2
    assert row.unique_utc_quarters == 1
    assert row.coverage_status == "INCOMPLETE"


def test_profile_coverage_complete_when_all_instants_are_distinct(spark):
    from umig6_metering_quality.public_profiles import profile_coverage
    data=screen_public_profiles(sample(spark,
        ("p","2024-01-01","2024-01-01T00:00:00.000Z","0.3","0"),
        ("p","2024-01-01","2024-01-01T00:15:00.000Z","0.3","0"),
    ))
    row=profile_coverage(data, expected_intervals=2,
        expected_start_utc="2024-01-01 00:00:00", expected_end_utc="2024-01-01 00:15:00").first()
    assert row.coverage_status == "COMPLETE"


def test_public_profile_utc_labels_are_explicitly_unverified(spark):
    x = screen_public_profiles(sample(spark, (
        "p", "2024-10-27", "2024-10-27T02:15:00.000Z", "0.3", "0")))
    assert x.valid.first().source_utc_label_unverified is True


def test_p6269_z_label_local_clock_not_reinterpreted_as_utc(spark):
    # Official P6269's Z labels behave like Brussels local wall time.
    winter = screen_public_profiles(sample(spark,
        ("p", "2024-01-01", "2024-01-01T00:15:00.000Z", "0.3", "0"))).valid.first()
    summer = screen_public_profiles(sample(spark,
        ("p", "2024-07-01", "2024-07-01T02:15:00.000Z", "0.3", "0"))).valid.first()
    assert winter.local_start_be.isoformat() == "2024-01-01T00:15:00"
    assert summer.local_start_be.isoformat() == "2024-07-01T02:15:00"
    assert winter.timestamp_semantics == "UTC_LABEL_UNVERIFIED"
    assert winter.revision_basis == "EXPORT_SNAPSHOT"
    assert winter.source_utc_label_unverified is True
