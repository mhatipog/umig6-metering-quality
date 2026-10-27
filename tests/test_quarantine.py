from umig6_metering_quality import envelope_quarantine, write_delta_batch
from conftest import EAN


def test_quarantine_replay_and_reason_counts(spark):
    data=spark.createDataFrame([(EAN,["BAD_INTERVAL","BAD_REGISTER"]),
                                ("541234567890123456",["BAD_INTERVAL"]),
                                ("541234567890123456",[])], "ean string, quality_errors array<string>")
    a=envelope_quarantine(data,dataset="metering",batch_id="run_20261027")
    b=envelope_quarantine(data,dataset="metering",batch_id="run_20261027")
    assert a.quarantined.count()==2
    assert sorted(x.quarantine_id for x in a.quarantined.collect())==sorted(x.quarantine_id for x in b.quarantined.collect())
    assert {x.rule_id:x.affected_rows for x in a.reason_counts.collect()}=={"BAD_INTERVAL":2,"BAD_REGISTER":1}


def test_batch_validation_prevents_unsafe_sql_predicates(spark):
    import pytest
    df=spark.createDataFrame([(1,["X"])],"a int,quality_errors array<string>")
    with pytest.raises(ValueError,match="batch_id"):
        envelope_quarantine(df,dataset="metering",batch_id="anything' OR 1=1")
    with pytest.raises(ValueError,match="Invalid batch_id"):
        write_delta_batch(df,df,batch_id="x'",current_table="x.good",quarantine_table="x.bad")
    with pytest.raises(ValueError,match="missing batch_id"):
        write_delta_batch(df,df,batch_id="run1",current_table="x.good",quarantine_table="x.bad")
