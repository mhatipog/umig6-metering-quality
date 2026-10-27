import importlib.util
from pathlib import Path


def test_synthetic_benchmark_builds_unique_interval_keys(spark):
    module_file = Path(__file__).resolve().parents[1] / "examples" / "benchmark_spark.py"
    spec = importlib.util.spec_from_file_location("benchmark_spark", module_file)
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    rows = benchmark.sample_metering(spark, 8192, 4)
    assert rows.count() == 8192
    assert rows.select("ean", "meter_id", "register", "interval_start_utc").distinct().count() == 8192
    assert rows.where("energy_kwh < 0 OR interval_end_utc <= interval_start_utc").count() == 0
