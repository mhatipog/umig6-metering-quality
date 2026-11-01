# Metering quality for Spark

A PySpark library for checking interval-metering data, tracing source revisions and keeping uncertain records out of trusted outputs. Built around Belgian quarter-hour metering, but the processing patterns are general.

## What it does

- Checks interval boundaries, readings, identifiers and source provenance.
- Keeps invalid and ambiguous records in separate quarantine/review streams.
- Resolves genuine source revisions without silently discarding conflicting values.
- Checks coverage against supplied active periods and validates meter/asset history.

The core is Spark-native DataFrame processing, with no row-by-row Python UDFs. It runs on Apache Spark, including Databricks and Microsoft Fabric.

## Try it

Python 3.10+ and Spark 3.5+:

```bash
python -m pip install -e '.[test]'
pytest -q
python examples/basic_pipeline.py
```

For an existing, *normalised* metering table with documented source revisions:

```python
from umig6_metering_quality import screen_metering_rows, screen_source_provenance, process_metering

rows = screen_metering_rows(spark.table("bronze.metering"))
origin = screen_source_provenance(rows.accepted)

with process_metering(origin.replay_ready, require_source_revision=True) as result:
    current = result.current
    rejected = result.rejected
    # Persist all required outputs before making a batch visible.
```

Persist the row-level quarantine and provenance review outputs too; they are not included in `current`.

## Evidence

The tests cover Spark 3.5 and 4.2. Synthetic correction fixtures and fault-injected Delta retries have also run on managed Fabric and Databricks. For real-data checks, the original public Fluvius 2024 P6269 corpus (84.3 million rows) was processed on Databricks; one of the two archives was processed on Fabric.

See [measured runs and limitations](docs/cross-platform-evidence.md). Raw results are in [evidence/](evidence/). These are reproducibility checks, **not** vendor performance rankings.

## Notes

UMIG 6-informed, **not** an official UMIG 6 or Atrias validator. Public Fluvius profiles are not market messages; their apparent UTC timestamps have unresolved daylight-saving-time semantics. Production pipelines need source-specific contracts, access controls and a batch publication gate.

[Design](docs/architecture.md) · [Databricks/Delta](docs/databricks.md) · [Rule sources](docs/rule-provenance.md) · [Public dataset](docs/public-fluvius-benchmark.md) · [Benchmarks](docs/benchmarking.md)

MIT license. Issues and reproducible contributions welcome.
