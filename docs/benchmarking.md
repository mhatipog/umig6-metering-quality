# Benchmark notes

Two different workloads are intentionally kept separate: **synthetic correction replay**, which exercises revision logic, and the **original Fluvius P6269** profiles, which exercise source parsing and clock completeness but contain no authoritative correction history.

## Synthetic correction replay

```bash
pip install -e .
python examples/benchmark_spark.py --rows 1000000 --partitions 16 --workload all --output-json benchmark.json
```

The generator creates distributed readings and revisions without building rows on the driver. The four-output replay benchmark materializes `current`, `rejected`, `superseded` and `changes` separately. Earlier local Spark 4.2.1 single runs on 1 million input rows took **59.418 s uncached** versus **32.426 s with opt-in shared caching**. These results are not Databricks Photon or production Delta measurements.

The [100k managed replay](../evidence/2026-11-01-managed-100k.json) verified matching output fingerprints in Fabric and Databricks. Timings are intentionally **not** normalized into a provider comparison.

## Original public data

Run [benchmark_public_fluvius.py](../examples/benchmark_public_fluvius.py) for one or both ZIP attachments from the official [Fluvius source](https://opendata.fluvius.be/explore/dataset/1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar/). The complete dataset has **84,326,400 rows**; public records are not native UMIG market messages.

Local clock completeness is distinct from strict UTC-label uniqueness because the source strings follow Belgian daylight-saving-time labels despite ending in `Z`. See [dataset evidence](public-fluvius-benchmark.md), [managed Spark results](cross-platform-evidence.md), and [within-platform CSV split tuning](p6269-cloud-tuning.md).

## Interpretation

Spark action times exclude cluster startup, archive download, staging and billing; source counts and fingerprints are necessary but not sufficient for functional certification. Query plans, cache state, shuffle partitions, parallelism and hardware must be held comparable before interpreting speed changes. Production-scale reconciliation, Delta writes, regulated metering correctness and cost require separate checks.
