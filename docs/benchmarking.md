# Benchmarking on Spark / Azure Databricks

The synthetic benchmark produces unique quarter-hour observations across 4,096 synthetic supply points without a Python UDF or building rows on the driver. It profiles **readiness**, **pinned rulebook prerequisites** and the **correction-aware metering pipeline** independently.

```bash
pip install -e .
python examples/benchmark_spark.py --rows 1000000 --partitions 16 --workload all --output-json benchmark.json
```

Each result includes Spark version, synthetic row count, source materialisation time (excluded from processing time), processing throughput, accepted/rejected totals, and an optimised logical plan hash. This is **not a full production cost benchmark**: it does not measure Delta MERGE, cloud ingestion, asset joins, skew from a real network, Photon, or data movement. Synthetic EANs are not genuine allocated supply points.

The `corrections` workload materialises two sinks, and Spark may repeat parts of the logical plan between actions. Do not compare its throughput directly with the single-action readiness profile as if both performed identical work. Consider materialisation boundaries and incremental processing on the actual platform.

## Public Belgian metering corpus

Fluvius publishes a [dataset of 2,400 anonymised full-year residential quarter-hour series](https://opendata.fluvius.be/explore/dataset/1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar/), with ZIP attachments `P6269_1_50_DMK_Sample_Elek_2024_Deel_1.zip` and `Deel_2.zip`, plus a separate legend. **These public files are not MIG6 messages, and must not be confused with production CMS exports.**

Before benchmarking on the public corpus, download the files from Fluvius, verify the legend and actual schema, document units, measurement interval and time zone, and implement a dataset-specific adapter. Do not invent EAN/asset identities or validation statuses for the public sample. Report source checksum, record count, missing periods and Spark job/environment metadata. No actual Fluvius benchmark is claimed by this repository yet.

## Databricks

Run with Photon where available and record runtime, node type, worker count, shuffle read/spill and job cost. Native Spark functions are Photon-compatible in principle, but acceleration cannot be inferred without an A/B measurement. For managed Delta targets follow [Azure Databricks optimisation guidance](https://learn.microsoft.com/en-us/azure/databricks/optimizations/); prefer current Unity Catalog and liquid clustering recommendations over hardcoded legacy partition/ZORDER defaults.

Large data must never be collected to the driver. Benchmark JSON contains aggregate counts only. Do not publish source EANs, measurements or credentials.

## Measured synthetic baseline — 27 October 2026

Verified GitHub Actions [benchmark run 40506012951](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40506012951) on commit `9a2a5b9b` (job logs and JSON artifact). GitHub-hosted Ubuntu runner, Spark **4.2.0**, local mode, **1,000,000 synthetic readings**, source materialised before processing timers, 8 range partitions, 16 shuffle partitions, AQE enabled.

| Workload | Processing seconds | Rows/second | Accepted | Rejected |
| --- | ---: | ---: | ---: | ---: |
| `readiness` | 4.516 | 221,458 | 1,000,000 | 0 |
| `rulebook` | 1.082 | 924,450 | 1,000,000* | 0 |
| `corrections` | 23.238 | 43,033 | 1,000,000 | 0 |

*Rulebook accepted here includes **INCOMPLETE**, because no source readout digit metadata was supplied. It is **not** a million completely validated records.*

These are different workloads. The correction benchmark computes two output counts, and may re-execute part of the Spark plan. This is not Azure Databricks / Photon throughput, not end-to-end Delta persistence, not a market-data certification and not a full-scale public Fluvius corpus test.
