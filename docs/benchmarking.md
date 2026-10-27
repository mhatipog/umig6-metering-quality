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

## Fast-path correctness boundary

`screen_metering_rows()` is a projection-only Bronze quality screen. It
preserves duplicates and emits an independent quarantine output. It does **not**
calculate latest revisions, missing quarters, supplier-state changes or asset
assignment validity. Never replace `process_metering()` with this fast path if
correction lineage is needed. The intended Databricks pattern is:

1. Row-wise Bronze screening for cheap malformed-value quarantine.
2. Explicit `screen_source_provenance()` review of source system, timestamp
   semantics and revision basis.
3. Correction-aware `process_metering()` on `replay_ready` rows, with
   separate quarantine, superseded and changes outputs.

The synthetic benchmark now times `bronze` and `provenance` stages separately
from the correction-heavy stateful step on identical source row counts. These
measure different operations and must **not** be described as a directly
interchangeable speed comparison. Report source materialization separately.
Databricks/Photon throughput and cost remain unmeasured.

## Measured baseline after adding the provenance/Bronze stages

Run [40550324856](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40550324856),
27 October 2026, **one million synthetic readings per stage**. PySpark
4.2.0, GitHub-hosted Ubuntu local Spark, 8 input partitions, 16 shuffle
partitions, source materialisation timed separately. These are separate jobs;
wall-clock times are not necessarily additive for one physical pipeline.

| Stage | Processing seconds | Rows/s | Distinct semantics |
|---|---:|---:|---|
| Readiness | 4.469 | 223,772 | Structural schema and values |
| Pinned UMIG prerequisite screen | 1.164 | 858,854 | No readout digit metadata, so incomplete |
| Bronze row screen | 1.361 | 734,827 | No dedup, no revision detection |
| Source provenance gate | 1.851 | 540,149 | Synthetic trusted evidence fields |
| Correction-aware | 23.428 | 42,685 | Grouping, windowing, conflicting revision checks |

Do **not** claim that Bronze has a 17x faster implementation of correction
replay: it does different work. The full reconciliation stage is still the
main measured performance target. Databricks Photon, Oracle compute,
cost per million rows and production-scale corrections are not measured.

## Full four-output correction replay and intermediate persistence

[Run 40551220098](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40551220098)
materialised `current`, `rejected`, `superseded` and `changes` on
1,000,000 uniquely keyed synthetic readings. Spark 4.2.0, GitHub-hosted
local mode (not Databricks): 8 shuffle partitions **55.100 s**, 32 shuffle
partitions **58.729 s**. The historical 23.428-second measurement
materialised only `current` and `rejected`: it was **not** full output
materialisation.

After introducing opt-in `cache_shared=True`,
[run 40551920250](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40551920250)
measured all four outputs, including cache release, on Spark 4.2.1:

| Mode | Shuffle partitions | Seconds | Rows per second |
|---|---:|---:|---:|
| No shared cache | 8 | 40.693 | 24,574 |
| Persist shared history, release after use | 8 | 25.281 | 39,556 |
| Persist shared history, release after use | 32 | 21.931 | 45,597 |

At the same 8-partition setting caching reduced wall time by approximately
38% for this test. The independent GitHub-hosted runners and Spark patch
versions prevent treating this as a general Databricks/Photon guarantee.
Runs above contain **unique** interval keys; additional runs with deliberate
source revisions must be interpreted separately.

When `cache_shared=True`, the library persists the shared Spark version
table and the safely annotated correction history using
`MEMORY_AND_DISK`. A caller must materialise all outputs **and then**
release the persisted intermediate data:

```python
with process_metering(source_revisions, cache_shared=True) as result:
    result.current.write.format("delta").mode("append").saveAsTable("silver.current")
    result.rejected.write.format("delta").mode("append").saveAsTable("ops.rejected")
    result.superseded.write.format("delta").mode("append").saveAsTable("ops.superseded")
    result.changes.write.format("delta").mode("append").saveAsTable("ops.changes")
```

These writes are independent and not idempotent as written; use the
versioned batch/transaction barrier pattern for real jobs. The example is
only to demonstrate the cache lifecycle, **not** production persistence
semantics. Cache memory, spill and node cost require validation on the
target Databricks workload.
