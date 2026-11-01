# P6269 CSV scan tuning

Measured on **42,163,200 original Fluvius observations** (archive 0) using Spark's `spark.sql.files.maxPartitionBytes`. Each configuration runs row screening, strict UTC-label coverage and local-clock coverage. All aggregate invariants matched across settings and platforms.

| File split | Fabric (1 trial) | Databricks (mean of 2) |
| --- | ---: | ---: |
| 64 MiB | **496.476 s** | 78.588 s |
| 128 MiB | 590.561 s | **69.345 s** |
| 256 MiB | 505.032 s | 70.293 s |

**Finding:** 64 MiB finished 15.9% sooner than 128 MiB in the one Fabric trial. Databricks showed no improvement from moving off 128 MiB across two non-randomized repetitions. These are **within-platform observations**, not vendor comparisons: compute sizes, admission, caching and shared-load effects were not normalized, and CUs/DBUs were not measured.

Source integrity and results: [Fabric JSON](../evidence/2026-11-01-fabric-p6269-split-tuning-archive0.json) · [Databricks JSON](../evidence/2026-11-01-databricks-p6269-split-tuning-archive0.json). The CSVs are public metering profiles, not UMIG 6 messages; clock labels have unresolved UTC meaning.

To reproduce, stage the verified original CSV files, then run [p6269_tuning.py](../examples/p6269_tuning.py) with the actual managed Spark session, three split values, repeat count and a JSON output path. Record engine/runtime, source hashes and action times; do not include network or cluster startup in action timing.
