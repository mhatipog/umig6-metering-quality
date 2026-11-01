# Cross-platform evidence

Runs below use identical, version-pinned PySpark code where indicated. The JSON files retain exact source commits, runtimes, checksums, counts, and caveats. Public records contain aggregates, not customer data.

| Check | Result | Evidence |
| --- | --- | --- |
| Correction replay | Four matching output fingerprints across local Spark, Fabric and Databricks on a synthetic eight-row fixture | [Fixture](../evidence/2026-11-01-fabric-fixture-parity.json) |
| Synthetic load | Identical results for 100,000 observations on both managed engines | [Load](../evidence/2026-11-01-managed-100k.json) |
| Delta failure/retry | Partial publication blocked; corrupt output detected; retry reconciled; cleanup passed on both managed engines | [Recovery](../evidence/2026-11-01-managed-delta-recovery.json) |
| Original Fluvius P6269 | Databricks: 84,326,400 rows across eight CSVs; Fabric: 42,163,200 rows across the first four | [Databricks](../evidence/2026-11-01-databricks-p6269-full-corpus.json) · [Fabric](../evidence/2026-11-01-fabric-p6269-archive0.json) |
| Scan-size tuning | Same source invariant; different measured preferences by platform | [Tuning](p6269-cloud-tuning.md) |

All eight original public CSV files were checksum-verified in the owned Fabric staging area, but **staging is not Spark validation**. Full Fabric processing of the second archive is not reported here.

The Fluvius data is **not** a UMIG 6 message feed. Its timestamps follow Belgian wall-clock DST behavior despite a `Z` suffix: all 2,400 profiles pass local-clock completeness, but unique UTC instants are not established. The public correction tests instead use explicit synthetic source revisions.

**Performance scope:** reported Spark action times exclude startup, transfer, staging and billing. No controlled cross-vendor speed or cost ranking has been established. Fingerprints are bounded regression checks, not a mathematical proof of complete row-set identity.

## Reproduce

- [Synthetic fixture](../examples/cloud_validation.py) and [100k replay](../examples/cloud_replay_benchmark.py): run on an existing Spark session.
- [Delta fault injection](../examples/cloud_delta_recovery.py): use a disposable catalog only.
- [Fluvius staging](../examples/cloud_stage_fluvius.py) and [source scans](../examples/cloud_fluvius.py): verify official ZIP/member SHA-256 values before processing.

[Data source & DST analysis](public-fluvius-benchmark.md) · [Detailed benchmark methodology](benchmarking.md)
