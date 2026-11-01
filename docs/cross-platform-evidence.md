# Cross-platform migration evidence

## Claim and limits

This work tests a narrow question: does an identical, canonical metering input
yield identical correction outcomes on different Spark runtimes?

It is not a test of market-message conformance, billing correctness, vendor
security boundaries or tenant isolation. The existing P6269 findings concern
public anonymised quarter-hour profiles, not authoritative UMIG6/CMS messages.

The evidence harness serializes columns in sorted order, with explicit nulls
and UTC timestamps, calculates SHA-256 for each row, and aggregates two
independent 60-bit fragments into fixed buckets with their multiplicities.
Only bounded bucket statistics return to the driver. Hash collisions remain
theoretically possible: this is a reproducible regression fingerprint, not a
formal proof of row-set equality. Full regulated acceptance requires complete
authorized comparisons plus independent monetary and energy reconciliation.

## Workloads

| Workload | Entry point | Contract |
| --- | --- | --- |
| Correction parity | examples/cloud_validation.py | Eight synthetic rows: 2 current, 3 rejected, 2 superseded, 1 change |
| Correction-heavy load | examples/cloud_replay_benchmark.py | Distributed synthetic generator; all four lanes actually evaluated |
| Public P6269 | examples/cloud_fluvius.py | Caller-staged original CSVs; distinct local-clock and UTC-label coverage |
| Delta readback | cloud_validation.run(spark, delta_root=...) | Owned path, four lane writes and checksummed read-backs, followed by retry |

Entrypoints expose a callable run(spark, ...) using an **existing** Spark
session; local Spark can also run the CLI. Never override Fabric or Databricks
compute with a local master. Run the same Git commit, package wheel, fixture,
bucket count and schema on each target.

### Fabric

Pin the wheel in a Fabric environment, attach a disposable Lakehouse, run
the correction fixture in a notebook, then the staged P6269 workload if the
fixture passes. Collect the Spark application ID, item/run IDs, capacity
consumption when actually observable, runtime version and benchmark JSON.
Do not claim CU cost without an actual supported meter.

### Databricks

Use a pinned workspace Git folder and installed wheel or supported job
environment. Run the fixture in an isolated Unity Catalog schema/volume.
Capture the runtime/compute policy, Photon availability, DBU/VM usage when
measured and the JSON artifact. Serverless can behave differently from a
classic cluster. A Free Edition execution is not a comparable paid cost study.

### Reproduction

1. Install a wheel built from the pinned public commit on the managed engine.
2. Run cloud_validation.run(spark). Fail on any unexpected output count.
3. Compare the exact four manifest fingerprints with the baseline from a
   matching source commit. Any schema or bucket difference is a failure.
4. For Delta persistence, set delta_root to a clearly isolated, owned path
   whose name contains 'umig6-evidence-'. No automatic cleanup occurs.
   This tests overwrite/read-back/retry. Four separate Delta writes are NOT
   an atomic transaction and require a publication barrier in production.
5. Run cloud_replay_benchmark.run(spark, rows=100000, partitions=8).
   Record cold/warm state, shuffle and spill. Compare caching only where
   supported and under controlled repeated measurements.
6. Stage the original eight CSV category files from the two official Fluvius
   2024 ZIPs. Record the official archive SHA-256 separately, then run
   cloud_fluvius.run(spark, paths, coverage=True, full_corpus=True).
   Eight-file row totals alone do not prove source identity.

**Publication rule:** publish only actual successful runs with pinned source
SHA, runtime details, inputs and sanitized result manifests. Skipped Actions
jobs, mocked output and preconfigured workflows are not platform evidence.
Keep tenant/workspace identifiers, file paths, auth tokens, customer records
and other private data out of the public evidence.

## Baseline and status

Historical local Spark and full 84,326,400-row Fluvius benchmarks:
[benchmarking](benchmarking.md) and [public P6269](public-fluvius-benchmark.md).

Managed Fabric correction-fixture parity: **verified 1 November 2026** on Fabric Spark 4.1.1.5.5.20260929.238183312. All four output counts, column schemas and 64-bucket manifest fingerprints matched both GitHub-hosted Spark 3.5.5 and a separate Oracle ARM64 Spark 4.2.1 run (four evidence tests passed). The [sanitized measured report](../evidence/2026-11-01-fabric-fixture-parity.json) records exact source commit and fingerprints. The underlying Fabric execution artifact is retained privately; the public report is independently reproducible but not a public vendor-certified attestation. This does not cover full P6269, managed Delta writes, recovery or capacity cost.

Managed Databricks correction-fixture parity: **verified 1 November 2026** on Spark 4.2.0 serverless. The owned workspace notebook returned all four expected counts; exact column schemas and lane fingerprints matched Fabric Spark 4.1.1, GitHub-hosted Spark 3.5.5 and Oracle Spark 4.2.1. See the [sanitized four-engine evidence](../evidence/2026-11-01-fabric-fixture-parity.json). This is not a managed Delta persistence, production throughput or cost study.

The two managed platforms should not be compared on speed/cost until data
locality, compute sizes, cold starts, worker prices, repetitions and billing
scope are documented. The published synthetic benchmark excludes cluster
startup, Delta persistence and network ingest unless expressly stated.

Source UTC-label caveat: the public P6269 corpus has valid Belgian local
calendar coverage but repeated autumn and absent spring local hour labels.
Do not turn those source clock strings into verified UTC instants.
