# Delta recovery experiment

Run `examples/cloud_delta_recovery.run(spark)` in an **owned disposable**
managed-Spark catalog/schema with permission to create and drop managed Delta
tables. No production catalog, customer workspace or imported records should
be involved.

The experiment uses eight synthetic observations. It independently
materializes four Delta outputs, injects a failure after the first sink,
completes and reconciles all sinks, publishes a gate marker, retracts that
marker, injects one corrupted output, verifies the mismatch blocks publication,
then restores all four outputs and republishes. It checks stable data-row
multiplicity and 64-bucket fingerprints, not only aggregate counts.

All managed tables have an unguessable `umig6_recovery_<hex>_` prefix. The
experiment drops only tables it created. If a drop fails, the runner reports
the failed table's suffix and must not claim cleanup success.

A marker table is **not** an atomic transaction across the four Delta
tables. This demonstrates how an orchestrator can fail closed when it
observes a partial or inconsistent batch. It does not protect against
concurrent consumers ignoring the marker, writes happening after marker
publication, or an externally compromised publisher. Production requires
a properly fenced batch protocol and permissions preventing direct reads
from incomplete tables.

Unexecuted workloads are not platform evidence. Public results must include
actual managed Spark runtime, source commit, run IDs retained privately,
sanitized counts/fingerprints, and whether cleanup succeeded.

## Managed results — 1 November 2026

The [sanitized independent executions](../evidence/2026-11-01-managed-delta-recovery.json)
were verified on Fabric Spark 4.1.1 and Databricks serverless Spark 4.2.0
using the same pinned source commit `d59b04563522f3bc6f96301ad4594fd568cf97cd`.
All four output-lane SHA-256 regression fingerprints matched across engines.
Both detected the intentionally damaged current lane, withheld publication,
replayed correctly and reported successful disposal of the scratch tables.
These tests use eight synthetic observations, not official market corrections.
