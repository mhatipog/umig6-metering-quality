# Databricks / Delta integration

The library returns Spark DataFrames. It does not create clusters, provision Unity Catalog, or silently write to customer tables. The same transformations run on supported Fabric Spark runtimes.

## Suggested flow

```text
Immutable Bronze
  -> screen_metering_rows -> quarantine
  -> screen_source_provenance -> review / replay_ready
  -> process_metering -> current / rejected / superseded / changes
  -> reconcile all outputs -> publish completed batch
```

Keep original records and source IDs in Bronze. `replay_ready` is for *declared source-authored revisions*, not successive exports or ingestion timestamps. All four correction outputs and the separate screening streams need controlled retention and access.

Write each output to governed, versioned Delta storage. For retries, use stable batch IDs and `MERGE`/replace semantics appropriate to the destination; do not append current state blindly. A successful write of one table must **not** mark the batch complete. If any sink fails, reconcile and retry all outputs before publication.

`write_delta_batch` is an illustrative per-table retry helper, not an atomic cross-table transaction. [Managed fault-injection evidence](../evidence/2026-11-01-managed-delta-recovery.json) validates the explicit publication gate on both Databricks and Fabric under synthetic conditions.

The core expressions are Spark-native (no row-wise Python UDFs). Runtime, caching, shuffles, spill, storage layout, access permissions and billing still require measurement in the target environment. [Basic example](../examples/basic_pipeline.py) · [Full batch example](../examples/databricks_batch.py) · [Source rules](rule-provenance.md).
