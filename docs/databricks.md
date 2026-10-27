# Databricks and Delta Lake integration

All core checks compile to Spark expressions: `when`, `array`, `filter`, `groupBy`, windows and equijoins/range joins. There are **no Python UDFs, per-row Python callbacks, or production `collect()` calls**. This preserves Catalyst/Photon optimisation opportunities, but does **not** constitute a Databricks throughput benchmark.

## Suggested Bronze → Silver → Gold pattern

1. Ingest each immutable source batch into Bronze, preserving original columns, source format, timestamps, source file, load IDs, and extraction/version metadata. Use Auto Loader's rescued-data column for unexpected fields if appropriate.
2. Adapt Bronze to the explicitly typed canonical schema. Never infer a billing revision from an ingestion timestamp.
3. Run `process_metering` and/or `validate_asset_history`. Optionally run `evaluate_readiness` on historical data and `reconcile_metering_assets` on accepted readings/asset history.
4. Publish the accepted current observations to Silver; quarantine rejected observations in a **restricted** Delta table with stable IDs, rule versions, source IDs and immutable batch IDs. Keep review warnings separate from hard failures.
5. Publish reason-count metrics; mark a batch **complete** only once all required outputs have been written and reconciled. Downstream Gold may consume completed batches only.

Databricks document two-stream quarantine patterns and warn that separate writes from `foreachBatch` can become inconsistent. This library therefore exposes transformations independently, rather than writing multiple sinks invisibly. `write_delta_batch` is an optional illustrative helper: it uses Delta `replaceWhere` for retries within a batch, **but is not an atomic multi-table transaction**. Caller must provision tables, provide batch IDs, monitor writes and enforce a commit barrier / retry all sinks.

```python
from pyspark.sql import functions as F
from umig6_metering_quality import process_metering, envelope_quarantine, write_delta_batch
run_id = 'load_2026_10_27_001'
quality = process_metering(spark.read.table('bronze.metering').filter(F.col('load_id') == run_id))
q = envelope_quarantine(quality.rejected, dataset='metering', batch_id=run_id)
write_delta_batch(quality.current.withColumn('batch_id', F.lit(run_id)), q.quarantined,
                  batch_id=run_id, current_table='silver.metering_batches',
                  quarantine_table='ops.metering_quarantine')
# A separate job action must confirm both tables and release this batch to Gold.
```

### Practical performance checklist

- **Filter early** by immutable load/window before global de-duplication and revision windows. Do not repeatedly reprocess the entire history for small incremental loads.
- **Keep UTC as a Spark timestamp** and numeric energy as decimals before calling the core; avoid parsing strings in dozens of checks.
- **Avoid Python UDFs.** Project only the required columns for optional historical asset joins. The asset range join can dominate runtime; benchmark and prune its inputs explicitly.
- Enable AQE where supported (`spark.sql.adaptive.enabled`); use `explain('formatted')` and Spark UI to inspect shuffles, skew, spills and broadcast decisions.
- Cache reused intermediate results only when multiple actions are actually materialized and memory metrics justify it. Spark evaluation is lazy.
- For new managed Databricks Delta tables, consider **liquid clustering** or Databricks-managed optimisation instead of creating tiny physical partitions per EAN or day. Choose columns from observed query filters, not guesses.
- Run the benchmark `python examples/benchmark_spark.py --rows 1000000 --partitions 16` on the intended Databricks Runtime/cluster. Record runtime version, instance type, Photon status, partitions, `rows/s`, shuffle statistics and cost. Synthetic timing is not a claim of production latency.
- Test historical batch replay, late corrections, multiple concurrent versions, DST, meter replacements, and malformed reference data before relying on the output.

## Security and operational boundaries

Quarantine retains raw sensitive records by design and must be access-restricted. A stable deterministic `quarantine_id` facilitates repeated-batch diagnostics but is not a substitute for encryption, access control, or row-level retention policy. Store error counts separately if broad dashboards should not see row-level energy data.

References:
- [Azure Databricks validation and quarantine](https://learn.microsoft.com/en-us/azure/databricks/transform/validate)
- [Lakeflow quarantine pattern](https://learn.microsoft.com/en-us/azure/databricks/ldp/expectation-patterns)
- [Databricks performance efficiency](https://docs.databricks.com/gcp/en/lakehouse-architecture/performance-efficiency/best-practices)

## Source-certified revision replay and performance stages

Do **not** feed all Bronze records straight into `process_metering()` when
their revision timestamps are merely ingestion timestamps or when a public
clock label has not been proven to be UTC. The expanded example in
[`examples/databricks_batch.py`](../examples/databricks_batch.py) performs:

1. `screen_metering_rows(bronze)` — native Spark row checks without a shuffle;
   **preserves redeliveries** and quarantines malformed rows early.
2. `screen_source_provenance(screen.accepted)` — separates review/invalid
   metadata from source records with a documented timestamp contract.
3. `process_metering(origin.replay_ready)` — revision-aware processing only
   where the input source provides an actual `SOURCE_REVISION` timestamp.
4. Restricted quarantine sinks for input validation, source metadata, review,
   revision conflicts and asset history; publication barrier outside this library.

This is a *technical evidence contract*. Merely putting `UTC_VERIFIED`
in a column or entering a plausible source document reference is not proof
that an operator audited that upstream system. For each source, document
time-zone and correction precedence with the system owner.

For append-only data with no source revision, the fast Bronze screen is still
useful. It must not produce a false correction timeline or a certified MIG6
validation status. The full correction engine can be expensive because it
must group and order versions to avoid arbitrary choices.

All runs here use synthetic or public data. [Rule provenance](rule-provenance.md)
explains precisely which controls derive from public UMIG documents and
which are project-defined checks.
