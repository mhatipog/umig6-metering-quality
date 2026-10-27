# Handoff — UMIG6 Metering Quality v0.8.0

**Status (27 October 2026):** public, installable-from-source, technically useful as a *non-certified* PySpark component. Suitable for demonstrations, research, evaluation of a normalised legacy metering dataset, and staged adoption into controlled analytics pipelines. **Not certified for live billing, settlement, Atrias inter-party exchange, or autonomous remediation.**

## Integrate by workload

1. Ingest and persist immutable Bronze records, including origin/batch metadata and unmodified source values.
2. Write an **authorised adapter** from your source to the documented canonical interval schema. Use strings for EANs and meter identifiers, decimals for kWh, and Spark timestamps only for independently established UTC instants.
3. Run `screen_metering_rows()` and route malformed observations to a restricted quarantine.
4. Run `screen_source_provenance()`. Hold unknown/ambiguous clock claims for review. Require a source contract; do not equate file ingestion time or export snapshot order with a source-authored market revision.
5. For actual source revision history, call `process_metering(origin.replay_ready, require_source_revision=True)`. Materialise `current`, `rejected`, `superseded`, `changes` and optional scoped coverage. Use `cache_shared=True` only after testing memory use.
6. Validate effective-dated assets with `validate_asset_history()` and optionally join readings with `reconcile_metering_assets()`. Perform structural readiness checks separately by scenario.
7. Write quarantine/review/accepted versions using the organisation's Delta transaction and batch-completion conventions. The example replacement writes are **not atomic across tables**.

## What is currently verified

| Evidence | Result | Boundary |
| --- | --- | --- |
| CI | 93 tests pass on both PySpark 3.5.5 and 4.2.1 — [run](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40557078089) | Synthetic fixtures, not live DGO messages |
| Public Fluvius P6269 | 84,326,400 rows screened; 2,400/2,400 locally complete profiles — [evidence](public-fluvius-benchmark.md) | Public samples are **not** native UMIG messages; `Z` timestamps show wall-clock DST behaviour |
| Correction-heavy Spark workload | 59.418s uncached vs 32.426s cached, 1M synthetic rows with 98,304 corrections — [method](benchmarking.md) | Local GitHub runner; not Photon or a cost claim |
| Documentation | Pinned historical public UMIG checks documented separately from technical quality rules — [rule register](rule-provenance.md) | No claim to implement grid-operator-specific unpublished tolerances |

## Before operational production use

- Verify the source-specific timestamp, physical meter/register/point identity, revision precedence, units, validity periods and correction semantics with the actual system owner.
- Establish historical expected scopes before judging missing intervals. Meter replacements may split scopes.
- Exercise duplicates, late/out-of-order revisions, concurrent conflicting messages, DST and ambiguous meter assignments on **authorised representative samples**; inspect all quarantine lanes.
- Validate Spark shuffle/skew/spill, Delta table layout, cache sizing, processing cost and Photon behaviour on the target Databricks Runtime.
- Establish access control, retention, GDPR rules, CI log hygiene and downstream batch-completion barriers.
- Keep a source-and-rule register: **project technical PASS ≠ official MIG6/V1/V2/V3 status**.

## Maintenance and reproducibility

Clone the repository and run `pip install -e '.[test]'`, then `pytest -q`. Synthetic Spark benchmarks are in `examples/benchmark_spark.py`; real-data acquisition and metrics are reproducible via `examples/benchmark_public_fluvius.py`, which downloads the official two public ZIPs at runtime. Public ZIP contents, original customer data and source-level meter measurements must **not** be committed to this repo. Review the [changelog](../CHANGELOG.md) and [benchmark evidence](benchmarking.md) when updating the package.

There is **no PyPI release** and no promise of support SLAs. GitHub Issues is the appropriate forum for reproducible public bug reports using synthetic fixtures.
