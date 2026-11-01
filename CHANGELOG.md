# Changelog

## Unreleased

- Verified four-lane synthetic replay and fault-injected Delta recovery in managed Fabric and Databricks.
- Added checksum-verified staging and real Spark screening for the original public Fluvius P6269 data; full Databricks corpus and partial Fabric corpus documented with reproducible evidence.
- Added Spark input-split experiments with unchanged correctness invariants.
- Simplified project documentation; detailed measurements remain in `evidence/`.

## 0.8.0 — 27 October 2026

- Added explicit source-provenance and revision authority checks, with separate review and quarantine paths.
- Improved DST, interval and energy screening, correction replay, asset-history checks and Delta batch handling.
- Published repeatable synthetic and official public-dataset benchmarks; expanded Spark 3.5/4.2 CI.

## 0.7.0 — 27 October 2026

- Introduced provenance-gated Bronze-to-Silver replay and optional shared Spark caching.
- Added source adapter, migration-readiness scenarios, basic quality rules and cross-version parser tests.

These are project-level data-quality controls, not official UMIG certification.
