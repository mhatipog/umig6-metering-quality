# Changelog

## 0.7.0 — 27 October 2026

### Correctness and evidence
- Added opt-in `TECH_SOURCE_PROVENANCE_V1`: separates trusted-UTC
  *assertions*, unresolved clock semantics and source-authored revisions from
  export snapshot/ingestion timestamps. The documentation explicitly states
  that caller metadata does not independently prove MIG6 compliance.
- Added a shuffle-free Bronze screen and separate quarantine lanes.
  Legacy feeds without a revision timestamp receive NULL, not a fabricated
  market correction time.
- Moved Belgian reporting-date enrichment behind provenance screening in
  the Bronze-first pattern.
- Rejected subsecond duration errors and nonfinite energy, including legacy
  readiness checks; refused to infer change sequences across conflicting
  intermediate revisions.
- Kept public P6269 local-wall-clock and strict UTC-label completeness
  distinct, following the observed DST source-label inconsistency.

### Performance and integration
- Benchmarked provenance and Bronze screening separately from full correction
  replay; added a four-sink benchmark that materialises current, rejected,
  superseded and changes.
- Added opt-in shared Spark intermediate persistence for multi-output replay,
  with explicit cleanup. See `docs/benchmarking.md` for measured evidence and
  separate synthetic versus real-data limitations.
- Updated Databricks batch example, quarantine review lanes and evidence
  documentation; introduced Spark 3.5 and 4.2 CI matrix.

Not official Atrias validation; not tested on production CMS exchanges or
Databricks Photon. No private customer data is published.

### Cross-version verification

- Normalised the Mijn Fluvius consumer CSV's supported day/month/year dates
  before a single strict timestamp parser, avoiding a Spark 3.5
  `SparkUpgradeException` from chained incompatible date formats.
  Two-digit report years follow the adapter's documented 20YY convention.
- [Spark 3.5.5 and 4.2.1 matrix](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40553118855):
  **86/86 tests passed** on each version after the parser correction.
- [Complete public Fluvius regression](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40549961842):
  84,326,400 rows, all 2,400 profiles locally complete, no official
  UTC-instant reconstruction.
