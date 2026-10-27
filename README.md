# UMIG 6 Metering Quality for PySpark

**PySpark validation, quarantine, asset-history checks and legacy migration-readiness profiling for Belgian energy data pipelines.**

Built for Spark jobs that ingest meter readings at scale and need to tell a trusted
record apart from a redelivery, a later correction, a contradictory update or a
real gap in an expected metering period.

The design follows publicly documented **UMIG 6 / MIG6 metering concepts**:
measurement identity, validation, estimated versus measured data, and corrections.
It does **not** parse proprietary CMS payloads, reproduce Atrias internal rules,
or claim official Atrias or Fluvius certification.

### Beyond metering: v0.3 modules (experimental)

- **Legacy readiness:** `evaluate_readiness(data, "METERING_REPLAY" | "ASSET_HISTORY" | "SUPPLIER_CHANGE")` reports row-level blockers/warnings and a distributed structural readiness score. Missing legacy columns become explicit failures rather than defaults. This is not certified UMIG conformance.
- **Asset history:** `validate_asset_history(assets)` validates point-to-meter assignment keys and periods. Possible overlapping assignments are separate **review** signals, not automatically rejected.
- **Cross-dataset check:** `reconcile_metering_assets(readings, accepted_assets)` flags metering observations with zero or multiple matching historical assignments. A temporal range join; filter to relevant slices before running at scale.
- **Quarantine pattern:** `envelope_quarantine(result.rejected, dataset="metering", batch_id="load_001")` attaches deterministic IDs and produces reason-level counts. Optional `write_delta_batch` demonstrates retryable per-batch Delta writes, **not atomic across two tables**.
- **Databricks benchmark harness:** `examples/benchmark_spark.py` generates synthetic Spark rows for measuring structural readiness on your cluster. No unverified throughput or Photon performance claims.

```python
from umig6_metering_quality import (
    process_metering, evaluate_readiness, validate_asset_history,
    reconcile_metering_assets, envelope_quarantine,
)

metering = process_metering(spark.read.table("bronze.metering"))
assets = validate_asset_history(spark.read.table("bronze.asset_history"))
readiness = evaluate_readiness(spark.read.table("bronze.old_contracts"), "SUPPLIER_CHANGE")
linked = reconcile_metering_assets(metering.current, assets.accepted)
quarantine = envelope_quarantine(metering.rejected, dataset="metering", batch_id="run_001")
# materialize using your platform's transactional/orchestration conventions
```

See [legacy migration scenarios](docs/migration-readiness.md), [Azure Databricks + Delta quarantine guidance](docs/databricks.md) and [Databricks batch example](examples/databricks_batch.py).

### Why this exists

Most examples of metering ETL stop at `dropDuplicates()` and null checks. That
loses information when an older reading is replaced, arbitrarily picks one
when conflicting messages arrive, and can flag thousands of false missing-data
alarms if the meter was not active during the period being checked.

This library is intentionally conservative:

- **Revision-aware replay.** The latest timestamped revision wins, but only when
  there is exactly one distinct observation at that revision. Identical re-deliveries
  are idempotent; conflicting revisions go to quarantine.
- **Evidence, not silent overwrites.** Separate `current`, `rejected`,
  `superseded` and `changes` DataFrames preserve an audit trail.
- **Scoped completeness.** Check missing and overlapping 15-minute intervals
  only against supplied active periods for a point, meter and register.
  The library never assumes every date in a year ought to have readings.
- **Belgian EAN format checks.** Opt-in/out validation of 18-digit point codes beginning `54`; identity remains distinct from a physical meter serial number.
- **Silver-layer enrichment.** Adds Belgian local reporting date/month and 15-minute average power in kW while keeping UTC instants as join keys.
- **UTC by construction.** Checks work across Belgium's 92/100-quarter
  daylight-saving days without inventing or discarding local timestamps.
- **Columnar Spark operations.** Window functions and joins; no Python UDF,
  network calls, `collect()` on production data or Delta Lake dependency.

### Install

```bash
pip install -e .                  # PySpark 3.5+ and Python 3.10+
pip install -e '.[test]'          # if you want to run the tests
pytest -q
```

For an actual Spark cluster, install the package on cluster nodes and match
Spark/Python versions to your managed runtime. Nothing is published to PyPI yet.

### Pipeline usage

```python
from umig6_metering_quality import process_metering

# Canonical silver candidate, e.g. adapted from a licensed source feed.
bronze = spark.read.parquet("/lake/bronze/metering")
scopes = spark.read.parquet("/lake/reference/active_meter_periods")
spark.conf.set("spark.sql.session.timeZone", "UTC")

result = process_metering(bronze, scopes=scopes)
result.current.write.mode("overwrite").parquet("/lake/demo/current")
result.rejected.write.mode("overwrite").parquet("/lake/demo/rejected")
result.superseded.write.mode("overwrite").parquet("/lake/demo/history")
result.changes.write.mode("overwrite").parquet("/lake/demo/changes")
result.coverage.write.mode("overwrite").parquet("/lake/demo/coverage")
```

The `overwrite` paths above are **example output snapshots**, not a production
write pattern. In production, persist immutable batch/run IDs and use an
idempotent merge or full rebuild for a materialized current-state table.

`examples/basic_pipeline.py` contains an end-to-end synthetic demonstration.

### Mijn Fluvius CSV adapter (v0.2, experimental)

An optional adapter supports Dutch-language **Mijn Fluvius 15-minute electricity
CSV reports**. These are *informative consumer exports*, not UMIG/CMS message
payloads or certified billing values. Source columns can be explicitly mapped
for another report layout; the adapter never guesses based on column position.

```python
from datetime import datetime, timezone
from umig6_metering_quality import read_fluvius_csv, process_metering

spark.conf.set("spark.sql.session.timeZone", "UTC")
loaded = read_fluvius_csv(
    spark, "/private/fluvius/quarter_hours.csv",
    snapshot_at_utc=datetime(2026, 10, 27, 8, 0, tzinfo=timezone.utc),
)
result = process_metering(loaded.readings)
# Inspect loaded.rejected AND result.rejected. No implicit coverage scope.
```

- Keeps `source_file`, original register, unit and validation status, plus
  `revision_basis=EXPORT_SNAPSHOT`. A newer export does **not** imply an
  official market-message revision.
- Accepts only recognized electricity import/injection registers and kWh.
  Unexpected units/registers/volume formats are quarantined, not guessed.
- Local timestamps are mapped to UTC; the repeated October 02:00 hour and
  nonexistent March 02:00 hour are quarantined when offsets are unavailable.
  This leaves affected periods incomplete rather than fabricating timestamps.
- Consumer CSV layouts can change. Unsupported headers fail explicitly.
  The exact current exports still need independent verification with authorised
  examples; no consumer data is bundled here.

Try `examples/fluvius_export_pipeline.py` with two CSV exports. A Spark
cluster, not a My Fluvius login or CMS access, is all that is required.

### Input contract

One row per observed interval **per revision**; source adapters must explicitly
normalize source units and UTC instants before calling the library.

| Column | Spark type | Meaning |
| --- | --- | --- |
| `ean` | string | Supply point identifier. Keep as string (leading zeros matter). |
| `meter_id` | string | Physical meter identity; may change during replacement. |
| `register` | string | Source-normalized metering register (e.g. `A+`, `A-`). |
| `interval_start_utc` | timestamp | Inclusive UTC start of observation. |
| `interval_end_utc` | timestamp | Exclusive UTC end; by default 15 minutes later. |
| `energy_kwh` | numeric | Volume in **kWh**, not kW; zero is valid. |
| `revision_at_utc` | timestamp, nullable | Time at which this observation version was issued/recorded. |
| `measurement_method` | string | Adapter-supplied status, e.g. `MEASURED` or `ESTIMATED`. |

Other columns from the source are preserved. A revision timestamp is a **source
contract**: if you only know ingestion time, label and document that limitation.
A later ingestion time is not proof of a later official meter-data revision.
The optional `validate_ean_format=False` disables Belgium-specific format checking
for synthetic or non-Belgian upstream identifiers.

Optional coverage contract (`scopes`): `scope_id`, `ean`, `meter_id`, `register`,
`valid_from_utc` (inclusive) and `valid_to_utc` (exclusive). `scope_id` must
identify a unique expected period. Scope metadata is caller-supplied and
is not inferred from the received readings. Partial and empty scopes are supported.

### Output contracts

| Dataset | Meaning |
| --- | --- |
| `current` | Unambiguous latest observation per interval key, enriched with `local_date_be`, `local_month_be`, `avg_power_kw` |
| `rejected` | Invalid input rows or conflicting latest revision values, with `quality_errors` array |
| `superseded` | Older valid observation versions, retained for audit, also enriched |
| `changes` | Previous and next observations when kWh or method changed |
| `coverage` | Gaps and overlaps *within explicitly declared periods*; optional |

Known limitations: this is a **metering-data quality library**, not a MIG6 XML
validator or a billing decision engine. The caller must supply canonical fields,
trustworthy version precedence, and accurate scope periods. It deliberately does
not assign official UMIG rectification codes, estimate missing values, assume a
supplier contract, or claim a record is commercially validated.

### Reproducibility

Tests cover negative/missing values, intervals, idempotent redeliveries,
revisions, ambiguous concurrent corrections, explicit scope gaps, missing
scopes, and both Belgian daylight-saving transitions. They use synthetic data
only. See `docs/architecture.md` for design decisions and correctness boundaries.

### Pinned public metering rulebook (v0.4)

`screen_continuous_measures()` adds a **historically pinned**, spec-referenced
prerequisite screen for continuous metering values: measurement presence and
caller-supplied readout digit-count compatibility. It returns separate
`evaluated`, `quarantine`, `review` and `summary` DataFrames. Missing digit
metadata becomes `INCOMPLETE`, **not** a false PASS. Passing is never an
official market validation level or certification. See
[`docs/public-rulebook.md`](docs/public-rulebook.md) for the 6.5.2.2 reference,
limitations and code examples.

A reproducible synthetic benchmark now covers readiness, this pinned rulebook,
and the full correction pipeline. The benchmark workflow records its environment
and plan fingerprint; it is **not** an Azure Databricks or live Fluvius
performance result. See [`docs/benchmarking.md`](docs/benchmarking.md).

### Documentation used

- [Atrias current UMIG market documentation](https://www.atrias.be/current-documentation)
- [Atrias market-party onboarding and UMIG 6 overview](https://www.atrias.be/new-members)
- [Fluvius explanation of EAN identities](https://www.fluvius.be/nl/veelgestelde-vragen/veelgestelde-vragen-over-de-ean-code)

This repository contains no copied Atrias specifications, proprietary XML,
customer extracts, EANs of real supply points, credentials or private project code.

### Contributing

Issues and tested pull requests welcome, especially additional adapter examples
and reproducible edge cases. Please use synthetic or fully authorised data.
Do not upload customer meter readings or confidential company documentation.

### License

MIT. Not affiliated with Atrias, Fluvius or any energy-market operator.

### Real Fluvius 2024 data benchmark

**Measured on all 84,326,400 official 2024 quarter-hours:** 229.913 seconds of
Spark quality processing on a GitHub-hosted local runner; 0 row-level violations
under the implemented checks. [Benchmark evidence and limitations](docs/public-fluvius-benchmark.md).
No claims about Databricks or actual CMS conformance are implied.

The [public P6269 profile adapter](docs/public-fluvius-benchmark.md) screens
all eight official 2024 CSV categories at source-row grain. It uses no private
CMS data, makes no fabricated physical-meter assumptions, preserves source ISO-8601 UTC instants and quarantines local-only
DST ambiguity and reports source SHA-256 and aggregate quality metrics.

```python
from umig6_metering_quality.public_profiles import (
    read_public_profiles, screen_public_profiles, public_profile_metrics,
)
checked = screen_public_profiles(read_public_profiles(spark, "/mnt/P6269_sample.csv"))
checked.valid.write.format("delta").saveAsTable("quality.public_profiles_valid")
checked.quarantine.write.format("delta").saveAsTable("quality.public_profiles_quarantine")
public_profile_metrics(checked).show()
```

Run `python examples/benchmark_public_fluvius.py --max-files 8` for reproducible
full-corpus metrics. These are anonymised *profiles*, not UMIG 6 messages.

#### Check full-year completeness (no false confidence from row counts)

```python
from umig6_metering_quality import profile_coverage
coverage = profile_coverage(checked)
# Literal Z-labelled timestamps are NOT unique during the source's fall DST hour.
problems = coverage.where("coverage_status = 'INCOMPLETE'")
```

This exact per-profile distinct count is a separate Spark shuffle workload;
see the [real-data benchmark](docs/public-fluvius-benchmark.md) for its cost.

### Public-source timestamp warning: DST (v0.5)

The official P6269 2024 CSV contains clock labels ending in `Z`, but
those strings have the 92/100-quarter pattern of Belgian *local time*:
all four `2024-03-31T02:xxZ` labels are absent and all four
`2024-10-27T02:xxZ` labels appear twice for the same anonymous profile.
This contradicts interpreting the labels as unique UTC instants.
The adapter marks `source_utc_label_unverified=True`; **do not use its
`interval_start_utc` column as an authoritative UTC join key.**

The independent `profile_local_clock_coverage()` check now validates local
calendar completeness, including both DST transitions, without manufacturing
a UTC offset. Strict `profile_coverage()` remains available to test what
happens if the source's UTC suffix is interpreted literally. These are
*different questions*. See [the reproducible diagnosis](docs/p6269-clock-semantics.md).

**Measured local-clock audit:** 300/300 public profiles complete on one official
10,540,800-row category; 65.466 s for exact local-clock coverage, separate
from 51.415 s for row-level screening (GitHub-hosted PySpark 4.2.0).
[Reproducible run](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40529787949).
The entire eight-category clock audit is a separate benchmark.

### Source provenance and legacy MIG-readiness gate

For versioned metering and legacy system migrations, use the technical
`screen_source_provenance()` gate **before** time-based joins or revision
deduplication. It separates source-timestamp claims from verified UTC instants
and export/ingestion timestamps from source-authored revisions:

```python
from umig6_metering_quality import screen_source_provenance, process_metering

evidence = screen_source_provenance(bronze_with_origin_metadata)
evidence.review.write.format("delta").mode("append").saveAsTable("quality.provenance_review")
evidence.quarantine.write.format("delta").mode("append").saveAsTable("quality.provenance_quarantine")
result = process_metering(evidence.replay_ready)
```

The adapter/operator must explicitly supply `source_system`,
`source_record_id`, `source_evidence_ref`, `timestamp_semantics` and
`revision_basis`. Only source revisions with non-null `revision_at_utc`
are replay eligible. P6269's ambiguous UTC-labelled timestamps are
**not** eligible for trusted UTC joins. See [rule provenance,
source contracts, and what PASS does not prove](docs/rule-provenance.md).

Interval-duration checks are exact to microseconds and non-finite energy
values are rejected. These are **technical data-quality rules**, not published
MIG6 rectification codes or grid-operator validation thresholds.

### Fast Bronze screening versus revision replay

```python
from umig6_metering_quality import screen_metering_rows, screen_source_provenance, process_metering

bronze = screen_metering_rows(normalized_readings)
origin = screen_source_provenance(bronze.accepted)
# The remaining rows are withheld for review/quarantine, not silently deleted.
replay = process_metering(origin.replay_ready)
```

This separates cheap structural checks from evidence gating and the
expensive distributed correction replay. `screen_metering_rows()` does
**not** deduplicate source rows or certify migrated data. Keep its quarantine,
the provenance review/quarantine, and the revision-replay outputs as distinct
Delta sinks with an explicit completion barrier.

The [measured benchmark stages](docs/benchmarking.md) are **different
operations**: a fast pre-screen does not replace correction validation.

### v0.7: provenance-first ingestion and repeatable Spark benchmarking

- Source provenance and MIG6-rule provenance are separately documented:
  [evidence register](docs/rule-provenance.md), pinned UMIG handbook profile
  versus **project-defined** legacy quality rules.
- Legacy Bronze snapshots can be screened even if they do not contain a
  revision timestamp; absence remains NULL and is never invented.
- Exact UTC interval-duration and nonfinite kWh checks, including the
  legacy migration-readiness preflight.
- No local reporting-date enrichment occurs before timestamp provenance
  screening in the fast Bronze stage.
- An opt-in `cache_shared=True` full correction path saves repeated Spark
  work when a caller materialises multiple sinks, with
  `result.release_cache()` or a context manager for cleanup.
- The [complete four-output benchmark](docs/benchmarking.md) is measured
  separately from the simpler two-output benchmark. Revision collisions
  and Spark 3.5 / 4.2 compatibility are tested independently.

This is not an Atrias-certified package, and no private MIG6 exchange feed
has been used to validate market-message conformance.


**Measured correction replay with real revision conflicts?** No: the latest
source-revision performance case is still *synthetic*, although it contains
98,304 revised observations with changed kWh. At 8 shuffle partitions,
four-output runtime was 59.418 s uncached versus 32.426 s with an opt-in
shared-history cache (Spark 4.2.1 local). The raw Fluvius 2024 profiles do
not include revision history, so performance and correctness of actual CMS
correction messages remain unverified.
[Detailed reproducible performance evidence](docs/benchmarking.md).
