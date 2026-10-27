# UMIG 6 Metering Quality for PySpark

**Audit-ready data quality checks for versioned electricity metering data in Belgian analytics pipelines.**

Built for Spark jobs that ingest meter readings at scale and need to tell a trusted
record apart from a redelivery, a later correction, a contradictory update or a
real gap in an expected metering period.

The design follows publicly documented **UMIG 6 / MIG6 metering concepts**:
measurement identity, validation, estimated versus measured data, and corrections.
It does **not** parse proprietary CMS payloads, reproduce Atrias internal rules,
or claim official Atrias or Fluvius certification.

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

Try `examples/fluvius_export_pipeline.py` with two CSV exports stored at
**different immutable paths**. Spark reads lazily: overwriting a path after
building a DataFrame can corrupt a historical comparison. A Spark cluster,
not a Mijn Fluvius login or CMS access, is all that is required.

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
