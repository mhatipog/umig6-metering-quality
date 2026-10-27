# Benchmarking on Fluvius's public 2024 meter profiles

Fluvius publishes 2,400 *anonymised* quarter-hour residential profiles across
8 categories (PV, heat pump, EV combinations). The two official P6269 ZIP
attachments hold eight large CSV files, approximately 6.39 GB uncompressed.
This dataset is **not** a UMIG6/CMS message exchange. Public profile IDs
are not contractual EANs or physical meter IDs.

The reproducible benchmark downloads only from the official Opendatasoft
Fluvius dataset endpoint, records SHA-256 of each ZIP, safely extracts one
CSV member at a time, and uses Spark-native projections and a single aggregate
per file. No Python UDF or per-reading collection on the driver is needed.
No customer-level measurements are committed to GitHub or published in
workflow artifacts.

Run it on a machine with enough free disk space, Java and Spark installed:

```bash
pip install -e .
python examples/benchmark_public_fluvius.py --max-files 1 --output sample.json
python examples/benchmark_public_fluvius.py --max-files 8 --output full.json
```

For each input row, the quality engine checks source identity, parseable
quarter-hour timestamps, explicit UTC timestamps (or unambiguous Europe/Brussels local time), import and
injection energy values, and interval alignment. It keeps anomalous rows out
of the valid subset. The official archive labels its timestamps with the UTC `Z` suffix, but the 2024 DST pattern contradicts treating those labels as reliably unique UTC instants. The two October 02:xx observations share identical timestamp strings and **cannot** be distinguished from the suffix alone. Local-time-only
exports lacking an offset remain explicitly quarantined at fall DST. This is a deliberate limit, not proof
that Fluvius's data is wrong.

Interpretation:

- A high accepted fraction is **not** UMIG6 certification or proof of billability.
- Missing source rows cannot be discovered without a separately supplied scope.
- The benchmark is single-snapshot validation; there are no revision histories
  in this public source. Do not manufacture official correction semantics.
- Report Spark/runtime, archive checksums, source count, passed/quarantined
  counts, and wall time. Do not extrapolate local Spark to Photon/Databricks.
- Compute cost, disk throughput, memory pressure and the Databricks runtime
  must be measured separately on the target platform.

[Dataset and licence](https://opendata.fluvius.be/explore/dataset/1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar/).

## Independent coverage workload

The optional `--check-coverage` measures the number of unique UTC intervals
per anonymised profile, not just the number of rows. In 2024 the expected
count is **35,136** quarters (366 days). The check also verifies the first
and last UTC timestamps and ensures no quarantined rows occur for a profile.
It can therefore distinguish a complete profile from one where a duplicate
observation hides a missing interval. This exact distinct counting requires
a Spark shuffle and is timed **separately** from row-wise validation.

```python
from umig6_metering_quality import profile_coverage
coverage = profile_coverage(checked)
coverage.where("coverage_status = 'INCOMPLETE'").show()
```

## Verified full-corpus measurement — 27 October 2026

[GitHub Actions run 40510590838](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40510590838),
commit `cce9911363ce1309d535474a319d855706c54aea`:

- **84,326,400 actual observations** from all eight 2024 category CSVs.
- **84,326,400 passed row-level checks; 0 quarantined.** This is a curated
  complete-profile sample, not a measure of real-world production error rates.
- **229.913 seconds** summed quality processing time, excluding network ZIP
  transfer and extraction; about **366,775 rows/second** over eight stages.
- Spark 4.2.0, GitHub-hosted Ubuntu runner, local execution, no Photon or
  Databricks compute. Separate jobs may have different runner specifications.
- First ZIP SHA-256: `0f8addd57bd9f7bc0fd0c28b019a4b6c8753a19e3fd301de758f5a5858c9d86c`
- Second ZIP SHA-256: `cfef37973c40e503fe53afa4966bbdcd7a12525fb8b20881f365d0ee8616a3d6`

| Source category | Rows | Quality processing (seconds) |
|---|---:|---:|
| Heat pump, no PV | 10,540,800 | 31.526 |
| Heat pump, PV | 10,540,800 | 28.423 |
| PV only | 10,540,800 | 28.527 |
| Neither PV nor heat pump | 10,540,800 | 28.538 |
| EV, no PV | 10,540,800 | 27.924 |
| EV, PV | 10,540,800 | 28.113 |
| Heat pump + EV, no PV | 10,540,800 | 28.347 |
| Heat pump + EV + PV | 10,540,800 | 28.515 |

The real source has an ISO-8601 `Datum_Startuur` string with a `Z` suffix; its DST calendar nevertheless behaves like Brussels wall time. The parser preserves the literal source label, but the parsed `interval_start_utc` must **not** be used as a trusted UTC instant until the data owner establishes the clock semantics. The original
consumer CSV adapter for local times uses separate, conservative DST handling.
All measurements are from the original public ZIPs, not a synthetic workload.

## Reproduced source-clock conflict and two independent checks

A public 2024 profile has 35,136 rows but just 35,132 distinct UTC-labelled
clock strings. The four absent labels are `2024-03-31T02:00/15/30/45Z`;
the four duplicated labels are `2024-10-27T02:00/15/30/45Z`.
The [diagnostic run](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40527488281)
confirms this from the unmodified official ZIP.

`profile_coverage()` measures strict UTC-label uniqueness. The separate,
Spark-native `profile_local_clock_coverage()` checks **exact local clock
multiplicities** (92 / 96 / 100 quarters by day), flags any missing clock
slot, and refuses a duplicate hiding a missing slot. It never guesses which
of the repeated autumn readings belongs to the first versus second offset.
The full-corpus benchmark now runs both checks and measures their times
separately. [Background and boundaries](p6269-clock-semantics.md).

## Final source-clock regression on the complete 2024 corpus

[GitHub Actions run 40549961842](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40549961842)
repeated the complete eight-file run after adjusting the public P6269 source
adapter so `local_start_be` reflects the raw source's local clock label rather
than an unjustified UTC-to-Brussels conversion.

- 84,326,400 public source rows, 84,326,400 passed row-level quality checks;
  zero quarantined under the implemented structural rules.
- 2,400 of 2,400 anonymised profiles satisfied the exact local-wall-clock
  calendar (including spring 92-quarter and autumn 100-quarter days).
- 2,400 of 2,400 **still fail strict UTC-instant uniqueness** if the
  source's `Z` suffix is taken literally. They are not certified UTC instants.
- 368.027 seconds summed row screening on this GitHub-hosted Spark 4.2.0
  execution, excluding archive transfer and the separate coverage actions.
  The earlier 229.913-second run used a different hosted execution and is
  **not** a controlled before/after performance comparison.

No source records or anonymised profile IDs were published in artifacts.
Both coverage checks are intentionally reported, not reconciled by guessing
which repeated October observation maps to which offset.
