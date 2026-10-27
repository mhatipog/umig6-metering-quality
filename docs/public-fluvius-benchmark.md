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
of the valid subset. The official archive uses UTC ISO-8601 timestamps with a `Z` suffix,
so repeated local 02:00 readings can be distinguished. Local-time-only
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

The real source has an ISO-8601 `Datum_Startuur` in UTC (`Z` suffix), not
ambiguous Brussels wall times. The parser preserves this offset. The original
consumer CSV adapter for local times uses separate, conservative DST handling.
All measurements are from the original public ZIPs, not a synthetic workload.
