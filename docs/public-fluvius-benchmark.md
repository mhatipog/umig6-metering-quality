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
