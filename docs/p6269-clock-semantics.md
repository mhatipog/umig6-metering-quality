# P6269: source clock labels versus UTC instants

**Observed on the real Fluvius 2024 P6269 CSV:** an anonymised profile contains
35,136 rows, but only 35,132 distinct timestamp strings ending in `Z`.

- Four absent labels: `2024-03-31T02:00/15/30/45:00.000Z`.
- Four twice-occurring labels: `2024-10-27T02:00/15/30/45:00.000Z`.
- Those dates are precisely the Brussels spring and autumn DST transitions.

This indicates the source labels follow **local Belgian clock rules despite the
UTC-looking suffix**. The source does not provide independently verified UTC
offsets for the repeated October hour. We preserve its clock text and do **not**
silently invent offsets.

## Two different assertions

* `profile_coverage()` checks whether the source's declared timestamps form a
  complete and unique UTC timeline *if taken literally*. For the official CSV
  this remains `INCOMPLETE` (four duplicated and four absent labels per profile).
* `profile_local_clock_coverage()` instead checks the exact local wall-clock
  calendar: 96 quarters on normal days, 92 on the last March Sunday, and 100 on
  the last October Sunday, with each repeated 02:xx quarter counted exactly twice.
  `COMPLETE_LOCAL_CLOCK` does **not** establish absolute UTC instants.

The local-clock assessment uses native Spark groupBy aggregations, detects
duplicates masking missing quarters, and is benchmarked separately from the
single-pass validation workload. An organisation must establish timestamp
semantics and first-versus-second October hour identity before using records
as UTC event-time joins in an analytics or MIG6 migration pipeline.

Evidence: the official P6269 2024 dataset, identified via its published ZIP
attachments, and the [GitHub diagnostic run](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40527488281).

## Verified real-data smoke — 27 October 2026

[GitHub Actions run 40529787949](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40529787949)
used one unmodified 2024 P6269 CSV, containing 10,540,800 records and 300
anonymous profiles. **All 300 were locally complete** with no unexpected
clock slots. The dedicated local-clock check took 65.466 seconds; row-wise
validation took 51.415 seconds (Spark 4.2.0, local mode, GitHub-hosted runner).
This proves completeness only for this tested category and the specific
source-clock contract; UTC offset reconstruction remains unresolved.
