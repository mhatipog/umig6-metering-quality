# UMIG6 Metering Quality for PySpark

[![Tests](https://github.com/mhatipog/umig6-metering-quality/actions/workflows/ci.yml/badge.svg)](https://github.com/mhatipog/umig6-metering-quality/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Spark](https://img.shields.io/badge/PySpark-3.5%20%7C%204.2-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Reusable PySpark data-quality, source-provenance, quarantine, reconciliation, and legacy migration-readiness checks for Belgian energy datasets.**

Built for Spark/Databricks data pipelines that ingest interval meter readings and need to separate malformed rows, ambiguous revisions, uncertain source timestamps and actual changes without losing the audit trail.

This is **MIG6 / UMIG 6-aware engineering software, not an official MIG6 validator**. It does not parse native market messages, reproduce unpublished DGO thresholds, certify billing values, or establish Atrias compliance. See [rule provenance](docs/rule-provenance.md).

## What it does

| Module | Purpose | Key output |
| --- | --- | --- |
| `screen_metering_rows` | Fast, row-level interval/identity/energy checks | accepted / quarantine / summary |
| `screen_source_provenance` | Gate timestamps, source identity, evidence references and genuine revision claims | analytics-ready / replay-ready / review / quarantine |
| `process_metering` | Resolve source revisions without choosing conflicting values; retain changes and older versions | current / rejected / superseded / changes / optional coverage |
| `coverage_issues` | Check gaps and overlaps against **caller-supplied active periods** | time-window issues |
| `validate_asset_history`, `reconcile_metering_assets` | Validate meter-to-EAN effective dating and annotate uncertain historical joins | accepted / rejected / review signals |
| `evaluate_readiness` | Structural preflight for `METERING_REPLAY`, `ASSET_HISTORY`, `SUPPLIER_CHANGE` | ready / blockers / quarantine / summary |
| `envelope_quarantine`, `write_delta_batch` | Stable batch-scoped evidence identifiers and retryable Delta table writes | quarantine and per-reason counts |
| `screen_continuous_measures` | Narrow checks linked to a **historically pinned** public UMIG rule handbook | PASS / FAIL / INCOMPLETE |
| `read_fluvius_csv`, `read_public_profiles` | Adapters for supported consumer CSVs and the public P6269 research dataset | normalized/checkable observations |

**Important:** structural validation is not equivalent to official market acceptance. An `EXPORT_SNAPSHOT` is not a `SOURCE_REVISION`; a local time with a `Z` suffix is not necessarily verified UTC. The library can quarantine or defer uncertain evidence rather than guess.

## Install

No PyPI release has been published. Install from this repository on the cluster/driver environment:

```bash
git clone https://github.com/mhatipog/umig6-metering-quality.git
cd umig6-metering-quality
python -m pip install -e .
python -m pip install -e '.[test]'
pytest -q
```

Requires Python 3.10+ and PySpark 3.5+ (tested on **3.5.5 and 4.2.1**, 93 tests each as of 27 October 2026). Match Python, Java and Spark versions to your Databricks Runtime. The package does not create or configure a cluster.

## Minimal Databricks/Spark example

The input here is **already normalised** and contains the documented canonical metering columns plus operator-supplied source-provenance metadata. An authorised adapter must interpret original timestamp and revision semantics first.

```python
from umig6_metering_quality import (
    screen_metering_rows,
    screen_source_provenance,
    process_metering,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
source = spark.read.table("energy_bronze.metering")

bronze = screen_metering_rows(source)
evidence = screen_source_provenance(bronze.accepted)

# Do not discard bronze.quarantine, evidence.review or evidence.quarantine.
# True source revisions only: export snapshot timestamps are not corrections.
with process_metering(
    evidence.replay_ready,
    require_source_revision=True,
    cache_shared=True,      # Optional: useful if materialising multiple sinks
) as resolved:
    resolved.current.show(5, truncate=False)
    resolved.rejected.show(5, truncate=False)
    resolved.superseded.show(5, truncate=False)
    resolved.changes.show(5, truncate=False)
```

The `cache_shared` context releases shared Spark persistence. `.show()` is for a demonstration, not an appropriate sink for sensitive production data. Retain immutable Bronze originals and write each review/quarantine lane to access-restricted, versioned Delta tables. **Independent Delta writes are not an atomic multi-table transaction**; only expose a completed batch after an orchestrator confirms every required output.

[Full Databricks batch example](examples/databricks_batch.py) · [Delta/quarantine operations](docs/databricks.md) · [Architecture](docs/architecture.md)

## Evidence and performance

Real-data and synthetic benchmarks answer **different questions**:

- **Public Fluvius 2024 corpus:** 84,326,400 quarter-hour rows across 2,400 anonymised profiles, read directly from the official ZIP attachments. Row-grain checks found no violations under their implemented technical rules. The **2,400 profiles are complete by local Belgian clock rules**; the timestamps are *not independently verified as UTC instants*. [Dataset analysis and full run](docs/public-fluvius-benchmark.md).
- **Correction-heavy synthetic replay:** 1,000,000 generated observations including 98,304 changed historical revisions. Materialising all four replay outputs at eight shuffle partitions took **59.418 seconds without caching** and **32.426 seconds with opt-in shared caching**, single runs on GitHub-hosted local Spark 4.2.1. These are not Azure Databricks/Photon timings. [Benchmark method, limitations and logs](docs/benchmarking.md).
- **CI:** [93 tests pass on PySpark 3.5.5 and 4.2.1](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40557078089).

Nothing in these results establishes official MIG6 message conformance or performance on actual production MDM/CMS history.

## Provenance and DST warning

The public P6269 2024 CSV has repeated `02:00–02:45` labels on 27 October and no such labels on 31 March, while its timestamps include a `Z` suffix. This matches **Belgian local-clock DST behaviour** but does not establish unique UTC instants. The source labels remain unverified, and the separate `profile_local_clock_coverage()` check validates the local clock without fabricating a UTC mapping.

In source-authored correction pipelines, call `screen_source_provenance()` first and run `process_metering(..., require_source_revision=True)` on its `replay_ready` subset. [Detailed evidence register](docs/rule-provenance.md) · [DST diagnosis](docs/p6269-clock-semantics.md).

## Documentation

- [Quick operational handoff](docs/HANDOFF.md): safe uses, limitations, adoption checklist and reproducibility.
- [Architecture and input assumptions](docs/architecture.md)
- [Source and validation-rule provenance](docs/rule-provenance.md) · [Pinned historical rule profile](docs/public-rulebook.md)
- [Legacy-data scenarios](docs/migration-readiness.md)
- [Azure Databricks and quarantine](docs/databricks.md)
- [Real Fluvius data benchmark](docs/public-fluvius-benchmark.md) · [Spark performance benchmarks](docs/benchmarking.md)
- [Changelog](CHANGELOG.md) · [CI tests](.github/workflows/ci.yml)

## Contributing and privacy

MIT-licensed, public, and unaffiliated with Atrias, Fluvius or other energy-market participants. Issues and tested pull requests are welcome. Use synthetic or authorised public data only; never post actual customer EANs, measurements, confidential UMIG artefacts, credentials or internal source contracts in GitHub issues, CI logs or pull requests. Quarantine outputs may contain sensitive raw data and require restricted access.
