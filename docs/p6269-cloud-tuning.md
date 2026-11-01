# Managed Spark read-tuning experiment: Fluvius P6269 2024

The experiment compares **configurations within each runtime**. It is not
an apples-to-apples Fabric-versus-Databricks cost or runtime ranking.

## Real input and acceptance contract

Two source ZIP archives from the public Fluvius Open Data P6269 catalog,
verified against the published SHA-256 fingerprints in
[the full-corpus evidence](../evidence/2026-11-01-databricks-p6269-full-corpus.json),
contain eight original CSV files, **84,326,400 quarter-hour observations**
across **2,400 anonymized profiles**.

Run each official four-file archive separately when the free-tier compute
limits prevent an eight-file action. After both parts pass, aggregate the
results only when their archive hashes, code commit and configuration match.
Do not present two independent jobs as one monolithic Spark job.

The benchmark performs all three workloads for each file:

1. Original P6269 source screening and import/injection volume aggregates.
2. Strict UTC-label uniqueness diagnostics.
3. Exact Belgian local-clock coverage, including the repeated autumn hour.

It checks all row, rejection, profile, and energy-total evidence **after
every configuration**. A purported speed improvement fails acceptance if
one invariant differs.

## Controlled variable

The first matrix varies **`spark.sql.files.maxPartitionBytes`**, which changes
input scan tasks without changing the algorithm:

| Experiment | File split target | Hypothesis |
| --- | ---: | --- |
| High parallelism | 64 MiB | More tasks; benefits if scan parsing saturates CPU |
| Platform default | 128 MiB | Reference, often a sensible balance |
| Larger tasks | 256 MiB | Less scheduler overhead, may hurt due to spill/underutilization |

Use `examples/p6269_tuning.py` with a real Spark session and **four staged
original CSV paths**, or eight when capacity permits:

```bash
python examples/p6269_tuning.py --archive 0 \
  --csv source-0-category-{0,1,2,3}.csv \
  --sizes 128 64 256 --repeats 2 --output archive0-spark.json
```

Those shell paths are illustrative: use the correct fully qualified
OneLake ABFS/Files paths on Fabric and governed Volume paths on Databricks.
No local `SparkSession.master('local')` override is used.

## Interpretation and controls

- Report data staging separately from Spark read/validation/coverage times.
- Record whether each configuration value actually took effect.
- Prefer repeated within-engine measurements; first-run cache, adaptive
  scheduling, competing jobs and dynamic executor allocation confound timing.
- **Do not hard-code executor cores/memory on Databricks serverless.** The
  vendor manages that compute. Supported SQL file-splitting options are
  more defensible for this comparison.
- AQE may be enabled by default; it should be captured, not implied to be a
  newly enabled speed optimization.
- Fabric capacity and Spark job admission can be shared with other work.
  Time spent waiting for admission is not the Spark action time.
- V-Order is a *Delta write layout feature*, not an input CSV parser boost;
  it should not be conflated with this benchmark.
- No Photon/NEE advantage may be claimed without an actual observed engine
  mode and measurement. Neither a speed ratio nor cost ratio can be
  extrapolated from one uncontrolled execution.

Source timestamp labels resemble UTC but follow Belgian local daylight
saving behavior; no configuration can invent authoritative UTC offsets.
The original public sample is not a native MIG6 message exchange, does not
validate billing, and should not be described as such.

Cross-engine reproducibility requires matching all eight-file evidence
against [the Databricks baseline](../evidence/2026-11-01-databricks-p6269-full-corpus.json).
Publish only completed measurements, not skipped runs or tuning hypotheses.
