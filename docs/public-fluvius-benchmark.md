# Original Fluvius P6269 benchmark

Fluvius publishes two ZIP attachments containing eight category CSVs: **84,326,400 quarter-hour observations**, 2,400 anonymised 2024 profiles, about 6.39 GB extracted. The [original public dataset](https://opendata.fluvius.be/explore/dataset/1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar/) is **not** a native UMIG 6 message feed.

## Key finding: source clock labels

The CSV timestamps end in `Z`, yet follow Belgian **local** daylight-saving time. On 31 March 2024, 02:00–02:45 is absent; on 27 October those quarter-hour labels appear twice. Consequently:

- All **2,400** profiles pass exact local-clock coverage (92/96/100 quarter-hours per day).
- All **2,400** fail strict UTC-label uniqueness if the apparent `Z` is treated as an authoritative UTC instant.
- The repeated autumn hour cannot be assigned a unique UTC offset from the CSV alone.

The library reports both interpretations; it does not silently repair the source. [DST method](p6269-clock-semantics.md).

## Verified runs

| Execution | Rows checked | Scope |
| --- | ---: | --- |
| [Local Spark, full corpus](https://github.com/mhatipog/umig6-metering-quality/actions/runs/40549961842) | 84,326,400 | Original ZIPs; local-clock verification |
| [Databricks serverless, full corpus](../evidence/2026-11-01-databricks-p6269-full-corpus.json) | 84,326,400 | Two jobs, four CSVs each |
| [Fabric Spark, first archive](../evidence/2026-11-01-fabric-p6269-archive0.json) | 42,163,200 | Four CSVs; second archive not screened |

Every checked row passed this project's **structural** checks; that does not establish billing validity or market certification. Original ZIP and member SHA-256 values appear in the managed evidence files. Timing excludes transfer and cluster startup.

## Run it

```bash
pip install -e .
python examples/benchmark_public_fluvius.py --max-files 1 --output sample.json
python examples/benchmark_public_fluvius.py --max-files 8 --output full.json
```

The script fetches the official public archives; it does not commit raw CSVs. For managed clusters, stage verified members first with [the staging utility](../examples/cloud_stage_fluvius.py). See [managed evidence](cross-platform-evidence.md) and [scan tuning](p6269-cloud-tuning.md).
