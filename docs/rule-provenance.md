# Evidence and rule provenance

This toolkit distinguishes **a row passing a project technical check** from
**a meter value passing an authorised market validation process**.

Atrias publicly documents UMIG 6 processes. Its historic 6.5.2.2
`UMIG - HB - ME - 03 - Validation Rules` handbook describes validation
methodology and minimum continuous-measure prerequisites. Importantly,
the handbook states that *detailed validation rules and tolerances can vary
by DGO and are not provided in the handbook*. The toolkit **does not**
implement such unpublished parameters.

## Rule origin register

| Component | Origin | What a PASS establishes | What it does not establish |
|---|---|---|---|
| `screen_continuous_measures()` | Historic public UMIG 6.5.2.2 handbook, section 5.1.2 (pinned) | Local presence / supplied meter readout-digit prerequisite | Market-assigned V1/V2/V3 validation, DGO threshold compliance, billability |
| `process_metering()` | Project technical checks | Valid structure, interval length, unambiguous revision precedence *assuming source revision metadata is correct* | CMS ordering, settlement outcome |
| `screen_source_provenance()` | **Project technical profile `TECH_SOURCE_PROVENANCE_V1`** | Caller supplies source identity, a reference to timestamp evidence, recognised UTC semantics, and a declared revision basis | That the evidence is genuine, official MIG6 message validity, or that an operator audited it |
| `evaluate_readiness()` | Project migration preflight | Structural readiness of supplied legacy tables | Executability of MIG6 scenarios |
| `profile_local_clock_coverage()` | Project dataset-specific quality check | P6269 has expected Belgian wall-clock labels on each calendar day | Authoritative mapping to UTC instants |

Sources: [Atrias current market documentation](https://www.atrias.be/current-documentation),
[Atrias onboarding / message protocols](https://www.atrias.be/new-members),
[official Fluvius P6269 public data](https://opendata.fluvius.be/explore/dataset/1_50-verbruiksprofielen-dm-elek-kwartierwaarden-voor-een-volledig-jaar/).

## Technical source-provenance gate

`screen_source_provenance()` deliberately does **not** infer an official
revision from ingestion time or an exported CSV snapshot. Its input contract
requires the original system's identity, a stable source record ID, an evidence
reference and explicit timestamp/revision semantics.

`timestamp_semantics` must be one of:
- `UTC_VERIFIED`: caller checked original system's timezone contract;
- `OFFSET_VERIFIED`: source offset is documented and canonical UTC was derived;
- `UTC_LABEL_UNVERIFIED`: a timestamp carries a UTC-looking label whose meaning
  is not established (this applies to the public P6269 clock example);
- `LOCAL_UNRESOLVED`: local clock instant is ambiguous or its offset unknown;
- `UNKNOWN`: insufficient metadata.

`revision_basis` is `SOURCE_REVISION`, `EXPORT_SNAPSHOT`,
`INGESTION_TIME` or `UNKNOWN`.

The profile returns three disjoint dispositions:

- `analytics_ready`: structurally sufficient *asserted evidence* for joining
  canonical verified instants; not official market validation;
- `review`: incomplete context, no UTC claim or missing origin evidence;
- `quarantine`: inconsistent/unsupported provenance metadata.

`replay_ready` is a **subset of analytics_ready** and additionally requires
`SOURCE_REVISION` and a typed non-null `revision_at_utc`. This prevents
mistaking successive exports for supplier-visible market corrections. A self-
declared `UTC_VERIFIED` plus an arbitrary document string **is not independent
evidence**; operators must audit the referenced source contract.

Example:

```python
from umig6_metering_quality import screen_source_provenance, process_metering

screen = screen_source_provenance(bronze)
screen.review.write.format("delta").mode("append").saveAsTable("dq.provenance_review")
screen.quarantine.write.format("delta").mode("append").saveAsTable("dq.provenance_errors")

# Only if the referenced upstream source contract was independently checked:
trusted_revisions = screen.replay_ready
metering = process_metering(trusted_revisions)
```

Do not run the revision engine on `screen.analytics_ready` indiscriminately:
an `EXPORT_SNAPSHOT` is not an official source revision.

All checks are Spark-native projections. The summary aggregate and user output
writes are separate Spark actions; they must be timed separately in benchmarks.


## Additional fail-closed protection in correction replay

`process_metering()` remains compatible with legacy normalized DataFrames
where `timestamp_semantics` and `revision_basis` are entirely absent;
callers remain responsible for source verification. But **when those columns
are present**, the engine rejects records that *declare* any
`timestamp_semantics` other than `UTC_VERIFIED` / `OFFSET_VERIFIED`, or, in **strict source-replay mode**, any `revision_basis` other than
`SOURCE_REVISION`, including nulls.

Rejected records retain `UNVERIFIED_UTC_INSTANT` /
`UNVERIFIED_SOURCE_REVISION` in `quality_errors`. The operation does not
quietly replace an unknown timestamp or export snapshot with a market revision.
This is deliberately a source-data-safety check, **not an official UMIG rule**.

**Never add a fake VERIFIED marker solely to make a pipeline pass.** The
provenance gate and the upstream verified source contract remain necessary
for a trusted correction replay.


## Snapshot comparison versus official-source correction

The `read_fluvius_csv()` adapter emits `revision_basis=EXPORT_SNAPSHOT`.
The generic `process_metering()` retains comparison of successive exports:
its `current` means most recent *snapshot*, not a source-authored UMIG market
revision. To enforce source corrections instead, set
`require_source_revision=True` after provenance screening; this rejects
snapshot/ingestion-time bases and fails if `revision_basis` is absent.
Any **explicitly declared** unresolved UTC timestamp remains rejected in
both modes. This keeps existing consumer use cases working while making the
stronger market-replay contract enforceable.
