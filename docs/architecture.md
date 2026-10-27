# Design notes and limits

## Why the canonical schema is deliberately small

UMIG 6 is a market process and message standard. A bulk Spark pipeline typically
operates on normalized readings rather than raw inter-party exchange messages.
This library handles the latter only after an authorised upstream adapter has
interpreted payload version, identity, register, status and timestamps.

There is **no** attempt to manufacture official UMIG validation results from
household exports or to infer CMS-internal correction procedures.

## Data flow

```text
Source/adapters -> canonical observation versions
                     | basic validity checks
                     +--> invalid -> rejected
                     |
                     +--> valid -> exact redelivery deduplication
                                     | rank source revisions
                                     +--> conflicting latest -> rejected
                                     +--> old versions -> superseded
                                     +--> current unambiguous -> current
                                     +--> changed versions -> changes
Scope reference -------------------------------------> coverage
```

## Key decisions

1. **Point identity is not meter identity.** The point can retain an EAN across
   a physical meter change. Distinguish `ean` from `meter_id` and do not force a
   continuous single-meter timeline across a replacement.
2. **A zero reading is real data.** No default `0` substitution for null or
   missing intervals.
3. **Revisions are evidence.** Never use `dropDuplicates([key])` as a correction
   policy. Deduplicate exact repeats only, preserve historical versions.
4. **Ambiguity is an error.** Multiple different values claiming the same latest
   revision timestamp must go to quarantine, not random tie-breaking.
5. **Coverage depends on reference facts.** Active scope ranges come from an
   explicit trusted source; source absence alone proves nothing.
6. **UTC event time.** Convert source local timestamps to unambiguous instants
   upstream. Spark session timezone is required to be UTC. 92 and 100-quarter
   local days are normal, not missing/extra data.
7. **No implicit commercial meaning.** Estimated and measured labels are
   supplied by the caller; never equate a data-quality warning with a billing
   error or an Atrias rectification.

## Before a production deployment

- Validate adapters and registered identity formats against your licensed feed.
- Define trusted revision ordering and late-arrival/watermark behaviour.
- Add schema contracts for reference data and access control for audit outputs.
- Persist provenance (file, batch, ingestion timestamp) in the input rows.
- Determine whether meter replacement causes an expected scope split.
- Ensure GDPR retention, role-based access and removal of identifying data
  from CI logs and sample reports.
- Monitor per-rule failure trends and manually review quarantines.
- Integrate with Delta/Iceberg/Hudi via a versioned, idempotent sink.

## Future candidates

- Effective-dated meter-to-point mapping (SCD2) with explicit ambiguity flags.
- Per-source adapters (approved export formats only).
- Optional EAN-18 format checks and configured register mapping.
- Configurable tolerance for precision/rounding where standards permit it.
- Data quality metric tables and observability hooks (without driver-side counts).

## Consumer CSV bridge (experimental)

`read_fluvius_csv` reads the documented Dutch-language Mijn Fluvius electricity
report fields, with optional explicit header mappings for other languages.
It produces canonical Spark readings and a separate adapter-level rejection
stream. It does **not** claim the report contains UMIG messages.

- Input timestamps are Belgian local wall-clock times; clean instants are
  converted to UTC. The repeated autumn hour and nonexistent spring hour are
  quarantined unless a future input format provides unambiguous offsets.
- The provided UTC snapshot timestamp orders different *exports*. It is not
  an official market-message sequence number or validation timestamp.
- Validation statuses are kept verbatim. They are not reinterpreted as a
  supplier-accepted or billing-grade status.
- Only recognized active-electricity import/injection registers and kWh are
  accepted. Other meters/units are not converted silently.
- The entire process remains a distributed Spark transformation. Paths and
  provenance should stay inside access-controlled environments.

## Provenance before Silver correction replay (v0.7)

```text
Immutable Bronze with source originals
  -> screen_metering_rows         : row-level technical validity
      | invalid -> quarantine
      -> screen_source_provenance : origin / clock / revision evidence
          | incomplete -> review  | conflicting metadata -> quarantine
          -> analytics_ready
             | only SOURCE_REVISION + typed version timestamp -> replay_ready
                 -> process_metering (optional shared Spark cache)
                     -> current / rejected / superseded / changes
```

The `TECH_SOURCE_PROVENANCE_V1` profile is deliberately a **project rule**,
not an Atrias validation or certification step. Ingestion time and successive
consumer exports are not equivalent to source-authored revisions. Presence
of an `UTC_VERIFIED` marker is only a caller assertion until the source
contract has been independently reviewed.

The revision resolver intentionally collapses *business-identical* redeliveries.
It does not retain every delivery's separate metadata in `current` or
`superseded`. Full traceability therefore requires retaining the original
immutable Bronze feed, along with its load and source IDs; do not describe
deduplicated Silver as a complete delivery audit log.
