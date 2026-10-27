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
