# Publicly documented rules, deliberately limited

`screen_continuous_measures()` provides an **optional, version-pinned** screening layer for historical UMIG metering data. It is separate from the library's own engineering quality controls and quarantine rules.

## Profile

`UMIG_ME_6_5_2_2_V1_PREREQUISITES` maps to the historical *UMIG - HB - ME - 03 - Validation Rules*, version **6.5.2.2**, section **5.1.2**, which describes minimum technical checks for 15/60-minute continuous metering values:

| Rule ID | What this library checks | What it deliberately does not infer |
| --- | --- | --- |
| `ME_CONTINUOUS_VALUE_PRESENT` | The numeric measure is present and not NaN. Zero counts as present. | Does not infer whether value is estimated, approved, billable, or commercially validated. |
| `ME_READOUT_DIGITS_MATCH` | Caller-supplied observed readout digit count agrees with caller-supplied expected count. | Does not derive digit counts from rendered kWh floats or decimal precision. |

The document is available from the [Atrias documentation portal](https://www.atrias.be/current-documentation) (historical 6.5.2.2 Measure Validation Rules). No proprietary content is bundled. The pinned reference is **not asserted to be the latest market baseline**, and this library does not implement the full V1 workflow or assign any validation levels.

```python
from umig6_metering_quality import screen_continuous_measures

# Both readout counts must come from a trusted source mapping, not guesses.
screen = screen_continuous_measures(
    bronze,
    observed_digits_column="observed_readout_digits",
    expected_digits_column="expected_readout_digits",
)
screen.quarantine.write.format("delta").mode("append").saveAsTable("dq_meter_quarantine")
screen.review.write.format("delta").mode("append").saveAsTable("dq_meter_review")
```

### Three states, not a checkbox

- `FAIL`: at least one tested prerequisite failed. Quarantine and investigate.
- `INCOMPLETE`: insufficient metadata to run all checks. Send to review, **not** to verified/passed outputs.
- `PASS`: local prerequisites tested successfully. **Not** Atrias V1 validation or approval.

Use `screen.evaluated` for one row per source row, or `screen.summary` for grouped Spark metrics. Counts are computed lazily. The API performs no Python UDF, `.collect()` or write side effects. Consumers must provide event/version provenance separately.

### Version policy

Never silently switch a consumer to a new handbook. Add new immutable profile IDs with reference links, dates, rule interpretation and regression fixtures, after reviewing the actual published source. A pinned profile is preferable to a misleading `latest` alias.
