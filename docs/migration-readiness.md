# Migration readiness for historical Belgian energy data

This library evaluates whether a **legacy data extract has enough structurally consistent information** to start a migration, an analytics replay or a mapping exercise. It is not an Atrias certification test, market-process authorisation, or official UMIG 6 readiness score.

## Supported scenarios

| Scenario | Expected input | Typical usefulness | Not established by this check |
|---|---|---|---|
| `METERING_REPLAY` | EAN, physical meter ID, register, UTC interval start/end, kWh; optional revision date and method | Migrating 20 years of meter readings from several MDM/data warehouse systems into a standard fact model | Whether a meter value was V3-validated, actually exchanged in CMS, or billable |
| `ASSET_HISTORY` | EAN, asset ID, meter ID, UTC validity start/end (open-ended allowed) | Identifying records without dates or stable point-to-meter keys before joining metering and assets | Whether a device was physically installed on that date |
| `SUPPLIER_CHANGE` | EAN, supplier ID, UTC contract start/end (open-ended allowed) | Finding gaps in older supplier-assignment extracts before mapping to process-level data | Whether a switch was approved, notified, or executed by CMS |

Do **not** confuse a 100% structural score with MIG6 conformance. Score denominator is only rows in the given DataFrame. Null/absent optional columns become warnings. Missing mandatory columns result in explicit blockers (no invented values). Data-model owners should additionally validate cross-system consistency and versioned source snapshots.

```python
from umig6_metering_quality import evaluate_readiness
r = evaluate_readiness(spark.read.parquet('/data/legacy/asset_assignments'), 'ASSET_HISTORY')
r.summary.show()             # scenario; checked/ready/blocked; structural readiness %
r.quarantine.write.mode('overwrite').parquet('/data/audit/asset_blockers')
```

### Cross-dataset reference assessment

```python
from umig6_metering_quality import validate_asset_history, reconcile_metering_assets
asset_quality = validate_asset_history(assets)
linked = reconcile_metering_assets(metering, asset_quality.accepted)
# Readings may have zero, one, or multiple matching physical-meter assignments.
```

This *optional* range join checks the half-open point/meter assignment interval against a reading's interval. For large asset histories, prune both inputs to the relevant period before joining. Broadcast is **opt-in** and should be used only with measured-small dimensions. `NO_MATCHING_ASSET_ASSIGNMENT` signals missing **evidence**, not proof that the reading or physical meter was invalid. `MULTIPLE_ASSET_ASSIGNMENTS` requires investigation; it is not automatically a rejection.

### Suggested validation extensions

- Dated supplier identity and role joins, using genuinely authorised reference records.
- Meter replacement events, register mappings and opening/closing indexes.
- Native UMIG XML and XSD validation **only when redistributable schema versions and permitted fixtures are available**.
- Official process-level rule sets, with explicit release IDs and public references.

## Scope and data safety

EANs and customer measurements may be personal data. Examples use synthetic identities. Store quarantine in restricted Unity Catalog tables, record provenance, and set retention/access policies. No internal Fluvius/Atrias files are bundled.

Sources: [Atrias UMIG onboarding](https://www.atrias.be/new-members), [Atrias FAQ](https://www.atrias.be/faq), [Fluvius report on market processes](https://jaarverslag.fluvius.be/2023/en/focus-on-customers).
