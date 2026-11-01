# Integration checklist

Start with [the README](../README.md) and [architecture](architecture.md). This is source-installable PySpark code, not a certified market-message validator or a hosted service.

1. Keep source files and their identifiers immutable in Bronze.
2. Map fields into the canonical schema using a source-specific adapter. Confirm clock offsets, EAN/meter identity, unit and revision semantics.
3. Run `screen_metering_rows()`; retain quarantine.
4. Run `screen_source_provenance()`; retain review/quarantine. Only source-authored revisions belong in `replay_ready`.
5. Run `process_metering(..., require_source_revision=True)`; persist all four result lanes.
6. Reconcile counts/versions and publish only after every required Delta output succeeds. A multi-table write is not atomic.
7. Validate real source fixtures, missing-scope references, DST, late revisions, retries and access policy before operational deployment.

Run locally with `pip install -e '.[test]' && pytest -q`. See [Databricks](databricks.md), [provenance rules](rule-provenance.md) and [measured evidence](cross-platform-evidence.md).

Use synthetic or licensed public data for issues and CI. Do not upload customer meter IDs, EANs, private contracts or raw readings.
