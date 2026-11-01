# Design

The library operates on **normalised interval readings**, not raw UMIG exchange messages. Source adapters must establish point/meter identity, register semantics, time zone, and revision authority before a record is accepted for correction replay.

```text
Source -> immutable Bronze -> row screen -> provenance gate
                             |                |
                         quarantine         review
                                              |
                                        source revisions
                                              |
                                      correction resolver
                                              |
                         current / rejected / superseded / changes
```

## Decisions

- **Point ≠ meter.** An EAN may persist across a physical meter replacement.
- **Zero ≠ missing.** Do not fill absent intervals with zero.
- **Exact duplicates ≠ changes.** Collapse identical redeliveries, preserve actual earlier versions, reject conflicting latest revisions.
- **Source revision ≠ ingestion order.** Export timestamps do not grant authority to rewrite market history. For source-authored changes, use `require_source_revision=True` after the provenance gate.
- **Missing intervals need scope.** Coverage checks require trusted active dates.
- **Time zones need evidence.** Set the Spark session to UTC; quarantine ambiguous local instants rather than guessing an offset.

## Operating model

Each output remains a DataFrame until written by the caller. Keep immutable Bronze, separate rejected/review streams, and an explicit batch-completion barrier. Four Delta table writes do **not** form a single transaction. The [failure-injection exercise](delta-recovery.md) demonstrates reconciliation and retry behavior in owned scratch tables, not an atomic multi-table guarantee.

For production use, adapt and test the organisation's source schema, revision ordering, active scopes, retention, access control and Delta merge/publish rules. The [rule register](rule-provenance.md) distinguishes local checks from externally validated market rules.

[Example](../examples/basic_pipeline.py) · [Databricks integration](databricks.md) · [Migration readiness](migration-readiness.md)
