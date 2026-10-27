"""Reusable quarantine envelopes and Databricks batch persistence guidance.

Pure DataFrame transformations have no I/O, UDFs or collect(); callers decide
how to secure and persist potentially sensitive energy/customer information.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

_BATCH = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


@dataclass(frozen=True)
class QuarantineResult:
    quarantined: DataFrame
    reason_counts: DataFrame


def envelope_quarantine(
    invalid: DataFrame, *, dataset: str, batch_id: str,
    errors_column: str = "quality_errors", rule_version: str = "technical-v1",
) -> QuarantineResult:
    """Add deterministic record identity and queryable reason counts.

    The ID is deterministic within an immutable batch, including all original
    columns in schema order. If the source has nondeterministic metadata (e.g.
    ingestion_at=now()), normalize it before this call. Full raw rows are retained
    in quarantine and must be protected like the underlying source dataset.
    """
    if not _BATCH.fullmatch(batch_id):
        raise ValueError("batch_id must use 1-100 letters, digits, underscores or hyphens")
    if not dataset or not dataset.strip():
        raise ValueError("dataset must be a non-empty logical source name")
    if errors_column not in invalid.columns:
        raise ValueError(f"Missing {errors_column} column")
    if not (isinstance(invalid.schema[errors_column].dataType, ArrayType) and
            isinstance(invalid.schema[errors_column].dataType.elementType, StringType)):
        raise TypeError(f"{errors_column} must be array<string>")
    if any(c in invalid.columns for c in ("quarantine_id", "batch_id", "quarantine_dataset", "rule_version")):
        raise ValueError("Input contains reserved quarantine envelope columns")
    payload = F.to_json(F.struct(*[F.col(f"`{c.replace('`','``')}`").alias(c) for c in invalid.columns]),
                        options={"ignoreNullFields": "false"})
    annotated = (invalid.withColumn("quarantine_id", F.sha2(F.concat_ws("|", F.lit(batch_id), F.lit(dataset), payload), 256))
                 .withColumn("batch_id", F.lit(batch_id))
                 .withColumn("quarantine_dataset", F.lit(dataset))
                 .withColumn("rule_version", F.lit(rule_version))
                 .withColumn("quarantine_state", F.lit("OPEN")))
    quarantined = annotated.filter(F.size(F.col(errors_column)) > 0)
    reason_counts = (quarantined.select("batch_id", "quarantine_dataset", F.explode(F.col(errors_column)).alias("rule_id"))
                     .groupBy("batch_id", "quarantine_dataset", "rule_id")
                     .agg(F.count(F.lit(1)).alias("affected_rows")))
    return QuarantineResult(quarantined, reason_counts)


def write_delta_batch(
    current: DataFrame, quarantined: DataFrame, *, batch_id: str,
    current_table: str, quarantine_table: str,
) -> None:
    """Replace a single immutable batch in two Delta tables on Databricks.

    Both DataFrames must contain batch_id set to the same value. Each table
    write uses Delta replaceWhere to make retries replace (not append) a batch.
    These TWO writes are NOT one cross-table transaction. Orchestrate a retry
    and downstream commit barrier outside this method before promoting Gold.
    The initial target tables must already exist with matching schemas.
    """
    if not _BATCH.fullmatch(batch_id):
        raise ValueError("Invalid batch_id")
    if current_table == quarantine_table:
        raise ValueError("Use separate Delta tables for current and quarantine")
    if not _TABLE.fullmatch(current_table) or not _TABLE.fullmatch(quarantine_table):
        raise ValueError("Supply ordinary one-, two- or three-part table identifiers")
    for frame, label in ((current, "current"), (quarantined, "quarantined")):
        if "batch_id" not in frame.columns:
            raise ValueError(f"{label} DataFrame missing batch_id")
    predicate = f"batch_id = '{batch_id}'"
    for frame, table in ((current, current_table), (quarantined, quarantine_table)):
        (frame.write.format("delta").mode("overwrite")
         .option("replaceWhere", predicate).saveAsTable(table))
