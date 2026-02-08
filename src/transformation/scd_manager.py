# ============================================================================
# Adaptive Data Governance Framework
# src/transformation/scd_manager.py
# ============================================================================
# Slowly Changing Dimension (SCD) Type 2 implementation using Delta Lake
# MERGE.  Tracks historical changes with valid_from / valid_to / is_current
# columns.
# ============================================================================

from __future__ import annotations

from typing import List, Optional

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class SCDType2Manager:
    """Manage SCD Type 2 dimensions on Delta Lake tables.

    Parameters
    ----------
    spark : SparkSession
    table_path : str
        Delta table path for the dimension.
    key_columns : list[str]
        Business key columns (natural keys).
    tracked_columns : list[str] | None
        Columns that trigger a new version when changed.
        If ``None``, all non-key, non-meta columns are tracked.
    """

    META_COLS = {"_valid_from", "_valid_to", "_is_current", "_version", "_hash"}

    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
    ):
        self.spark = spark
        self.table_path = table_path
        self.key_columns = key_columns
        self.tracked_columns = tracked_columns

    # ------------------------------------------------------------------
    # Hash helper
    # ------------------------------------------------------------------

    @staticmethod
    def _row_hash(df: DataFrame, columns: List[str]) -> DataFrame:
        """Add an ``_hash`` column computed from the tracked columns."""
        return df.withColumn(
            "_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in columns]), 256),
        )

    # ------------------------------------------------------------------
    # Initialise dimension
    # ------------------------------------------------------------------

    def initialise(self, df: DataFrame) -> None:
        """Write the initial load of the dimension table."""
        tracked = self.tracked_columns or [
            c for c in df.columns if c not in self.key_columns and c not in self.META_COLS
        ]

        init = (
            self._row_hash(df, tracked)
            .withColumn("_valid_from", F.current_timestamp())
            .withColumn("_valid_to", F.lit(None).cast("timestamp"))
            .withColumn("_is_current", F.lit(True))
            .withColumn("_version", F.lit(1))
        )

        init.write.format("delta").mode("overwrite").save(self.table_path)
        logger.info(
            "SCD2 dimension initialised at {p} — {n} rows",
            p=self.table_path, n=init.count(),
        )

    # ------------------------------------------------------------------
    # Apply changes (merge)
    # ------------------------------------------------------------------

    def apply_changes(self, incoming_df: DataFrame) -> None:
        """Merge incoming records into the dimension using SCD Type 2 logic.

        Changed rows:
            1. Close the current version (set ``_valid_to``, ``_is_current=False``)
            2. Insert a new row with ``_is_current=True``

        New rows:
            Inserted with ``_is_current=True``, ``_version=1``.
        """
        from delta.tables import DeltaTable

        existing = DeltaTable.forPath(self.spark, self.table_path)
        existing_df = existing.toDF()

        tracked = self.tracked_columns or [
            c for c in incoming_df.columns if c not in self.key_columns and c not in self.META_COLS
        ]

        incoming = self._row_hash(incoming_df, tracked)

        # Join on business key, current records only
        join_cond = " AND ".join(
            [f"existing.{k} = incoming.{k}" for k in self.key_columns]
        )
        join_cond += " AND existing._is_current = true"

        # Identify changed and new records
        joined = incoming.alias("incoming").join(
            existing_df.filter("_is_current = true").alias("existing"),
            on=self.key_columns,
            how="left",
        )

        # Changed records: hash differs
        changed = (
            joined
            .filter(
                F.col("existing._hash").isNotNull()
                & (F.col("incoming._hash") != F.col("existing._hash"))
            )
            .select(
                *[F.col(f"incoming.{c}").alias(c) for c in incoming.columns if c != "_hash"],
                F.col("incoming._hash"),
            )
        )

        # New records: no match
        new_records = (
            joined
            .filter(F.col("existing._hash").isNull())
            .select(
                *[F.col(f"incoming.{c}").alias(c) for c in incoming.columns if c != "_hash"],
                F.col("incoming._hash"),
            )
        )

        # 1. Expire changed current rows
        if changed.count() > 0:
            expire_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in self.key_columns]
            )
            expire_condition += " AND target._is_current = true"

            existing.alias("target").merge(
                changed.alias("source"),
                expire_condition,
            ).whenMatchedUpdate(set={
                "_valid_to": F.current_timestamp(),
                "_is_current": F.lit(False),
            }).execute()

            # 2. Insert new versions for changed records
            max_versions = (
                existing.toDF()
                .groupBy(*self.key_columns)
                .agg(F.max("_version").alias("max_v"))
            )
            new_versions = (
                changed
                .join(max_versions, on=self.key_columns, how="left")
                .withColumn("_valid_from", F.current_timestamp())
                .withColumn("_valid_to", F.lit(None).cast("timestamp"))
                .withColumn("_is_current", F.lit(True))
                .withColumn("_version", F.coalesce(F.col("max_v"), F.lit(0)) + 1)
                .drop("max_v")
            )
            new_versions.write.format("delta").mode("append").save(self.table_path)
            logger.info("SCD2: {} changed records updated.", changed.count())

        # 3. Insert brand-new records
        if new_records.count() > 0:
            inserts = (
                new_records
                .withColumn("_valid_from", F.current_timestamp())
                .withColumn("_valid_to", F.lit(None).cast("timestamp"))
                .withColumn("_is_current", F.lit(True))
                .withColumn("_version", F.lit(1))
            )
            inserts.write.format("delta").mode("append").save(self.table_path)
            logger.info("SCD2: {} new records inserted.", new_records.count())

        logger.info("SCD2 merge complete for {}", self.table_path)
