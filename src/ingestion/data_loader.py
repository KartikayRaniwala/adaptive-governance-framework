# ============================================================================
# Adaptive Data Governance Framework
# src/ingestion/data_loader.py
# ============================================================================
# Multi-source data ingestion with schema inference and Delta Lake
# integration.  Supports CSV, Parquet, JSON, and Delta formats.
# Implements incremental loading with watermark tracking and automatic
# schema-drift detection.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Type alias for alerting callbacks
AlertCallback = Callable[[str, Dict], None]


class DataLoader:
    """Multi-source data ingestion with Delta Lake integration.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session with Delta Lake extensions configured.
    base_data_path : str
        Root directory for the data lake (e.g. ``"data"``).
    """

    def __init__(
        self,
        spark: SparkSession,
        base_data_path: str = "data",
        quarantine_path: Optional[str] = None,
        alert_callback: Optional[AlertCallback] = None,
    ):
        self.spark = spark
        self.base_path = Path(base_data_path)
        self.quarantine_path = Path(quarantine_path or str(self.base_path / "quarantine"))
        self.alert_callback = alert_callback
        self._watermarks: Dict[str, str] = {}
        self._watermark_file = self.base_path / ".watermarks.json"
        self._load_watermarks()

    # ------------------------------------------------------------------
    # Watermark persistence
    # ------------------------------------------------------------------

    def _load_watermarks(self) -> None:
        if self._watermark_file.exists():
            with open(self._watermark_file) as f:
                self._watermarks = json.load(f)

    def _save_watermarks(self) -> None:
        self._watermark_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._watermark_file, "w") as f:
            json.dump(self._watermarks, f, indent=2)

    # ------------------------------------------------------------------
    # CSV ingestion
    # ------------------------------------------------------------------

    def load_from_csv(
        self,
        path: str,
        schema: Optional[StructType] = None,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        multiline: bool = False,
    ) -> DataFrame:
        """Read a CSV file (or directory of CSVs) into a Spark DataFrame.

        Parameters
        ----------
        path : str
            Path to CSV file or directory.
        schema : StructType | None
            Explicit PySpark schema.  If ``None``, schema is inferred.
        header : bool
            Whether the CSV has a header row.
        infer_schema : bool
            Let Spark infer data types (ignored if *schema* is supplied).
        delimiter : str
            Field delimiter.
        multiline : bool
            Whether values can span multiple lines.
        """
        reader = self.spark.read.option("header", str(header).lower())
        reader = reader.option("delimiter", delimiter)
        reader = reader.option("multiLine", str(multiline).lower())

        if schema is not None:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", str(infer_schema).lower())

        df = reader.csv(path)
        logger.info(
            "Loaded CSV — path={p}, rows={r}, cols={c}",
            p=path, r=df.count(), c=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------
    # Parquet ingestion
    # ------------------------------------------------------------------

    def load_from_parquet(self, path: str) -> DataFrame:
        """Read Parquet files into a Spark DataFrame."""
        df = self.spark.read.parquet(path)
        logger.info(
            "Loaded Parquet — path={p}, rows={r}, cols={c}",
            p=path, r=df.count(), c=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------
    # JSON ingestion
    # ------------------------------------------------------------------

    def load_from_json(
        self,
        path: str,
        schema: Optional[StructType] = None,
        multiline: bool = True,
    ) -> DataFrame:
        """Read JSON files into a Spark DataFrame."""
        reader = self.spark.read.option("multiLine", str(multiline).lower())
        if schema:
            reader = reader.schema(schema)
        df = reader.json(path)
        logger.info(
            "Loaded JSON — path={p}, rows={r}, cols={c}",
            p=path, r=df.count(), c=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------
    # Delta Lake read
    # ------------------------------------------------------------------

    def load_from_delta(self, path: str, version: Optional[int] = None) -> DataFrame:
        """Read a Delta Lake table, optionally at a specific version (time-travel)."""
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        df = reader.load(path)
        logger.info(
            "Loaded Delta — path={p}, version={v}, rows={r}",
            p=path, v=version or "latest", r=df.count(),
        )
        return df

    # ------------------------------------------------------------------
    # Schema-drift detection
    # ------------------------------------------------------------------

    def detect_schema_drift(
        self,
        incoming_df: DataFrame,
        reference_path: str,
    ) -> Dict:
        """Compare incoming DataFrame schema against a reference Delta table.

        Returns
        -------
        dict
            Keys: ``has_drift``, ``added_columns``, ``removed_columns``,
            ``type_changes``.
        """
        try:
            ref_df = self.spark.read.format("delta").load(reference_path)
            ref_schema = {f.name: str(f.dataType) for f in ref_df.schema.fields}
        except Exception:
            logger.warning("No reference table at {p}; treating as first load.", p=reference_path)
            return {"has_drift": False, "added_columns": [], "removed_columns": [], "type_changes": []}

        inc_schema = {f.name: str(f.dataType) for f in incoming_df.schema.fields}

        added = [c for c in inc_schema if c not in ref_schema]
        removed = [c for c in ref_schema if c not in inc_schema]
        type_changes = [
            {"column": c, "old_type": ref_schema[c], "new_type": inc_schema[c]}
            for c in inc_schema
            if c in ref_schema and inc_schema[c] != ref_schema[c]
        ]

        has_drift = bool(added or removed or type_changes)

        if has_drift:
            logger.warning(
                "Schema drift detected — added={a}, removed={r}, changed={c}",
                a=added, r=removed, c=type_changes,
            )
        else:
            logger.info("No schema drift detected.")

        return {
            "has_drift": has_drift,
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes,
        }

    # ------------------------------------------------------------------
    # Write to Bronze layer
    # ------------------------------------------------------------------

    def write_to_bronze(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        merge_schema: bool = True,
        enforce_drift_check: bool = True,
        quarantine_on_drift: bool = True,
    ) -> Tuple[str, Optional[Dict]]:
        """Write a DataFrame to the Bronze Delta Lake layer.

        Automatically detects schema drift against the existing table.
        If drift is found and *quarantine_on_drift* is True, the
        drifted records are written to the quarantine layer and an
        alert is fired.

        Parameters
        ----------
        df : DataFrame
            Source data.
        table_name : str
            Logical table name (used as subdirectory under bronze/).
        partition_cols : list[str] | None
            Columns to partition by.
        merge_schema : bool
            Whether to allow schema evolution on append.
        enforce_drift_check : bool
            Run schema-drift detection before writing.
        quarantine_on_drift : bool
            If True, type-change records are quarantined and an alert
            is triggered.

        Returns
        -------
        (str, dict | None)
            Bronze table path and drift report (None if no drift).
        """
        bronze_path = str(self.base_path / "bronze" / table_name)
        drift_report: Optional[Dict] = None

        # ---------- Schema-drift detection ----------
        if enforce_drift_check:
            drift_report = self.detect_schema_drift(df, bronze_path)

            if drift_report["has_drift"]:
                logger.warning(
                    "Schema drift on '{}' — added={}, removed={}, changed={}",
                    table_name,
                    drift_report["added_columns"],
                    drift_report["removed_columns"],
                    drift_report["type_changes"],
                )

                # Fire alert
                self._fire_alert(
                    f"Schema drift detected on '{table_name}'",
                    drift_report,
                )

                # Quarantine if type changes exist (most dangerous)
                if quarantine_on_drift and drift_report["type_changes"]:
                    self._quarantine_drift(
                        df, table_name, drift_report,
                    )

        # ---------- Add ingestion metadata ----------
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_enriched = (
            df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn(
                "_has_drift",
                F.lit(drift_report["has_drift"] if drift_report else False),
            )
        )

        writer = (
            df_enriched.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", str(merge_schema).lower())
            .option("optimizeWrite", "true")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(bronze_path)

        # Update watermark
        self._watermarks[table_name] = datetime.now().isoformat()
        self._save_watermarks()

        logger.info(
            "Written to Bronze — table={t}, path={p}, rows={r}",
            t=table_name, p=bronze_path, r=df.count(),
        )
        return bronze_path, drift_report

    # ------------------------------------------------------------------
    # Alerting
    # ------------------------------------------------------------------

    def _fire_alert(self, message: str, context: Dict) -> None:
        """Fire an alert via the registered callback."""
        alert_payload = {
            "message": message,
            "severity": "WARNING",
            "timestamp": datetime.now().isoformat(),
            **context,
        }
        logger.warning("🚨 ALERT: {} | {}", message, json.dumps(context, default=str))

        if self.alert_callback:
            try:
                self.alert_callback(message, alert_payload)
            except Exception as exc:
                logger.error("Alert callback failed: {}", exc)

    # ------------------------------------------------------------------
    # Schema-drift quarantine
    # ------------------------------------------------------------------

    def _quarantine_drift(
        self,
        df: DataFrame,
        table_name: str,
        drift_report: Dict,
    ) -> None:
        """Write drifted records to quarantine with metadata."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        q_path = str(self.quarantine_path / f"drift_{table_name}" / ts)
        (
            df
            .withColumn("_quarantine_reason", F.lit("schema_drift"))
            .withColumn("_quarantine_ts", F.current_timestamp())
            .withColumn(
                "_drift_detail",
                F.lit(json.dumps(drift_report, default=str)),
            )
            .write.format("parquet")
            .mode("append")
            .save(q_path)
        )
        logger.warning(
            "Quarantined drift batch → {} ({} rows)",
            q_path, df.count(),
        )

    # ------------------------------------------------------------------
    # Incremental loading
    # ------------------------------------------------------------------

    def load_incremental(
        self,
        path: str,
        table_name: str,
        timestamp_col: str,
        file_format: str = "parquet",
    ) -> DataFrame:
        """Load only records newer than the last watermark.

        Parameters
        ----------
        path : str
            Source path.
        table_name : str
            Logical name for watermark tracking.
        timestamp_col : str
            Column used for watermark comparison.
        file_format : str
            ``"parquet"``, ``"csv"``, ``"json"``, or ``"delta"``.
        """
        if file_format == "csv":
            full_df = self.load_from_csv(path)
        elif file_format == "json":
            full_df = self.load_from_json(path)
        elif file_format == "delta":
            full_df = self.load_from_delta(path)
        else:
            full_df = self.load_from_parquet(path)

        last_watermark = self._watermarks.get(table_name)
        if last_watermark:
            full_df = full_df.filter(F.col(timestamp_col) > F.lit(last_watermark))
            logger.info(
                "Incremental load — after {w}, rows={r}",
                w=last_watermark, r=full_df.count(),
            )
        else:
            logger.info("Full initial load — no prior watermark for {t}", t=table_name)

        return full_df
