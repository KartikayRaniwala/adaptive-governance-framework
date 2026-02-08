# ============================================================================
# Adaptive Data Governance Framework
# src/quality/anomaly_detector.py
# ============================================================================
# Statistical anomaly detection for data quality monitoring.
#
# Provides column-level and row-level anomaly detection using:
#   - Z-Score (parametric, assumes approximate normality)
#   - IQR (non-parametric, robust to skewed distributions)
#   - Isolation Forest (ML-based, catches multivariate anomalies)
#
# Integrates with the DQ gate to flag batches with abnormal distributions
# and quarantine individual anomalous rows.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# ============================================================================
# AnomalyDetector
# ============================================================================


class AnomalyDetector:
    """Statistical and ML-based anomaly detection on PySpark DataFrames.

    Parameters
    ----------
    spark : SparkSession
    z_threshold : float
        Number of standard deviations beyond which a value is flagged
        as anomalous in Z-Score mode (default 3.0).
    iqr_factor : float
        Multiplier for the IQR to compute fences (default 1.5).
    contamination : float
        Expected fraction of anomalies for Isolation Forest (default 0.05).
    history_path : str | None
        Directory where historical statistics are persisted as JSON so
        that thresholds can be compared across pipeline runs.
    """

    def __init__(
        self,
        spark: SparkSession,
        z_threshold: float = 3.0,
        iqr_factor: float = 1.5,
        contamination: float = 0.05,
        history_path: Optional[str] = None,
    ):
        self.spark = spark
        self.z_threshold = z_threshold
        self.iqr_factor = iqr_factor
        self.contamination = contamination
        self.history_path = Path(history_path) if history_path else None
        if self.history_path:
            self.history_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Z-Score anomaly detection
    # ------------------------------------------------------------------

    def zscore_detect(
        self,
        df: DataFrame,
        numeric_columns: Optional[List[str]] = None,
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Flag rows whose numeric values exceed ±z_threshold standard deviations.

        Returns
        -------
        (flagged_df, report)
            ``flagged_df`` has a new boolean column ``_is_anomaly_zscore``
            that is True when **any** monitored column is anomalous.
            ``report`` contains per-column stats and counts.
        """
        if numeric_columns is None:
            numeric_columns = [
                f.name for f in df.schema.fields
                if isinstance(f.dataType, (IntegerType, DoubleType))
            ]

        logger.info(
            "Z-Score anomaly detection on {} columns (threshold={}σ)",
            len(numeric_columns), self.z_threshold,
        )

        # Compute mean and stddev for each column
        agg_exprs = []
        for c in numeric_columns:
            agg_exprs.extend([
                F.mean(F.col(c)).alias(f"_mean_{c}"),
                F.stddev(F.col(c)).alias(f"_stddev_{c}"),
            ])

        stats_row = df.agg(*agg_exprs).collect()[0]

        col_stats: Dict[str, Dict[str, float]] = {}
        anomaly_flags = []

        for c in numeric_columns:
            mean_val = float(stats_row[f"_mean_{c}"] or 0)
            std_val = float(stats_row[f"_stddev_{c}"] or 1)
            if std_val == 0:
                std_val = 1  # avoid division by zero

            col_stats[c] = {"mean": round(mean_val, 4), "stddev": round(std_val, 4)}

            # |value - mean| / stddev > threshold
            z_col = F.abs((F.col(c).cast("double") - F.lit(mean_val)) / F.lit(std_val))
            flag_col = F.when(z_col > self.z_threshold, True).otherwise(False)
            df = df.withColumn(f"_anomaly_z_{c}", flag_col)
            anomaly_flags.append(F.col(f"_anomaly_z_{c}"))

        # Composite flag: any column anomalous
        if anomaly_flags:
            composite = anomaly_flags[0]
            for f_ in anomaly_flags[1:]:
                composite = composite | f_
            df = df.withColumn("_is_anomaly_zscore", composite)
        else:
            df = df.withColumn("_is_anomaly_zscore", F.lit(False))

        anomaly_count = df.filter(F.col("_is_anomaly_zscore")).count()
        total = df.count()

        report = {
            "method": "z-score",
            "threshold": self.z_threshold,
            "columns_monitored": numeric_columns,
            "column_stats": col_stats,
            "total_rows": total,
            "anomaly_rows": anomaly_count,
            "anomaly_pct": round(anomaly_count / total * 100, 2) if total else 0,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(
            "Z-Score: {}/{} rows flagged ({:.2f}%)",
            anomaly_count, total, report["anomaly_pct"],
        )
        return df, report

    # ------------------------------------------------------------------
    # IQR anomaly detection
    # ------------------------------------------------------------------

    def iqr_detect(
        self,
        df: DataFrame,
        numeric_columns: Optional[List[str]] = None,
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Flag rows outside the IQR fences [Q1 - k*IQR, Q3 + k*IQR].

        Returns
        -------
        (flagged_df, report)
        """
        if numeric_columns is None:
            numeric_columns = [
                f.name for f in df.schema.fields
                if isinstance(f.dataType, (IntegerType, DoubleType))
            ]

        logger.info(
            "IQR anomaly detection on {} columns (factor={})",
            len(numeric_columns), self.iqr_factor,
        )

        col_stats: Dict[str, Dict[str, float]] = {}
        anomaly_flags = []

        for c in numeric_columns:
            quantiles = df.approxQuantile(c, [0.25, 0.75], 0.01)
            if len(quantiles) < 2 or quantiles[0] is None:
                continue

            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_fence = q1 - self.iqr_factor * iqr
            upper_fence = q3 + self.iqr_factor * iqr

            col_stats[c] = {
                "q1": round(q1, 4),
                "q3": round(q3, 4),
                "iqr": round(iqr, 4),
                "lower_fence": round(lower_fence, 4),
                "upper_fence": round(upper_fence, 4),
            }

            flag = (
                (F.col(c).cast("double") < F.lit(lower_fence))
                | (F.col(c).cast("double") > F.lit(upper_fence))
            )
            df = df.withColumn(f"_anomaly_iqr_{c}", flag)
            anomaly_flags.append(F.col(f"_anomaly_iqr_{c}"))

        if anomaly_flags:
            composite = anomaly_flags[0]
            for f_ in anomaly_flags[1:]:
                composite = composite | f_
            df = df.withColumn("_is_anomaly_iqr", composite)
        else:
            df = df.withColumn("_is_anomaly_iqr", F.lit(False))

        anomaly_count = df.filter(F.col("_is_anomaly_iqr")).count()
        total = df.count()

        report = {
            "method": "iqr",
            "iqr_factor": self.iqr_factor,
            "columns_monitored": numeric_columns,
            "column_stats": col_stats,
            "total_rows": total,
            "anomaly_rows": anomaly_count,
            "anomaly_pct": round(anomaly_count / total * 100, 2) if total else 0,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(
            "IQR: {}/{} rows flagged ({:.2f}%)",
            anomaly_count, total, report["anomaly_pct"],
        )
        return df, report

    # ------------------------------------------------------------------
    # Isolation Forest (ML-based)
    # ------------------------------------------------------------------

    def isolation_forest_detect(
        self,
        df: DataFrame,
        feature_columns: Optional[List[str]] = None,
        sample_fraction: float = 0.1,
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Use scikit-learn Isolation Forest on a sample to score all rows.

        The model is trained on a sample of the data and then applied
        back via a broadcast join.

        Parameters
        ----------
        feature_columns : list[str] | None
            Numeric columns to use as features.  Defaults to all numeric.
        sample_fraction : float
            Fraction of data to sample for training (default 10 %).

        Returns
        -------
        (scored_df, report)
            ``scored_df`` has ``_anomaly_score`` (float, lower = more
            anomalous) and ``_is_anomaly_iforest`` (bool).
        """
        from sklearn.ensemble import IsolationForest

        if feature_columns is None:
            feature_columns = [
                f.name for f in df.schema.fields
                if isinstance(f.dataType, (IntegerType, DoubleType))
            ]

        logger.info(
            "Isolation Forest on {} features (contamination={}, sample={})",
            len(feature_columns), self.contamination, sample_fraction,
        )

        # Sample to Pandas for sklearn
        sample_df = (
            df.select(feature_columns)
            .dropna()
            .sample(withReplacement=False, fraction=sample_fraction)
        )
        pdf = sample_df.toPandas()

        if pdf.empty or len(pdf) < 10:
            logger.warning("Insufficient data for Isolation Forest — skipping.")
            df = df.withColumn("_anomaly_score", F.lit(0.0))
            df = df.withColumn("_is_anomaly_iforest", F.lit(False))
            return df, {"method": "isolation_forest", "skipped": True}

        # Train
        clf = IsolationForest(
            n_estimators=100,
            contamination=self.contamination,
            random_state=42,
            n_jobs=-1,
        )
        clf.fit(pdf[feature_columns].values)

        # Score entire dataset (sample to Pandas in chunks if too large)
        full_pdf = df.select(feature_columns).dropna().toPandas()
        scores = clf.decision_function(full_pdf[feature_columns].values)
        predictions = clf.predict(full_pdf[feature_columns].values)

        full_pdf["_anomaly_score"] = scores
        full_pdf["_is_anomaly_iforest"] = predictions == -1

        # Convert back to Spark and join
        score_sdf = self.spark.createDataFrame(full_pdf)

        # Add monotonically increasing id for joining
        df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
        score_sdf_with_id = score_sdf.withColumn(
            "_row_id", F.monotonically_increasing_id()
        )

        result_df = df_with_id.join(
            score_sdf_with_id.select(
                "_row_id", "_anomaly_score", "_is_anomaly_iforest"
            ),
            on="_row_id",
            how="left",
        ).drop("_row_id")

        anomaly_count = result_df.filter(F.col("_is_anomaly_iforest")).count()
        total = result_df.count()

        report = {
            "method": "isolation_forest",
            "contamination": self.contamination,
            "sample_fraction": sample_fraction,
            "feature_columns": feature_columns,
            "training_sample_size": len(pdf),
            "total_rows": total,
            "anomaly_rows": anomaly_count,
            "anomaly_pct": round(anomaly_count / total * 100, 2) if total else 0,
            "mean_anomaly_score": round(float(np.mean(scores)), 4),
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(
            "Isolation Forest: {}/{} rows flagged ({:.2f}%)",
            anomaly_count, total, report["anomaly_pct"],
        )
        return result_df, report

    # ------------------------------------------------------------------
    # Batch-level anomaly detection (cross-run comparison)
    # ------------------------------------------------------------------

    def detect_batch_anomaly(
        self,
        current_metrics: Dict[str, float],
        label: str = "dq_batch",
    ) -> Dict[str, Any]:
        """Compare current batch DQ metrics against historical runs.

        Uses a sliding-window Z-score: if the current batch's overall DQ
        score deviates by more than ``z_threshold`` standard deviations
        from the historical mean, it is flagged.

        Parameters
        ----------
        current_metrics : dict
            Must contain ``"overall_score"`` and optionally per-dimension
            scores under ``"dimensions"``.
        label : str
            Identifier for the metric series (e.g. ``"silver_orders"``).

        Returns
        -------
        dict
            Batch anomaly report with ``is_anomaly``, ``z_score``,
            historical stats, and recommended action.
        """
        history = self._load_history(label)
        current_score = current_metrics.get("overall_score", 0)

        report: Dict[str, Any] = {
            "label": label,
            "current_score": current_score,
            "timestamp": datetime.now().isoformat(),
        }

        if len(history) < 3:
            # Not enough history to compute meaningful stats
            logger.info(
                "Batch anomaly: insufficient history ({} runs) — skipping",
                len(history),
            )
            report["is_anomaly"] = False
            report["reason"] = "insufficient_history"
            report["history_count"] = len(history)
        else:
            hist_scores = [h["overall_score"] for h in history]
            hist_mean = float(np.mean(hist_scores))
            hist_std = float(np.std(hist_scores))
            if hist_std == 0:
                hist_std = 1.0

            z = (current_score - hist_mean) / hist_std
            is_anomaly = abs(z) > self.z_threshold

            report["is_anomaly"] = is_anomaly
            report["z_score"] = round(z, 4)
            report["historical_mean"] = round(hist_mean, 2)
            report["historical_std"] = round(hist_std, 2)
            report["history_count"] = len(history)
            report["recommended_action"] = (
                "INVESTIGATE — batch DQ score is a statistical outlier"
                if is_anomaly
                else "OK — within normal range"
            )

            if is_anomaly:
                logger.warning(
                    "BATCH ANOMALY: score={:.2f} (z={:.2f}, mean={:.2f}±{:.2f})",
                    current_score, z, hist_mean, hist_std,
                )
            else:
                logger.info(
                    "Batch OK: score={:.2f} (z={:.2f}, mean={:.2f}±{:.2f})",
                    current_score, z, hist_mean, hist_std,
                )

        # Persist current run to history
        self._append_history(label, current_metrics)
        return report

    # ------------------------------------------------------------------
    # History persistence helpers
    # ------------------------------------------------------------------

    def _history_file(self, label: str) -> Optional[Path]:
        if self.history_path is None:
            return None
        return self.history_path / f"{label}_history.json"

    def _load_history(self, label: str) -> List[Dict]:
        path = self._history_file(label)
        if path and path.exists():
            with open(path) as f:
                return json.load(f)
        return []

    def _append_history(self, label: str, metrics: Dict) -> None:
        path = self._history_file(label)
        if path is None:
            return
        history = self._load_history(label)
        history.append({
            "overall_score": metrics.get("overall_score", 0),
            "dimensions": metrics.get("dimensions", {}),
            "timestamp": datetime.now().isoformat(),
        })
        # Keep last 100 runs
        history = history[-100:]
        with open(path, "w") as f:
            json.dump(history, f, indent=2)
        logger.debug("History saved → {} ({} entries)", path, len(history))
