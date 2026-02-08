# ============================================================================
# Adaptive Data Governance Framework
# src/quality/quality_metrics.py
# ============================================================================
# Calculate and track the six core data-quality dimensions:
#   Completeness, Accuracy, Consistency, Timeliness, Validity, Uniqueness
#
# Optionally exposes metrics via a Prometheus-compatible endpoint.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ============================================================================
# QualityMetrics
# ============================================================================

class QualityMetrics:
    """Compute and persist data-quality dimension scores.

    Parameters
    ----------
    spark : SparkSession
    metrics_path : str
        Directory where metric snapshots are saved.
    """

    def __init__(
        self,
        spark: SparkSession,
        metrics_path: str = "data/metrics",
    ):
        self.spark = spark
        self.metrics_path = Path(metrics_path)
        self.metrics_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Individual dimension calculators
    # ------------------------------------------------------------------

    @staticmethod
    def completeness(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, float]:
        """Fraction of non-null values per column.

        Returns
        -------
        dict[str, float]
            ``{column: completeness_pct}``
        """
        cols = columns or df.columns
        total = df.count()
        if total == 0:
            return {c: 0.0 for c in cols}

        result = {}
        for c in cols:
            non_null = df.filter(F.col(c).isNotNull()).count()
            result[c] = round(non_null / total * 100, 2)
        return result

    @staticmethod
    def uniqueness(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, float]:
        """Fraction of unique (distinct) values per column."""
        cols = columns or df.columns
        total = df.count()
        if total == 0:
            return {c: 0.0 for c in cols}

        result = {}
        for c in cols:
            distinct = df.select(c).distinct().count()
            result[c] = round(distinct / total * 100, 2)
        return result

    @staticmethod
    def validity(
        df: DataFrame,
        rules: Dict[str, str],
    ) -> Dict[str, float]:
        """Fraction of rows matching a regex pattern per column.

        Parameters
        ----------
        rules : dict[str, str]
            ``{column: regex_pattern}``
        """
        total = df.count()
        if total == 0:
            return {c: 0.0 for c in rules}

        result = {}
        for col, pattern in rules.items():
            valid = df.filter(F.col(col).rlike(pattern)).count()
            result[col] = round(valid / total * 100, 2)
        return result

    @staticmethod
    def timeliness(
        df: DataFrame,
        timestamp_col: str,
        sla_hours: float = 24.0,
    ) -> Dict[str, float]:
        """Percentage of records processed within the SLA window.

        Compares the difference between ``current_timestamp`` and the
        given *timestamp_col*.
        """
        total = df.count()
        if total == 0:
            return {"timeliness_pct": 0.0, "avg_latency_hours": 0.0}

        with_latency = df.withColumn(
            "_latency_hours",
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col(timestamp_col))) / 3600,
        )
        within_sla = with_latency.filter(F.col("_latency_hours") <= sla_hours).count()
        avg_latency = with_latency.agg(F.avg("_latency_hours")).collect()[0][0]

        return {
            "timeliness_pct": round(within_sla / total * 100, 2),
            "avg_latency_hours": round(avg_latency or 0, 2),
        }

    @staticmethod
    def consistency(
        df: DataFrame,
        cross_field_rules: List[Dict],
    ) -> Dict[str, float]:
        """Evaluate cross-field consistency rules.

        Parameters
        ----------
        cross_field_rules : list[dict]
            Each dict: ``{"name": "...", "condition": "col_a > col_b"}``.
        """
        total = df.count()
        if total == 0:
            return {}

        result = {}
        for rule in cross_field_rules:
            try:
                passing = df.filter(F.expr(rule["condition"])).count()
                result[rule["name"]] = round(passing / total * 100, 2)
            except Exception as exc:
                logger.error("Consistency rule '{}' failed: {}", rule["name"], exc)
                result[rule["name"]] = 0.0
        return result

    # ------------------------------------------------------------------
    # Aggregated quality score
    # ------------------------------------------------------------------

    def compute_overall_score(
        self,
        df: DataFrame,
        required_columns: Optional[List[str]] = None,
        validity_rules: Optional[Dict[str, str]] = None,
        consistency_rules: Optional[List[Dict]] = None,
        timestamp_col: Optional[str] = None,
        weights: Optional[Dict[str, float]] = None,
    ) -> Dict:
        """Compute a weighted overall DQ score across all dimensions.

        Parameters
        ----------
        weights : dict | None
            ``{dimension: weight}`` — must sum to 1.0.
            Default equal weighting across available dimensions.
        """
        scores: Dict[str, float] = {}

        # Completeness
        comp = self.completeness(df, required_columns)
        scores["completeness"] = sum(comp.values()) / len(comp) if comp else 0

        # Uniqueness
        uniq = self.uniqueness(df, required_columns)
        scores["uniqueness"] = sum(uniq.values()) / len(uniq) if uniq else 0

        # Validity
        if validity_rules:
            val = self.validity(df, validity_rules)
            scores["validity"] = sum(val.values()) / len(val) if val else 0
        else:
            scores["validity"] = 100.0

        # Timeliness
        if timestamp_col:
            ti = self.timeliness(df, timestamp_col)
            scores["timeliness"] = ti["timeliness_pct"]
        else:
            scores["timeliness"] = 100.0

        # Consistency
        if consistency_rules:
            cons = self.consistency(df, consistency_rules)
            scores["consistency"] = sum(cons.values()) / len(cons) if cons else 0
        else:
            scores["consistency"] = 100.0

        # Weighted aggregate
        if weights is None:
            weights = {k: 1.0 / len(scores) for k in scores}

        overall = sum(scores[d] * weights.get(d, 0) for d in scores)

        result = {
            "overall_score": round(overall, 2),
            "dimensions": scores,
            "weights": weights,
            "row_count": df.count(),
            "timestamp": datetime.now().isoformat(),
        }

        logger.info("Overall DQ score: {:.2f}%", overall)
        return result

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_metrics(self, metrics: Dict, label: str = "dq_metrics") -> Path:
        """Save metrics snapshot as a JSON file."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.metrics_path / f"{label}_{ts}.json"
        with open(path, "w") as f:
            json.dump(metrics, f, indent=2)
        logger.info("Metrics saved → {}", path)
        return path

    def save_metrics_as_delta(self, metrics: Dict, table_name: str = "quality_metrics") -> None:
        """Persist metrics as a row in a Delta table for time-series tracking."""
        import pandas as pd

        flat = {
            "timestamp": metrics["timestamp"],
            "overall_score": metrics["overall_score"],
            "row_count": metrics["row_count"],
        }
        for dim, score in metrics.get("dimensions", {}).items():
            flat[f"dim_{dim}"] = score

        pdf = pd.DataFrame([flat])
        sdf = self.spark.createDataFrame(pdf)

        path = str(self.metrics_path / table_name)
        sdf.write.format("delta").mode("append").save(path)
        logger.info("Metrics appended to Delta table → {}", path)
