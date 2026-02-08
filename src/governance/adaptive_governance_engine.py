# ============================================================================
# Adaptive Data Governance Framework
# src/governance/adaptive_governance_engine.py
# ============================================================================
# Central Adaptive Governance Engine
#
# This is the "brain" that makes the framework truly adaptive. It ties
# together every AI/ML component into a single orchestration layer:
#
#   1. Anomaly Detection (Z-score, IQR, Isolation Forest)
#   2. Adaptive DQ Scoring (dynamic thresholds, learned weights)
#   3. PII Confidence Tuning (feedback-driven F1 optimisation)
#   4. Self-Learning Feedback Loop (records outcomes, retrains)
#
# The engine is invoked by the Airflow DQ gate task instead of a
# hard-coded threshold check.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from pyspark.sql import DataFrame, SparkSession


# ============================================================================
# AdaptiveGovernanceEngine
# ============================================================================


class AdaptiveGovernanceEngine:
    """Unified adaptive governance orchestrator.

    Wraps anomaly detection, adaptive DQ scoring, PII tuning, and
    feedback loops into a single ``evaluate()`` call that the Airflow
    DAG can invoke.

    Parameters
    ----------
    spark : SparkSession
    data_root : str
        Base path of the data lake (e.g. ``/opt/framework/data``).
    config : dict | None
        Override configuration; keys ``z_threshold``, ``iqr_factor``,
        ``contamination``, ``sensitivity``, ``baseline_window``.
    """

    def __init__(
        self,
        spark: SparkSession,
        data_root: str = "/opt/framework/data",
        config: Optional[Dict[str, Any]] = None,
    ):
        self.spark = spark
        self.data_root = data_root
        self._cfg = config or {}

        # Lazy-init sub-components
        self._anomaly_detector = None
        self._adaptive_scorer = None
        self._pii_tuner = None

    # ------------------------------------------------------------------
    # Lazy component initialisation
    # ------------------------------------------------------------------

    @property
    def anomaly_detector(self):
        if self._anomaly_detector is None:
            from src.quality.anomaly_detector import AnomalyDetector
            self._anomaly_detector = AnomalyDetector(
                spark=self.spark,
                z_threshold=self._cfg.get("z_threshold", 3.0),
                iqr_factor=self._cfg.get("iqr_factor", 1.5),
                contamination=self._cfg.get("contamination", 0.05),
                history_path=f"{self.data_root}/metrics/anomaly_history",
            )
        return self._anomaly_detector

    @property
    def adaptive_scorer(self):
        if self._adaptive_scorer is None:
            from src.quality.adaptive_scorer import AdaptiveDQScorer
            self._adaptive_scorer = AdaptiveDQScorer(
                history_dir=f"{self.data_root}/metrics/adaptive",
                baseline_window=self._cfg.get("baseline_window", 20),
                sensitivity=self._cfg.get("sensitivity", 1.5),
            )
        return self._adaptive_scorer

    @property
    def pii_tuner(self):
        if self._pii_tuner is None:
            from src.pii_detection.adaptive_pii_tuner import AdaptivePIITuner
            self._pii_tuner = AdaptivePIITuner(
                feedback_dir=f"{self.data_root}/metrics/pii_feedback",
            )
        return self._pii_tuner

    # ------------------------------------------------------------------
    # Main evaluation entry point
    # ------------------------------------------------------------------

    def evaluate(
        self,
        df: DataFrame,
        label: str = "silver_orders",
        required_columns: Optional[List[str]] = None,
        validity_rules: Optional[Dict[str, str]] = None,
        numeric_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Run the full adaptive governance evaluation.

        Steps:
        1. Compute standard DQ metrics (completeness, validity, etc.)
        2. Run statistical anomaly detection on the DataFrame
        3. Compute an adaptive threshold from history
        4. Learn dimension weights from past runs
        5. Re-score using learned weights
        6. Check early-warning signals
        7. Detect batch-level anomaly
        8. Record the run for future learning
        9. Return a comprehensive report with pass/fail decision

        Parameters
        ----------
        df : DataFrame
            The Silver-layer DataFrame to evaluate.
        label : str
            Label for the metric series.
        required_columns : list[str] | None
            Columns to check for completeness.
        validity_rules : dict | None
            ``{column: regex}`` for validity checks.
        numeric_columns : list[str] | None
            Columns for anomaly detection; auto-detected if None.

        Returns
        -------
        dict
            Full governance report with keys:
            ``dq_metrics``, ``anomaly_report``, ``adaptive_threshold``,
            ``learned_weights``, ``early_warning``, ``batch_anomaly``,
            ``decision`` ("PASS" / "FAIL" / "WARN"), ``score``.
        """
        logger.info("═══ Adaptive Governance Evaluation: '{}' ═══", label)

        # ---- Step 1: Standard DQ metrics ----
        from src.quality.quality_metrics import QualityMetrics
        qm = QualityMetrics(self.spark, metrics_path=f"{self.data_root}/metrics")

        # Get learned weights (if any)
        learned_weights = self.adaptive_scorer.learn_dimension_weights(label)

        dq_metrics = qm.compute_overall_score(
            df,
            required_columns=required_columns,
            validity_rules=validity_rules,
            weights=learned_weights if learned_weights else None,
        )
        qm.save_metrics(dq_metrics, label=label)

        overall_score = dq_metrics["overall_score"]
        logger.info("DQ score: {:.2f}% (weights={})", overall_score,
                     "learned" if learned_weights else "equal")

        # ---- Step 2: Anomaly detection (Z-score + IQR + Isolation Forest) ----
        # 2a: Z-score
        try:
            _, zscore_report = self.anomaly_detector.zscore_detect(
                df, numeric_columns
            )
        except Exception as exc:
            logger.warning("Z-score anomaly detection skipped: {}", exc)
            zscore_report = {"skipped": True, "reason": str(exc)}

        # 2b: IQR fence detection
        try:
            _, iqr_report = self.anomaly_detector.iqr_detect(
                df, numeric_columns
            )
        except Exception as exc:
            logger.warning("IQR anomaly detection skipped: {}", exc)
            iqr_report = {"skipped": True, "reason": str(exc)}

        # 2c: Isolation Forest (ML-based)
        try:
            _, iforest_report = self.anomaly_detector.isolation_forest_detect(
                df, numeric_columns
            )
        except Exception as exc:
            logger.warning("Isolation Forest detection skipped: {}", exc)
            iforest_report = {"skipped": True, "reason": str(exc)}

        # Combined anomaly summary
        anomaly_report = {
            "zscore": zscore_report,
            "iqr": iqr_report,
            "isolation_forest": iforest_report,
            "combined_anomaly_rows": sum(
                r.get("anomaly_rows", 0)
                for r in [zscore_report, iqr_report, iforest_report]
                if not r.get("skipped")
            ),
        }

        # ---- Step 3: Adaptive threshold ----
        threshold_info = self.adaptive_scorer.compute_adaptive_threshold(label)
        adaptive_threshold = threshold_info["threshold"]

        # ---- Step 4: Early warning ----
        early_warning = self.adaptive_scorer.check_early_warning(
            overall_score, label
        )

        # ---- Step 5: Batch anomaly ----
        batch_anomaly = self.anomaly_detector.detect_batch_anomaly(
            dq_metrics, label
        )

        # ---- Step 6: Record run for learning ----
        self.adaptive_scorer.record_run(dq_metrics, label)

        # ---- Step 7: PII drift check ----
        try:
            pii_drift = self.pii_tuner.detect_pii_drift()
        except Exception as exc:
            logger.warning("PII drift check skipped: {}", exc)
            pii_drift = {"has_drift": False, "reason": str(exc)}

        # ---- Step 8: PII threshold re-tuning ----
        try:
            pii_thresholds = self.pii_tuner.tune_thresholds()
        except Exception as exc:
            logger.warning("PII threshold tuning skipped: {}", exc)
            pii_thresholds = {}

        # ---- Step 9: Regression-based weight learning ----
        try:
            regression_weights = self.adaptive_scorer.learn_weights_regression(label)
        except Exception as exc:
            logger.warning("Regression weight learning skipped: {}", exc)
            regression_weights = {}

        # ---- Step 10: Decision ----
        if overall_score < adaptive_threshold:
            decision = "FAIL"
        elif early_warning.get("alert_level") in ("warning", "critical"):
            decision = "WARN"
        elif batch_anomaly.get("is_anomaly"):
            decision = "WARN"
        else:
            decision = "PASS"

        report = {
            "label": label,
            "score": overall_score,
            "adaptive_threshold": adaptive_threshold,
            "decision": decision,
            "dq_metrics": dq_metrics,
            "anomaly_report": anomaly_report,
            "threshold_info": threshold_info,
            "learned_weights": learned_weights,
            "regression_weights": regression_weights,
            "early_warning": early_warning,
            "batch_anomaly": batch_anomaly,
            "pii_drift": pii_drift,
            "pii_thresholds": pii_thresholds,
            "timestamp": datetime.now().isoformat(),
        }

        # Persist full report
        self._save_report(report, label)

        logger.info(
            "═══ Decision: {} (score={:.2f}%, threshold={:.2f}%) ═══",
            decision, overall_score, adaptive_threshold,
        )

        return report

    # ------------------------------------------------------------------
    # PII-specific adaptive evaluation
    # ------------------------------------------------------------------

    def get_adaptive_pii_thresholds(self) -> Dict[str, float]:
        """Return tuned PII confidence thresholds.

        If sufficient feedback has been recorded, returns per-entity-type
        thresholds optimised for F1.  Otherwise returns empty dict
        (caller should use defaults).
        """
        thresholds = self.pii_tuner.get_thresholds()
        if thresholds:
            logger.info("Using adaptive PII thresholds: {}", thresholds)
        return thresholds.get("thresholds", {})

    def retune_pii(self) -> Dict[str, float]:
        """Force a re-tuning of PII confidence thresholds."""
        return self.pii_tuner.tune_thresholds()

    def pii_drift_check(self) -> Dict[str, Any]:
        """Check for PII detection drift."""
        return self.pii_tuner.detect_pii_drift()

    # ------------------------------------------------------------------
    # Report persistence
    # ------------------------------------------------------------------

    def _save_report(self, report: Dict, label: str) -> None:
        out_dir = Path(f"{self.data_root}/metrics/governance_reports")
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = out_dir / f"{label}_{ts}.json"
        with open(path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("Governance report saved → {}", path)
