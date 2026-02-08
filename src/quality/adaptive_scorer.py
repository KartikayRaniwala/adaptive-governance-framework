# ============================================================================
# Adaptive Data Governance Framework
# src/quality/adaptive_scorer.py
# ============================================================================
# Adaptive DQ Threshold Engine
#
# Instead of hard-coding a single DQ threshold (e.g. 85 %), this module
# learns from historical pipeline runs to:
#
#   1. Auto-adjust DQ dimension weights based on failure patterns
#      (dimensions that historically fail more get higher weight).
#   2. Set dynamic pass/fail thresholds using a rolling baseline
#      (mean − k·std of last N runs).
#   3. Provide trend analysis and early-warning alerts when DQ scores
#      are declining even if still above threshold.
#
# Uses scikit-learn linear regression to learn weight adjustments and
# NumPy for statistical computations.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger


# ============================================================================
# AdaptiveDQScorer
# ============================================================================


class AdaptiveDQScorer:
    """Self-tuning data quality scorer that adapts thresholds and weights.

    Parameters
    ----------
    history_dir : str
        Directory where per-label metric history is stored as JSON.
    baseline_window : int
        Number of recent runs to include in the baseline calculation.
    sensitivity : float
        Number of standard deviations below the rolling mean to set the
        adaptive threshold.  Lower = stricter (default 1.5).
    min_threshold : float
        Absolute minimum DQ score that the adaptive threshold can never
        go below (safety floor, default 70.0).
    max_threshold : float
        Absolute maximum so that threshold doesn't become unreachable
        (default 99.0).
    """

    def __init__(
        self,
        history_dir: str = "data/metrics/adaptive",
        baseline_window: int = 20,
        sensitivity: float = 1.5,
        min_threshold: float = 70.0,
        max_threshold: float = 99.0,
    ):
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        self.baseline_window = baseline_window
        self.sensitivity = sensitivity
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    # ------------------------------------------------------------------
    # Adaptive threshold
    # ------------------------------------------------------------------

    def compute_adaptive_threshold(
        self, label: str = "silver_orders"
    ) -> Dict[str, Any]:
        """Compute a dynamic pass/fail threshold from recent history.

        Strategy:
            adaptive_threshold = max(
                min_threshold,
                min(max_threshold, rolling_mean - sensitivity * rolling_std)
            )

        Returns
        -------
        dict
            ``threshold``, ``rolling_mean``, ``rolling_std``,
            ``history_count``, ``trend`` (slope of last N scores).
        """
        history = self._load_history(label)
        recent = history[-self.baseline_window:]

        if len(recent) < 3:
            logger.info(
                "Adaptive threshold: <3 history points — using default 85.0"
            )
            return {
                "threshold": 85.0,
                "rolling_mean": None,
                "rolling_std": None,
                "history_count": len(recent),
                "trend": 0.0,
                "status": "default",
            }

        scores = [h["overall_score"] for h in recent]
        rolling_mean = float(np.mean(scores))
        rolling_std = float(np.std(scores))

        threshold = rolling_mean - self.sensitivity * rolling_std
        threshold = max(self.min_threshold, min(self.max_threshold, threshold))

        # Trend: least-squares slope over time
        trend = self._compute_trend(scores)

        result = {
            "threshold": round(threshold, 2),
            "rolling_mean": round(rolling_mean, 2),
            "rolling_std": round(rolling_std, 2),
            "history_count": len(recent),
            "trend": round(trend, 4),
            "status": (
                "declining" if trend < -0.5
                else "stable" if abs(trend) <= 0.5
                else "improving"
            ),
        }

        logger.info(
            "Adaptive threshold: {:.2f}% (mean={:.2f}±{:.2f}, trend={})",
            threshold, rolling_mean, rolling_std, result["status"],
        )
        return result

    # ------------------------------------------------------------------
    # Adaptive dimension weights
    # ------------------------------------------------------------------

    def learn_dimension_weights(
        self, label: str = "silver_orders",
    ) -> Dict[str, float]:
        """Learn optimal DQ dimension weights from historical data.

        Strategy: dimensions that have lower average scores (i.e. are
        more problematic) get higher weight so they have more influence
        on the overall score.  This surfaces the weakest dimensions.

        Uses inverse-mean weighting, normalised to sum to 1.0.

        Returns
        -------
        dict[str, float]
            ``{dimension_name: weight}`` summing to 1.0.
        """
        history = self._load_history(label)
        recent = history[-self.baseline_window:]

        if len(recent) < 3:
            logger.info("Weight learning: <3 history points — equal weights")
            return {}

        # Collect dimension scores
        dim_scores: Dict[str, List[float]] = {}
        for h in recent:
            dims = h.get("dimensions", {})
            for d, s in dims.items():
                dim_scores.setdefault(d, []).append(s)

        if not dim_scores:
            return {}

        # Inverse-mean weighting: lower avg → higher weight
        dim_means = {d: np.mean(v) for d, v in dim_scores.items()}
        # Add small epsilon to avoid division by zero
        inv_means = {d: 1.0 / (m + 1e-6) for d, m in dim_means.items()}
        total = sum(inv_means.values())
        weights = {d: round(v / total, 4) for d, v in inv_means.items()}

        logger.info(
            "Learned dimension weights: {}",
            {d: f"{w:.3f}" for d, w in weights.items()},
        )
        return weights

    # ------------------------------------------------------------------
    # ML-based weight learning (linear regression)
    # ------------------------------------------------------------------

    def learn_weights_regression(
        self,
        label: str = "silver_orders",
        target_score: float = 95.0,
    ) -> Dict[str, float]:
        """Use linear regression to learn weights that best predict
        the overall score from dimension scores.

        If a target_score is provided, the weights are adjusted to
        emphasise dimensions whose variance most affects the overall
        score (higher coefficient = more importance).

        Returns
        -------
        dict[str, float]
            Normalised dimension weights.
        """
        from sklearn.linear_model import LinearRegression

        history = self._load_history(label)
        recent = history[-self.baseline_window:]

        if len(recent) < 5:
            logger.info("Regression weights: <5 data points — equal weights")
            return {}

        # Build feature matrix
        dim_names = sorted(
            set().union(*(h.get("dimensions", {}).keys() for h in recent))
        )

        X = []
        y = []
        for h in recent:
            dims = h.get("dimensions", {})
            row = [dims.get(d, 0.0) for d in dim_names]
            X.append(row)
            y.append(h.get("overall_score", 0.0))

        X_arr = np.array(X)
        y_arr = np.array(y)

        if X_arr.shape[0] < X_arr.shape[1]:
            logger.warning("Regression: fewer samples than features — skip")
            return {}

        model = LinearRegression(fit_intercept=True)
        model.fit(X_arr, y_arr)

        # Normalise absolute coefficients as weights
        coefs = np.abs(model.coef_)
        total = coefs.sum()
        if total == 0:
            return {}

        weights = {dim_names[i]: round(float(c / total), 4) for i, c in enumerate(coefs)}

        logger.info(
            "Regression weights (R²={:.3f}): {}",
            model.score(X_arr, y_arr),
            {d: f"{w:.3f}" for d, w in weights.items()},
        )
        return weights

    # ------------------------------------------------------------------
    # Early-warning system
    # ------------------------------------------------------------------

    def check_early_warning(
        self,
        current_score: float,
        label: str = "silver_orders",
    ) -> Dict[str, Any]:
        """Detect if DQ is on a declining trend even if still above threshold.

        Alerts when:
        1. Last 3 runs show a consistent decline.
        2. Current score < rolling_mean (even if above threshold).
        3. Rate of decline is accelerating.

        Returns
        -------
        dict
            ``alert_level`` in ``("none", "info", "warning", "critical")``.
        """
        history = self._load_history(label)
        recent = history[-self.baseline_window:]

        if len(recent) < 3:
            return {"alert_level": "none", "reason": "insufficient_history"}

        scores = [h["overall_score"] for h in recent]
        rolling_mean = np.mean(scores)
        trend = self._compute_trend(scores)

        # Check last 3 runs for consecutive decline
        last3 = scores[-3:]
        consecutive_decline = all(
            last3[i] > last3[i + 1] for i in range(len(last3) - 1)
        )

        # Determine alert level
        if consecutive_decline and current_score < rolling_mean and trend < -1.0:
            alert_level = "critical"
        elif consecutive_decline and trend < -0.5:
            alert_level = "warning"
        elif current_score < rolling_mean:
            alert_level = "info"
        else:
            alert_level = "none"

        result = {
            "alert_level": alert_level,
            "current_score": current_score,
            "rolling_mean": round(float(rolling_mean), 2),
            "trend_slope": round(trend, 4),
            "consecutive_decline": consecutive_decline,
            "last_3_scores": [round(s, 2) for s in last3],
            "recommendation": {
                "none": "DQ is stable or improving.",
                "info": "DQ is below average — monitor closely.",
                "warning": "DQ is declining — review recent data sources.",
                "critical": "DQ is in rapid decline — immediate investigation needed.",
            }.get(alert_level, ""),
        }

        if alert_level != "none":
            logger.warning(
                "Early warning [{}]: score={:.2f}, trend={:.4f}, consecutive_decline={}",
                alert_level, current_score, trend, consecutive_decline,
            )

        return result

    # ------------------------------------------------------------------
    # Record current run to history
    # ------------------------------------------------------------------

    def record_run(
        self,
        metrics: Dict[str, Any],
        label: str = "silver_orders",
    ) -> None:
        """Append a DQ run's metrics to the adaptive history."""
        history = self._load_history(label)
        history.append({
            "overall_score": metrics.get("overall_score", 0),
            "dimensions": metrics.get("dimensions", {}),
            "row_count": metrics.get("row_count", 0),
            "timestamp": datetime.now().isoformat(),
        })
        # Keep sliding window
        history = history[-200:]
        self._save_history(label, history)
        logger.info(
            "Recorded DQ run #{} for '{}' (score={:.2f})",
            len(history), label, metrics.get("overall_score", 0),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_trend(scores: List[float]) -> float:
        """Least-squares slope over the score series."""
        if len(scores) < 2:
            return 0.0
        x = np.arange(len(scores), dtype=float)
        y = np.array(scores, dtype=float)
        # polyfit degree 1 → [slope, intercept]
        slope, _ = np.polyfit(x, y, 1)
        return float(slope)

    def _history_file(self, label: str) -> Path:
        return self.history_dir / f"{label}_adaptive_history.json"

    def _load_history(self, label: str) -> List[Dict]:
        path = self._history_file(label)
        if path.exists():
            with open(path) as f:
                return json.load(f)
        return []

    def _save_history(self, label: str, history: List[Dict]) -> None:
        path = self._history_file(label)
        with open(path, "w") as f:
            json.dump(history, f, indent=2)
