# ============================================================================
# Adaptive Data Governance Framework
# src/quality/bayesian_scorer.py
# ============================================================================
# Bayesian Adaptive DQ Threshold Engine
#
# NOVEL CONTRIBUTION: Replaces the frequentist μ − kσ approach with a
# Bayesian conjugate-prior model that provides:
#
#   1. Posterior credible intervals for the "true" DQ score distribution
#   2. Principled uncertainty quantification (narrow CI = confident threshold)
#   3. Prior regularisation on cold-start (first 1-5 runs)
#   4. Automatic sensitivity scaling via posterior variance
#   5. CUSUM (Cumulative Sum Control Chart) for change-point detection
#
# Mathematical Formulation:
#   Prior:     μ ~ Normal(μ₀, σ₀²/κ₀),  σ² ~ Inv-Gamma(α₀, β₀)
#   Posterior: After observing N scores x₁…xₙ, update via conjugate:
#     κₙ = κ₀ + N
#     μₙ = (κ₀·μ₀ + Σxᵢ) / κₙ
#     αₙ = α₀ + N/2
#     βₙ = β₀ + 0.5·Σ(xᵢ − x̄)² + (κ₀·N·(x̄ − μ₀)²) / (2·κₙ)
#   Threshold: Lower bound of 95% credible interval of posterior predictive
#
# References:
#   - Murphy, K.P. (2007) Conjugate Bayesian analysis of the Gaussian
#     distribution. UBC Tech Report.
#   - Page, E.S. (1954) Continuous inspection schemes. Biometrika.
#   - Adams & MacKay (2007) Bayesian Online Changepoint Detection.
# ============================================================================

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger
from scipy import stats as sp_stats


class BayesianDQScorer:
    """Bayesian conjugate-prior adaptive DQ scorer.

    Uses a Normal-Inverse-Gamma (NIG) conjugate model to maintain a
    posterior belief about the **population DQ score distribution**,
    updated after every pipeline run.

    Parameters
    ----------
    history_dir : str
        JSON-based persistence directory.
    prior_mean : float
        μ₀ — prior belief about mean DQ score (default 85.0).
    prior_strength : float
        κ₀ — how many "virtual observations" the prior is worth.
        Higher = more resistant to early noisy data. (default 3.0)
    prior_alpha : float
        α₀ — shape of Inv-Gamma prior on variance (default 2.0).
    prior_beta : float
        β₀ — scale of Inv-Gamma prior on variance (default 50.0).
    credible_level : float
        Probability mass for the credible interval (default 0.95).
    min_threshold : float
        Absolute floor for the adaptive threshold (default 70.0).
    max_threshold : float
        Ceiling for the adaptive threshold (default 99.0).
    cusum_drift : float
        CUSUM allowable drift parameter δ (default 2.0).
    cusum_limit : float
        CUSUM decision interval h (default 5.0).
    """

    def __init__(
        self,
        history_dir: str = "data/metrics/adaptive",
        prior_mean: float = 85.0,
        prior_strength: float = 3.0,
        prior_alpha: float = 2.0,
        prior_beta: float = 50.0,
        credible_level: float = 0.95,
        min_threshold: float = 70.0,
        max_threshold: float = 99.0,
        cusum_drift: float = 2.0,
        cusum_limit: float = 5.0,
    ):
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(parents=True, exist_ok=True)

        # NIG prior hyperparameters
        self.mu0 = prior_mean
        self.kappa0 = prior_strength
        self.alpha0 = prior_alpha
        self.beta0 = prior_beta

        self.credible_level = credible_level
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

        # CUSUM parameters (Page, 1954)
        self.cusum_drift = cusum_drift
        self.cusum_limit = cusum_limit

    # ------------------------------------------------------------------
    # Bayesian posterior computation
    # ------------------------------------------------------------------

    def _compute_posterior(
        self, scores: List[float],
    ) -> Dict[str, float]:
        """Update NIG prior with observed scores → posterior.

        Returns posterior hyperparameters and derived quantities:
        - posterior_mean, posterior_var (of μ)
        - predictive_mean, predictive_std (of next observation)
        - credible_lower, credible_upper
        """
        n = len(scores)
        if n == 0:
            # Pure prior — use wide credible interval
            pred_var = self.beta0 / (self.alpha0 - 1) if self.alpha0 > 1 else 100.0
            pred_std = math.sqrt(pred_var * (1 + 1 / self.kappa0))
            # Use prior mean minus 2 standard deviations as lower bound
            ci_lower = self.mu0 - 2.0 * pred_std
            ci_upper = self.mu0 + 2.0 * pred_std
            return {
                "kappa_n": self.kappa0,
                "mu_n": self.mu0,
                "alpha_n": self.alpha0,
                "beta_n": self.beta0,
                "posterior_mean": self.mu0,
                "posterior_var": pred_var / self.kappa0,
                "predictive_mean": self.mu0,
                "predictive_std": round(pred_std, 4),
                "credible_lower": round(ci_lower, 4),
                "credible_upper": round(ci_upper, 4),
                "degrees_of_freedom": 2 * self.alpha0,
                "observations": 0,
            }

        x_bar = float(np.mean(scores))
        ss = float(np.sum((np.array(scores) - x_bar) ** 2))

        # Posterior hyperparameters (Murphy, 2007)
        kappa_n = self.kappa0 + n
        mu_n = (self.kappa0 * self.mu0 + n * x_bar) / kappa_n
        alpha_n = self.alpha0 + n / 2.0
        beta_n = (
            self.beta0
            + 0.5 * ss
            + (self.kappa0 * n * (x_bar - self.mu0) ** 2) / (2.0 * kappa_n)
        )

        # Posterior of μ: t-distribution
        # μ|data ~ t_{2αₙ}(μₙ, βₙ/(αₙ·κₙ))
        post_scale = beta_n / (alpha_n * kappa_n)
        post_df = 2 * alpha_n

        # Posterior predictive: next observation ~ t_{2αₙ}(μₙ, βₙ(1+1/κₙ)/αₙ)
        pred_scale = beta_n * (1 + 1 / kappa_n) / alpha_n
        pred_std = math.sqrt(pred_scale) if pred_scale > 0 else 1.0

        # Credible interval of posterior predictive
        tail = (1 - self.credible_level) / 2
        t_crit = sp_stats.t.ppf(tail, df=post_df)
        ci_lower = mu_n + t_crit * pred_std
        ci_upper = mu_n - t_crit * pred_std

        return {
            "kappa_n": kappa_n,
            "mu_n": round(mu_n, 4),
            "alpha_n": alpha_n,
            "beta_n": round(beta_n, 4),
            "posterior_mean": round(mu_n, 4),
            "posterior_var": round(float(post_scale), 4),
            "predictive_mean": round(mu_n, 4),
            "predictive_std": round(pred_std, 4),
            "credible_lower": round(ci_lower, 4),
            "credible_upper": round(ci_upper, 4),
            "degrees_of_freedom": round(post_df, 2),
            "observations": n,
        }

    # ------------------------------------------------------------------
    # Adaptive threshold (Bayesian)
    # ------------------------------------------------------------------

    def compute_adaptive_threshold(
        self, label: str = "silver_orders",
    ) -> Dict[str, Any]:
        """Compute adaptive threshold from Bayesian posterior.

        The threshold is the **lower bound** of the posterior predictive
        credible interval, clipped to [min_threshold, max_threshold].

        This is academically rigorous because:
        1. Cold-start: prior dominates → threshold ≈ prior_mean − margin
        2. With data: posterior tightens → threshold rises toward actual mean
        3. High variance data: wide CI → lower threshold (more lenient)
        4. Stable data: narrow CI → threshold tracks mean closely
        """
        history = self._load_history(label)
        scores = [h["overall_score"] for h in history[-200:]]

        posterior = self._compute_posterior(scores)
        raw_threshold = posterior["credible_lower"]
        threshold = max(self.min_threshold, min(self.max_threshold, raw_threshold))

        # Trend analysis
        trend = self._compute_trend(scores) if len(scores) >= 2 else 0.0

        result = {
            "threshold": round(threshold, 2),
            "method": "bayesian_nig_posterior",
            "posterior": posterior,
            "trend": round(trend, 4),
            "history_count": len(scores),
            "status": (
                "default_prior" if len(scores) < 3
                else "declining" if trend < -0.5
                else "stable" if abs(trend) <= 0.5
                else "improving"
            ),
        }

        logger.info(
            "Bayesian threshold: {:.2f}% (posterior μ={:.2f}, σ_pred={:.2f}, "
            "CI=[{:.2f}, {:.2f}], n={})",
            threshold,
            posterior["posterior_mean"],
            posterior["predictive_std"],
            posterior["credible_lower"],
            posterior["credible_upper"],
            posterior["observations"],
        )
        return result

    # ------------------------------------------------------------------
    # CUSUM Change-Point Detection (Page, 1954)
    # ------------------------------------------------------------------

    def cusum_detect(
        self, label: str = "silver_orders",
    ) -> Dict[str, Any]:
        """CUSUM (Cumulative Sum) control chart for DQ score change-point.

        Detects both upward and downward shifts in the mean DQ score.
        Uses the target as the posterior mean and monitors deviations.

        Parameters are ``cusum_drift`` (δ) and ``cusum_limit`` (h):
        - S⁺ₜ = max(0, S⁺ₜ₋₁ + (xₜ − μ − δ/2))   (detects upward shift)
        - S⁻ₜ = max(0, S⁻ₜ₋₁ + (μ − δ/2 − xₜ))   (detects downward shift)
        - Signal if S⁺ₜ > h or S⁻ₜ > h

        Returns
        -------
        dict
            ``change_detected``, ``direction``, ``signal_strength``,
            ``cusum_pos``, ``cusum_neg``, ``alarm_index``.
        """
        history = self._load_history(label)
        scores = [h["overall_score"] for h in history[-200:]]

        if len(scores) < 5:
            return {
                "change_detected": False,
                "reason": "insufficient_history",
                "history_count": len(scores),
            }

        target = float(np.mean(scores[:max(5, len(scores) // 2)]))
        half_drift = self.cusum_drift / 2.0

        s_pos = 0.0
        s_neg = 0.0
        alarm_idx = -1
        direction = None
        cusum_pos_series = []
        cusum_neg_series = []

        for i, x in enumerate(scores):
            s_pos = max(0, s_pos + (x - target - half_drift))
            s_neg = max(0, s_neg + (target - half_drift - x))
            cusum_pos_series.append(round(s_pos, 4))
            cusum_neg_series.append(round(s_neg, 4))

            if alarm_idx < 0:
                if s_pos > self.cusum_limit:
                    alarm_idx = i
                    direction = "upward_shift"
                elif s_neg > self.cusum_limit:
                    alarm_idx = i
                    direction = "downward_shift"

        result = {
            "change_detected": alarm_idx >= 0,
            "direction": direction,
            "alarm_index": alarm_idx,
            "target_mean": round(target, 2),
            "final_cusum_pos": round(s_pos, 4),
            "final_cusum_neg": round(s_neg, 4),
            "cusum_limit_h": self.cusum_limit,
            "cusum_drift_delta": self.cusum_drift,
            "history_count": len(scores),
        }

        if alarm_idx >= 0:
            logger.warning(
                "CUSUM: {} detected at run #{} (S⁺={:.2f}, S⁻={:.2f}, h={})",
                direction, alarm_idx, s_pos, s_neg, self.cusum_limit,
            )
        else:
            logger.info("CUSUM: No change-point detected (S⁺={:.2f}, S⁻={:.2f})",
                        s_pos, s_neg)
        return result

    # ------------------------------------------------------------------
    # Dimension weight learning (Bayesian posterior weighting)
    # ------------------------------------------------------------------

    def learn_dimension_weights(
        self, label: str = "silver_orders",
    ) -> Dict[str, float]:
        """Bayesian dimension weighting using posterior variance.

        Dimensions with higher posterior variance (less stable) get
        higher weight — they need more attention. This is the Bayesian
        analogue of inverse-mean weighting but grounded in uncertainty.

        Weight_d ∝ 1 / posterior_mean_d × posterior_std_d
        (lower mean AND higher variance = more problematic = higher weight)
        """
        history = self._load_history(label)
        recent = history[-200:]

        if len(recent) < 3:
            logger.info("Bayesian weights: <3 history points — equal weights")
            return {}

        dim_scores: Dict[str, List[float]] = {}
        for h in recent:
            for d, s in h.get("dimensions", {}).items():
                dim_scores.setdefault(d, []).append(s)

        if not dim_scores:
            return {}

        raw_weights = {}
        for d, scores in dim_scores.items():
            posterior = self._compute_posterior(scores)
            mean = max(posterior["posterior_mean"], 1.0)
            std = max(posterior["predictive_std"], 0.1)
            # Weight = uncertainty / mean (high uncertainty + low mean → high weight)
            raw_weights[d] = std / mean

        total = sum(raw_weights.values())
        if total == 0:
            return {}

        weights = {d: round(v / total, 4) for d, v in raw_weights.items()}

        logger.info("Bayesian dimension weights: {}", weights)
        return weights

    # ------------------------------------------------------------------
    # ML-based weight learning (linear regression, same as before)
    # ------------------------------------------------------------------

    def learn_weights_regression(
        self,
        label: str = "silver_orders",
        target_score: float = 95.0,
    ) -> Dict[str, float]:
        """Linear regression weights with cross-validation."""
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import cross_val_score

        history = self._load_history(label)
        recent = history[-200:]

        if len(recent) < 5:
            logger.info("Regression weights: <5 data points — skip")
            return {}

        dim_names = sorted(
            set().union(*(h.get("dimensions", {}).keys() for h in recent))
        )

        X, y = [], []
        for h in recent:
            dims = h.get("dimensions", {})
            X.append([dims.get(d, 0.0) for d in dim_names])
            y.append(h.get("overall_score", 0.0))

        X_arr, y_arr = np.array(X), np.array(y)
        if X_arr.shape[0] < X_arr.shape[1]:
            return {}

        model = LinearRegression(fit_intercept=True)
        model.fit(X_arr, y_arr)

        # Cross-validated R² for statistical rigor
        if len(X_arr) >= 10:
            cv_scores = cross_val_score(model, X_arr, y_arr, cv=min(5, len(X_arr)), scoring="r2")
            cv_r2 = float(np.mean(cv_scores))
        else:
            cv_r2 = model.score(X_arr, y_arr)

        coefs = np.abs(model.coef_)
        total = coefs.sum()
        if total == 0:
            return {}

        weights = {dim_names[i]: round(float(c / total), 4) for i, c in enumerate(coefs)}
        logger.info("Regression weights (CV R²={:.3f}): {}", cv_r2, weights)
        return weights

    # ------------------------------------------------------------------
    # Early warning (enhanced with Bayesian prediction intervals)
    # ------------------------------------------------------------------

    def check_early_warning(
        self,
        current_score: float,
        label: str = "silver_orders",
    ) -> Dict[str, Any]:
        """Enhanced early warning using Bayesian posterior predictive.

        Alerts when current score falls below the posterior predictive
        mean minus 1 predictive std (i.e., it's an unlikely observation
        given the learned DQ distribution).
        """
        history = self._load_history(label)
        scores = [h["overall_score"] for h in history[-200:]]

        if len(scores) < 3:
            return {"alert_level": "none", "reason": "insufficient_history"}

        posterior = self._compute_posterior(scores)
        pred_mean = posterior["predictive_mean"]
        pred_std = posterior["predictive_std"]

        # Bayesian surprise: how many predictive stddevs away?
        surprise = (pred_mean - current_score) / max(pred_std, 0.01)

        # CUSUM check
        cusum = self.cusum_detect(label)

        # Consecutive decline check
        last3 = scores[-3:]
        consecutive_decline = all(last3[i] > last3[i + 1] for i in range(len(last3) - 1))

        trend = self._compute_trend(scores)

        # Multi-signal alert
        if surprise > 2.0 and cusum.get("change_detected") and consecutive_decline:
            alert_level = "critical"
        elif surprise > 1.5 or cusum.get("change_detected"):
            alert_level = "warning"
        elif surprise > 1.0 or current_score < pred_mean:
            alert_level = "info"
        else:
            alert_level = "none"

        result = {
            "alert_level": alert_level,
            "current_score": current_score,
            "predictive_mean": round(pred_mean, 2),
            "predictive_std": round(pred_std, 2),
            "bayesian_surprise": round(surprise, 4),
            "cusum_change_detected": cusum.get("change_detected", False),
            "consecutive_decline": consecutive_decline,
            "trend_slope": round(trend, 4),
            "recommendation": {
                "none": "DQ is within expected Bayesian posterior range.",
                "info": "DQ is below posterior mean — monitor.",
                "warning": "DQ is significantly below expected range — investigate.",
                "critical": "DQ is in rapid decline with change-point detected — HALT.",
            }.get(alert_level, ""),
        }

        if alert_level not in ("none",):
            logger.warning("Early warning [{}]: surprise={:.2f}σ", alert_level, surprise)
        return result

    # ------------------------------------------------------------------
    # Record run
    # ------------------------------------------------------------------

    def record_run(self, metrics: Dict[str, Any], label: str = "silver_orders") -> None:
        """Append a DQ run's metrics to the Bayesian history."""
        history = self._load_history(label)
        history.append({
            "overall_score": metrics.get("overall_score", 0),
            "dimensions": metrics.get("dimensions", {}),
            "row_count": metrics.get("row_count", 0),
            "timestamp": datetime.now().isoformat(),
        })
        history = history[-200:]
        self._save_history(label, history)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_trend(scores: List[float]) -> float:
        if len(scores) < 2:
            return 0.0
        x = np.arange(len(scores), dtype=float)
        slope, _ = np.polyfit(x, np.array(scores, dtype=float), 1)
        return float(slope)

    def _history_file(self, label: str) -> Path:
        return self.history_dir / f"{label}_bayesian_history.json"

    def _load_history(self, label: str) -> List[Dict]:
        path = self._history_file(label)
        if path.exists():
            with open(path) as f:
                return json.load(f)
        # Fall back to legacy adaptive history for migration
        legacy = self.history_dir / f"{label}_adaptive_history.json"
        if legacy.exists():
            with open(legacy) as f:
                return json.load(f)
        return []

    def _save_history(self, label: str, history: List[Dict]) -> None:
        path = self._history_file(label)
        with open(path, "w") as f:
            json.dump(history, f, indent=2)
