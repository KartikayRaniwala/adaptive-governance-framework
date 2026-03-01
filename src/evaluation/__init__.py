# ============================================================================
# Adaptive Data Governance Framework
# src/evaluation/evaluation_framework.py
# ============================================================================
# Rigorous Evaluation Framework for Dissertation
#
# Provides:
#   1. Baseline comparisons (fixed threshold vs adaptive vs Bayesian)
#   2. Ablation study (systematic component removal)
#   3. Statistical significance testing (paired t-test, Wilcoxon)
#   4. PII detection benchmarking (precision/recall/F1 per entity type)
#   5. Multi-run experiments with reproducible seeds
#
# This module addresses the academic rigor requirements for a
# dissertation-quality evaluation.
# ============================================================================

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from scipy import stats as sp_stats


class EvaluationFramework:
    """Dissertation-grade evaluation framework for adaptive governance.

    Supports multi-run experiments, baseline comparisons, ablation
    studies, and statistical significance testing.
    """

    # Framework root so relative paths resolve correctly from any CWD
    _FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]

    def __init__(
        self,
        output_dir: str = "data/evaluation",
        random_seed: int = 42,
    ):
        _p = Path(output_dir)
        self.output_dir = _p if _p.is_absolute() else self._FRAMEWORK_ROOT / _p
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seed = random_seed
        np.random.seed(random_seed)

    # ==================================================================
    # 1. Baseline Comparison: Fixed vs Adaptive vs Bayesian
    # ==================================================================

    def compare_threshold_strategies(
        self,
        dq_scores: List[float],
        labels: Optional[List[str]] = None,
        fixed_thresholds: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        """Compare three threshold strategies on the same DQ score history.

        Strategies:
        1. Fixed threshold (e.g. 85%, 80%, 75%)
        2. Frequentist adaptive (μ − kσ)
        3. Bayesian adaptive (posterior credible interval)

        Metrics: True Positive Rate, False Positive Rate, F1-Score
        where TP = correctly passed good data, FP = incorrectly passed bad data.
        """
        if fixed_thresholds is None:
            fixed_thresholds = [75.0, 80.0, 85.0, 90.0]

        n = len(dq_scores)
        results = {"n_observations": n, "strategies": {}}

        # --- Strategy 1: Fixed thresholds ---
        for ft in fixed_thresholds:
            decisions = ["PASS" if s >= ft else "FAIL" for s in dq_scores]
            results["strategies"][f"fixed_{ft}"] = {
                "threshold_type": "fixed",
                "threshold_value": ft,
                "decisions": decisions,
                "pass_rate": sum(1 for d in decisions if d == "PASS") / n,
                "thresholds_over_time": [ft] * n,
            }

        # --- Strategy 2: Frequentist μ − kσ ---
        freq_thresholds = []
        freq_decisions = []
        window = 20
        k = 1.5
        for i in range(n):
            if i < 3:
                t = 85.0
            else:
                recent = dq_scores[max(0, i - window):i]
                t = max(70.0, min(99.0, np.mean(recent) - k * np.std(recent)))
            freq_thresholds.append(round(t, 2))
            freq_decisions.append("PASS" if dq_scores[i] >= t else "FAIL")

        results["strategies"]["frequentist_mu_ksigma"] = {
            "threshold_type": "frequentist_adaptive",
            "decisions": freq_decisions,
            "pass_rate": sum(1 for d in freq_decisions if d == "PASS") / n,
            "thresholds_over_time": freq_thresholds,
            "parameters": {"window": window, "k": k},
        }

        # --- Strategy 3: Bayesian posterior ---
        from src.quality.bayesian_scorer import BayesianDQScorer
        bayes = BayesianDQScorer(
            history_dir=str(self.output_dir / "bayesian_eval"),
            prior_mean=85.0, prior_strength=3.0,
        )
        bayes_thresholds = []
        bayes_decisions = []
        for i in range(n):
            observed = dq_scores[:i]
            posterior = bayes._compute_posterior(observed)
            t = max(70.0, min(99.0, posterior["credible_lower"]))
            bayes_thresholds.append(round(t, 2))
            bayes_decisions.append("PASS" if dq_scores[i] >= t else "FAIL")

        results["strategies"]["bayesian_nig"] = {
            "threshold_type": "bayesian_adaptive",
            "decisions": bayes_decisions,
            "pass_rate": sum(1 for d in bayes_decisions if d == "PASS") / n,
            "thresholds_over_time": bayes_thresholds,
            "parameters": {
                "prior_mean": 85.0, "prior_strength": 3.0,
                "credible_level": 0.95,
            },
        }

        # --- Compute comparison metrics ---
        # Ground truth: scores < 70 are "truly bad", >= 70 are "truly good"
        # (this is a configurable assumption for the evaluation)
        ground_truth = ["GOOD" if s >= 70.0 else "BAD" for s in dq_scores]

        for name, strat in results["strategies"].items():
            tp = sum(1 for g, d in zip(ground_truth, strat["decisions"])
                     if g == "GOOD" and d == "PASS")
            fp = sum(1 for g, d in zip(ground_truth, strat["decisions"])
                     if g == "BAD" and d == "PASS")
            fn = sum(1 for g, d in zip(ground_truth, strat["decisions"])
                     if g == "GOOD" and d == "FAIL")
            tn = sum(1 for g, d in zip(ground_truth, strat["decisions"])
                     if g == "BAD" and d == "FAIL")

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

            strat["metrics"] = {
                "TP": tp, "FP": fp, "FN": fn, "TN": tn,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1_score": round(f1, 4),
                "accuracy": round((tp + tn) / n, 4) if n > 0 else 0,
            }

        # Save results
        out_path = self.output_dir / "threshold_comparison.json"
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info("Threshold comparison saved → {}", out_path)

        return results

    # ==================================================================
    # 2. Statistical Significance Testing
    # ==================================================================

    def test_significance(
        self,
        scores_a: List[float],
        scores_b: List[float],
        method_names: Tuple[str, str] = ("Method_A", "Method_B"),
        alpha: float = 0.05,
    ) -> Dict[str, Any]:
        """Paired statistical test between two methods' scores.

        Uses Wilcoxon signed-rank test (non-parametric) and paired t-test.
        Reports p-values, effect sizes (Cohen's d), and confidence intervals.
        """
        a = np.array(scores_a)
        b = np.array(scores_b)
        diff = a - b

        # Paired t-test
        t_stat, p_ttest = sp_stats.ttest_rel(a, b)

        # Wilcoxon signed-rank (non-parametric alternative)
        try:
            w_stat, p_wilcoxon = sp_stats.wilcoxon(diff)
        except ValueError:
            w_stat, p_wilcoxon = 0, 1.0

        # Cohen's d (effect size)
        pooled_std = np.sqrt((np.var(a) + np.var(b)) / 2)
        cohens_d = float(np.mean(diff) / pooled_std) if pooled_std > 0 else 0.0

        # Bootstrap 95% CI for mean difference
        np.random.seed(self.seed)
        boot_means = [
            np.mean(np.random.choice(diff, size=len(diff), replace=True))
            for _ in range(10000)
        ]
        ci_lower, ci_upper = np.percentile(boot_means, [2.5, 97.5])

        result = {
            "methods": list(method_names),
            "n_observations": len(a),
            "mean_a": round(float(np.mean(a)), 4),
            "mean_b": round(float(np.mean(b)), 4),
            "mean_difference": round(float(np.mean(diff)), 4),
            "paired_ttest": {
                "t_statistic": round(float(t_stat), 4),
                "p_value": round(float(p_ttest), 6),
                "significant": p_ttest < alpha,
            },
            "wilcoxon_test": {
                "w_statistic": round(float(w_stat), 4),
                "p_value": round(float(p_wilcoxon), 6),
                "significant": p_wilcoxon < alpha,
            },
            "effect_size": {
                "cohens_d": round(cohens_d, 4),
                "interpretation": (
                    "negligible" if abs(cohens_d) < 0.2
                    else "small" if abs(cohens_d) < 0.5
                    else "medium" if abs(cohens_d) < 0.8
                    else "large"
                ),
            },
            "confidence_interval_95": {
                "lower": round(float(ci_lower), 4),
                "upper": round(float(ci_upper), 4),
            },
            "alpha": alpha,
        }

        logger.info(
            "Significance test: {} vs {} → p={:.4f} (t-test), "
            "Cohen's d={:.3f} ({})",
            method_names[0], method_names[1],
            p_ttest, cohens_d, result["effect_size"]["interpretation"],
        )
        return result

    # ==================================================================
    # 3. PII Detection Benchmarking
    # ==================================================================

    def benchmark_pii_detection(
        self,
        test_texts: List[str],
        ground_truth_labels: List[Dict[str, List[str]]],
        detector_configs: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """Benchmark PII detection across configurations.

        Parameters
        ----------
        test_texts : list[str]
            Input texts to scan.
        ground_truth_labels : list[dict]
            For each text, a dict of {entity_type: [list_of_PII_strings]}
        detector_configs : list[dict]
            Different detector configurations to compare.
        """
        from src.pii_detection.pii_detector import PIIDetector

        if detector_configs is None:
            detector_configs = [
                {"name": "regex_only", "use_ner_model": False, "confidence_threshold": 0.85},
                {"name": "regex+ner_0.85", "use_ner_model": True, "confidence_threshold": 0.85},
                {"name": "regex+ner_0.70", "use_ner_model": True, "confidence_threshold": 0.70},
                {"name": "regex+ner_0.50", "use_ner_model": True, "confidence_threshold": 0.50},
            ]

        results = {}
        for config in detector_configs:
            name = config.pop("name", "unknown")
            detector = PIIDetector(**config)

            per_entity_metrics: Dict[str, Dict[str, int]] = {}
            total_tp, total_fp, total_fn = 0, 0, 0
            latencies = []

            for text, gt in zip(test_texts, ground_truth_labels):
                t0 = time.time()
                detected = detector.detect_pii(text)
                latencies.append(time.time() - t0)

                detected_by_type: Dict[str, set] = {}
                for e in detected:
                    detected_by_type.setdefault(e.entity_type, set()).add(e.text)

                for etype, gt_values in gt.items():
                    gt_set = set(gt_values)
                    det_set = detected_by_type.get(etype, set())
                    tp = len(gt_set & det_set)
                    fp = len(det_set - gt_set)
                    fn = len(gt_set - det_set)

                    if etype not in per_entity_metrics:
                        per_entity_metrics[etype] = {"TP": 0, "FP": 0, "FN": 0}
                    per_entity_metrics[etype]["TP"] += tp
                    per_entity_metrics[etype]["FP"] += fp
                    per_entity_metrics[etype]["FN"] += fn
                    total_tp += tp
                    total_fp += fp
                    total_fn += fn

            # Compute F1 per entity type
            entity_f1 = {}
            for etype, counts in per_entity_metrics.items():
                p = counts["TP"] / (counts["TP"] + counts["FP"]) if (counts["TP"] + counts["FP"]) > 0 else 0
                r = counts["TP"] / (counts["TP"] + counts["FN"]) if (counts["TP"] + counts["FN"]) > 0 else 0
                f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
                entity_f1[etype] = {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f1, 4)}

            # Macro and micro F1
            micro_p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
            micro_r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
            micro_f1 = 2 * micro_p * micro_r / (micro_p + micro_r) if (micro_p + micro_r) > 0 else 0

            macro_f1 = float(np.mean([m["f1"] for m in entity_f1.values()])) if entity_f1 else 0

            results[name] = {
                "config": config,
                "per_entity_f1": entity_f1,
                "micro_f1": round(micro_f1, 4),
                "macro_f1": round(macro_f1, 4),
                "micro_precision": round(micro_p, 4),
                "micro_recall": round(micro_r, 4),
                "total_TP": total_tp,
                "total_FP": total_fp,
                "total_FN": total_fn,
                "avg_latency_ms": round(float(np.mean(latencies)) * 1000, 2),
                "p95_latency_ms": round(float(np.percentile(latencies, 95)) * 1000, 2),
            }

            config["name"] = name  # restore

        out_path = self.output_dir / "pii_benchmark.json"
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        logger.info("PII benchmark saved → {}", out_path)
        return results

    # ==================================================================
    # 4. Multi-Run Experiment
    # ==================================================================

    def run_multi_experiment(
        self,
        experiment_fn,
        n_runs: int = 30,
        base_seed: int = 42,
    ) -> Dict[str, Any]:
        """Run an experiment multiple times with different seeds.

        Parameters
        ----------
        experiment_fn : callable
            Function(seed: int) → dict with at least "score" key.
        n_runs : int
            Number of repetitions.
        base_seed : int
            Seeds will be base_seed, base_seed+1, ..., base_seed+n_runs-1.

        Returns
        -------
        dict
            Mean, std, CI, per-run results.
        """
        per_run = []
        for i in range(n_runs):
            seed = base_seed + i
            result = experiment_fn(seed)
            result["seed"] = seed
            result["run_id"] = i + 1
            per_run.append(result)

        scores = [r["score"] for r in per_run]
        mean = float(np.mean(scores))
        std = float(np.std(scores, ddof=1))
        se = std / np.sqrt(n_runs)
        t_crit = sp_stats.t.ppf(0.975, df=n_runs - 1)

        return {
            "n_runs": n_runs,
            "mean_score": round(mean, 4),
            "std_score": round(std, 4),
            "ci_95_lower": round(mean - t_crit * se, 4),
            "ci_95_upper": round(mean + t_crit * se, 4),
            "min_score": round(min(scores), 4),
            "max_score": round(max(scores), 4),
            "per_run": per_run,
        }


# ============================================================================
# Ablation Study
# ============================================================================

class AblationStudy:
    """Systematic component removal to measure marginal contribution.

    For each AI component, runs the pipeline with and without it,
    measures the impact on DQ score, anomaly detection, and PII F1.
    """

    COMPONENTS = [
        "zscore", "iqr", "isolation_forest",
        "adaptive_threshold", "bayesian_threshold",
        "dimension_weights", "regression_weights",
        "early_warning", "cusum",
        "ner_model", "pii_drift", "pii_tuner",
    ]

    def __init__(self, output_dir: str = "data/evaluation"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_ablation(
        self,
        full_system_score: float,
        component_scores: Dict[str, float],
    ) -> pd.DataFrame:
        """Compute ablation metrics.

        Parameters
        ----------
        full_system_score : float
            DQ trust score with all components active.
        component_scores : dict[str, float]
            {component_name: score_without_this_component}
            Each entry is the system's score when that component
            is removed.

        Returns
        -------
        pd.DataFrame
            Ablation results showing marginal contribution of each component.
        """
        rows = []
        for comp, score_without in component_scores.items():
            delta = full_system_score - score_without
            pct_contribution = (delta / full_system_score * 100) if full_system_score > 0 else 0

            rows.append({
                "component": comp,
                "full_system_score": round(full_system_score, 2),
                "score_without": round(score_without, 2),
                "marginal_contribution": round(delta, 2),
                "pct_contribution": round(pct_contribution, 2),
                "critical": abs(delta) > 2.0,
            })

        df = pd.DataFrame(rows).sort_values("marginal_contribution", ascending=False)

        out_path = self.output_dir / "ablation_study.json"
        df.to_json(out_path, orient="records", indent=2)
        logger.info("Ablation study saved → {}", out_path)
        return df

    def generate_report(self, ablation_df: pd.DataFrame) -> str:
        """Generate a Markdown ablation report."""
        lines = [
            "# Ablation Study Results\n",
            "| Component | Full Score | Without | Δ | % Contribution | Critical |",
            "|-----------|-----------|---------|---|----------------|----------|",
        ]
        for _, row in ablation_df.iterrows():
            lines.append(
                f"| {row['component']} | {row['full_system_score']} | "
                f"{row['score_without']} | {row['marginal_contribution']:+.2f} | "
                f"{row['pct_contribution']:.1f}% | "
                f"{'⚠️' if row['critical'] else '✅'} |"
            )
        return "\n".join(lines)
