# Adaptive Data Governance Framework

> **Dissertation Project** — A Bayesian AI-driven, self-improving data governance framework for Indian e-commerce platforms, built with PySpark, Delta Lake, and Apache Airflow. Implements novel Bayesian adaptive thresholds, CUSUM change-point detection, Fellegi-Sunter probabilistic record linkage, and DPDP Act 2023 compliance enforcement.

---

## Table of Contents

- [Research Questions](#research-questions)
- [Overview](#overview)
- [Architecture](#architecture)
- [AI / ML Models](#ai--ml-models)
- [Pipeline Flow](#pipeline-flow)
- [Project Structure](#project-structure)
- [Evaluation Framework](#evaluation-framework)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Access Points](#access-points)
- [Pipeline Output](#pipeline-output)
- [Configuration](#configuration)
- [Key Modules](#key-modules)
- [References](#references)

---

## Research Questions

This dissertation investigates four inter-related research questions:

### RQ1: Bayesian Adaptive Thresholds vs Fixed/Frequentist Baselines
> *To what extent does a Bayesian conjugate-prior (Normal-Inverse-Gamma) adaptive threshold outperform fixed and frequentist (μ − kσ) baselines in detecting data quality degradation across evolving e-commerce data streams?*

**Hypothesis:** The Bayesian threshold adapts faster to distribution shifts (measured via CUSUM change-point detection) while maintaining lower false-positive rates than the frequentist baseline.

**Metrics:** F1 score, false-positive rate, mean time to threshold adaptation, posterior credible interval width.

### RQ2: PII Detection Accuracy — Hybrid Regex + NER
> *How does a hybrid PII detection approach (regex patterns + transformer-based NER) with adaptive F1-optimal thresholds compare against regex-only and NER-only baselines for Indian PII entity types (Aadhaar, PAN, phone, email)?*

**Hypothesis:** The hybrid approach achieves higher recall than regex-only and higher precision than NER-only, with adaptive thresholds improving F1 by ≥5% over static thresholds.

**Metrics:** Per-entity-type precision, recall, F1; detection latency; drift false-negative rates.

### RQ3: DPDP Act 2023 Compliance Enforcement
> *Can an automated compliance engine enforce key provisions of the Digital Personal Data Protection Act 2023 (Sections 6, 11, 12, 13, 16) within a medallion-architecture data pipeline without manual intervention?*

**Hypothesis:** Automated erasure, retention enforcement, consent state management, and cross-border validation achieve ≥95% compliance against a manual audit baseline.

**Metrics:** Erasure completeness (cascading across all layers), retention policy precision, consent state accuracy, audit trail queryability.

### RQ4: End-to-End Impact on Data Quality & ROI
> *What is the measurable impact of adaptive governance (Bayesian thresholds, anomaly detection, identity resolution, PII enforcement) on overall data quality scores, duplicate reduction, and calculated return on investment?*

**Hypothesis:** The adaptive framework improves DQ scores by ≥5 percentage points over a non-adaptive baseline and resolves ≥2% customer duplicates via Fellegi-Sunter probabilistic linkage.

**Metrics:** DQ score improvement, duplicates resolved, CLV accuracy, ROI multiplier.

---

## Overview

This framework implements a production-grade **Adaptive Data Governance** system for e-commerce data platforms. Unlike traditional rule-based governance, every threshold, weight, and detection boundary **learns and adapts** from historical pipeline runs using Bayesian inference and statistical process control.

### What Makes It Adaptive

| Capability | Traditional | This Framework |
|---|---|---|
| DQ Pass/Fail Threshold | Hard-coded (e.g. 85%) | **Bayesian NIG posterior credible interval** (Murphy, 2007) |
| Frequentist Baseline | Fixed μ − kσ | Dual-track: Bayesian primary + frequentist comparison |
| Change-Point Detection | None | **CUSUM** (Page, 1954) with configurable sensitivity |
| DQ Dimension Weights | Equal (20% each) | Bayesian posterior-variance + linear regression |
| Anomaly Detection | Single method | 3 methods: Z-score, IQR, Isolation Forest (Liu et al., 2008) |
| PII Detection | Regex only | Regex + DistilBERT NER (Devlin et al., 2019) |
| PII Confidence | Static threshold | F1-optimal per-entity-type tuning from feedback |
| PII Monitoring | None | Drift detection (FN rate baseline vs recent) |
| Identity Resolution | Rule-based dedup | **Fellegi-Sunter** (1969) probabilistic record linkage |
| Quality Trends | None | Bayesian surprise early warning system |
| Dimension Floor | None | Hard 60% minimum per dimension (no compensation attacks) |
| DPDP Compliance | Metadata flags | **Actual enforcement**: erasure, retention, consent, audit |

### Key Technologies

| Component | Technology |
|---|---|
| Processing Engine | Apache PySpark 3.5.0 |
| Storage Layer | Delta Lake 3.0.0 (ACID, time travel) |
| Orchestration | Apache Airflow 2.8.0 |
| Bayesian Inference | SciPy 1.11.4 (Normal-Inverse-Gamma conjugate priors) |
| ML / AI | scikit-learn 1.3.2, Hugging Face Transformers 4.36.2, PyTorch 2.1.2 |
| NER Model | dslim/bert-base-NER (DistilBERT fine-tuned for NER) |
| Statistical Testing | SciPy (paired t-test, Wilcoxon, Cohen's d, bootstrap CI) |
| Containerisation | Docker Compose (6 services) |
| Language | Python 3.10 |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG (12 Tasks)                        │
│                                                                         │
│  start → generate_data → ingest_bronze ─┬─ streaming_ingestion ──┐     │
│                                          │                         │     │
│                                          └─ bronze_to_silver       │     │
│                                                │                   │     │
│                                          quality_gate (AI Engine)  │     │
│                                                │                   │     │
│                                          silver_to_gold            │     │
│                                                │                   │     │
│                                          pii_scan_summary          │     │
│                                                │                   │     │
│                                          dpdp_compliance           │     │
│                                                │                   │     │
│                                          log_completion ◄──────────┘     │
│                                                │                         │
│                                               end                        │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                     Medallion Architecture (Delta Lake)                  │
│                                                                         │
│  Raw/Streaming  ──►  Bronze (Delta)  ──►  Silver (Delta)  ──►  Gold     │
│  - 500K orders       - Schema drift       - PII masking        - CLV   │
│  - 103K customers      detection          - Quarantine         - RFM   │
│  - 10K products      - Append-only        - DQ validation      - Churn │
│  - 200K reviews      - Metadata           - Deduplication      - Rev   │
│  - 20K clickstream                                             - Golden│
│                                                                  Rec.  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐          │
│  │  Quarantine  │    │ Data Contract│    │ Streaming Bronze │          │
│  │  (Failed DQ) │    │ (YAML SLAs)  │    │ (Clickstream)    │          │
│  └──────────────┘    └──────────────┘    └──────────────────┘          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                     Adaptive AI Engine (17 Models)                       │
│                                                                         │
│  ┌───────────────────┐  ┌───────────────────┐  ┌────────────────────┐  │
│  │ Anomaly Detection │  │ Bayesian Scoring  │  │ PII Intelligence   │  │
│  │ - Z-Score         │  │ - NIG posterior   │  │ - Regex (8 types)  │  │
│  │ - IQR Fences      │  │ - CUSUM (Page 54) │  │ - NER (DistilBERT) │  │
│  │ - Isolation Forest│  │ - Bayesian wts    │  │ - F1 tuner         │  │
│  │ - Batch anomaly   │  │ - Regression wts  │  │ - Drift detection  │  │
│  │                   │  │ - Early warning   │  │                    │  │
│  └───────────────────┘  └───────────────────┘  └────────────────────┘  │
│                                                                         │
│  ┌───────────────────┐  ┌───────────────────┐  ┌────────────────────┐  │
│  │ Identity Resoln.  │  │ DPDP Compliance   │  │ Governance Reports │  │
│  │ - Fellegi-Sunter  │  │ - Erasure (S.12)  │  │ - JSON timestamped │  │
│  │ - Jaro-Winkler    │  │ - Retention (S.11)│  │ - Full audit trail │  │
│  │ - Golden records  │  │ - Consent (S.6)   │  │ - Data contracts   │  │
│  │                   │  │ - Residency (S.16)│  │ - GE validation    │  │
│  └───────────────────┘  └───────────────────┘  └────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## AI / ML Models

Every model listed below is **actually invoked at runtime** during the pipeline execution:

### 1. Z-Score Anomaly Detection
- **File**: `src/quality/anomaly_detector.py` → `zscore_detect()`
- **Method**: Computes mean and stddev per numeric column; flags rows where |value − μ| > z_threshold × σ
- **Reference**: Grubbs (1969), "Procedures for Detecting Outlying Observations in Samples"
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 2a

### 2. IQR Fence Anomaly Detection
- **File**: `src/quality/anomaly_detector.py` → `iqr_detect()`
- **Method**: Computes Q1, Q3 via `approxQuantile`; flags rows outside [Q1 − 1.5×IQR, Q3 + 1.5×IQR]
- **Reference**: Tukey (1977), *Exploratory Data Analysis*
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 2b

### 3. Isolation Forest (sklearn)
- **File**: `src/quality/anomaly_detector.py` → `isolation_forest_detect()`
- **Method**: Trains `sklearn.ensemble.IsolationForest` on a 10% sample, scores all rows
- **Reference**: Liu, Ting & Zhou (2008), "Isolation Forest" (ICDM)
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 2c

### 4. Bayesian Adaptive DQ Threshold (NIG Posterior)
- **File**: `src/quality/bayesian_scorer.py` → `compute_adaptive_threshold()`
- **Method**: Normal-Inverse-Gamma conjugate prior; threshold = lower bound of 95% posterior predictive t-distribution credible interval
- **Formulation**: κₙ = κ₀ + N, μₙ = (κ₀·μ₀ + Σxᵢ)/κₙ, αₙ = α₀ + N/2, βₙ = β₀ + 0.5·SS + correction
- **Reference**: Murphy (2007), *Conjugate Bayesian Analysis of the Gaussian Distribution*
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 3 (primary threshold)

### 5. Frequentist Adaptive Threshold (μ − kσ)
- **File**: `src/quality/adaptive_scorer.py` → `compute_adaptive_threshold()`
- **Method**: Rolling mean − k×std over last N runs; clamped to [70%, 99%]
- **Reference**: Shewhart (1931), *Economic Control of Quality of Manufactured Product*
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 3 (comparison baseline)

### 6. CUSUM Change-Point Detection
- **File**: `src/quality/bayesian_scorer.py` → `cusum_detect()`
- **Method**: Cumulative Sum control chart; detects upward/downward shifts in DQ score mean
- **Formulation**: S⁺ₙ = max(0, S⁺ₙ₋₁ + (xₙ − μ₀ − k)), S⁻ₙ = max(0, S⁻ₙ₋₁ − (xₙ − μ₀ + k))
- **Reference**: Page (1954), "Continuous Inspection Schemes" (Biometrika)
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 3b

### 7. Bayesian Dimension Weight Learning
- **File**: `src/quality/bayesian_scorer.py` → `learn_dimension_weights()`
- **Method**: Posterior variance-based weighting; higher-variance dimensions get higher attention
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 1

### 8. Linear Regression Weight Learning
- **File**: `src/quality/adaptive_scorer.py` → `learn_weights_regression()`
- **Method**: Fits sklearn LinearRegression to predict overall score from dimension scores
- **Reference**: Hastie, Tibshirani & Friedman (2009), *Elements of Statistical Learning*
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 9

### 9. Early Warning System (Bayesian Surprise)
- **File**: `src/quality/bayesian_scorer.py` → `check_early_warning()`
- **Method**: Bayesian surprise (score deviation from posterior predictive) + CUSUM + 3-run trend
- **Reference**: Itti & Baldi (2009), "Bayesian Surprise Attracts Human Attention"
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 4

### 10. Batch Anomaly Detection
- **File**: `src/quality/anomaly_detector.py` → `detect_batch_anomaly()`
- **Method**: Cross-run Z-score comparison using persisted DQ history
- **Invoked by**: `AdaptiveGovernanceEngine.evaluate()` → Step 5

### 11. PII Detection — Regex (8 Patterns)
- **File**: `src/pii_detection/pii_detector.py` → `detect_pii()`
- **Patterns**: EMAIL, PHONE_NUMBER, AADHAAR, PAN, CREDIT_CARD, IPV4, ADDRESS, IFSC
- **Invoked by**: `_pii_scan_summary()` task + `BronzeToSilverTransformer`

### 12. PII Detection — NER (DistilBERT)
- **File**: `src/pii_detection/pii_detector.py` → NER pipeline
- **Model**: `dslim/bert-base-NER` (Hugging Face) with chunked processing (450-char windows)
- **Reference**: Devlin et al. (2019), "BERT: Pre-training of Deep Bidirectional Transformers"
- **Invoked by**: `_pii_scan_summary()` task with `use_ner_model=True`

### 13. PII Confidence Tuner
- **File**: `src/pii_detection/adaptive_pii_tuner.py` → `tune_thresholds()`
- **Method**: Grid search over 50 candidate thresholds to maximise F1 per entity type
- **Invoked by**: `_pii_scan_summary()` + `AdaptiveGovernanceEngine.evaluate()` → Step 8

### 14. PII Drift Detection
- **File**: `src/pii_detection/adaptive_pii_tuner.py` → `detect_pii_drift()`
- **Method**: Compares false-negative rates between baseline (75%) and recent (25%) feedback
- **Invoked by**: `_pii_scan_summary()` + `AdaptiveGovernanceEngine.evaluate()` → Step 7

### 15. Identity Resolution — Fellegi-Sunter Probabilistic Linkage
- **File**: `src/governance/identity_resolution.py` → `fuzzy_match_link()`
- **Method**: Log-likelihood ratio scoring with m/u probabilities; w_agree = log₂(m/u), w_disagree = log₂((1−m)/(1−u))
- **Reference**: Fellegi & Sunter (1969), "A Theory for Record Linkage" (JASA)
- **Invoked by**: `_silver_to_gold()` task → `IdentityResolver`

### 16. Great Expectations Validation
- **File**: `src/quality/dq_framework.py`
- **Method**: 8-rule ExpectationSuite with automated quarantine of failures
- **Reference**: Great Expectations open-source project
- **Invoked by**: `_data_quality_gate()` task

### 17. DPDP Compliance Engine
- **File**: `src/governance/dpdp_compliance.py`
- **Method**: Cascading erasure (Section 12), retention enforcement (Section 11), consent state machine (Section 6), cross-border validation (Section 16), queryable audit trail (Section 13)
- **Reference**: Government of India (2023), *The Digital Personal Data Protection Act, 2023*
- **Invoked by**: `_dpdp_compliance()` task

---

## Pipeline Flow

The `medallion_pipeline_dag` runs **12 tasks** in the following order:

| # | Task | What It Does |
|---|---|---|
| 1 | `start` | Pipeline entry point |
| 2 | `generate_synthetic_data` | Generates 500K orders, 103K customers, 10K products, 200K reviews, 500K order items with real-world scenarios (fraud, festival spikes, PII leakage, duplicates) |
| 3 | `ingest_to_bronze` | Reads raw Parquet → writes to Bronze Delta Lake (5 tables) |
| 4 | `streaming_ingestion` | Produces 10 micro-batches × 2,000 clickstream events with 5% PII injection; consumes via Structured Streaming |
| 5 | `bronze_to_silver` | PII masking (hash/redact), quarantine invalid records, add Silver metadata |
| 6 | `data_quality_check` | **Adaptive AI Engine** — runs 17 AI models: Bayesian NIG threshold, CUSUM, 3 anomaly detectors, dimension weight learning, early warning, batch anomaly, PII drift, PII tuning |
| 7 | `silver_to_gold` | Revenue aggregates, RFM segmentation, CLV scoring, churn features, Identity Resolution (Fellegi-Sunter probabilistic dedup → golden records) |
| 8 | `pii_scan_summary` | Scans Silver with Regex + NER, records PII feedback, auto-tunes thresholds, checks drift |
| 9 | `dpdp_compliance` | DPDP Act 2023 enforcement: retention check, data residency validation, compliance report |
| 10 | `log_completion` | Prints full pipeline summary with row counts, all governance metrics, and AI model execution log |
| 11 | `end` | Pipeline exit |

---

## Project Structure

```
adaptive-governance-framework/
├── README.md                              # This file
├── docker-compose.yml                     # 6 services: Spark, Airflow, Jupyter, PostgreSQL
├── Dockerfile.jupyter                     # JupyterLab with PySpark
├── requirements.txt                       # Host Python dependencies
├── requirements.airflow.txt               # Airflow container dependencies
├── requirements.jupyter.txt               # Jupyter container dependencies
│
├── airflow/dags/
│   └── medallion_pipeline_dag.py          # Main 12-task DAG
│
├── config/
│   ├── config.yaml                        # Central configuration
│   └── data_contracts/                    # YAML data contract definitions
│       └── ecommerce_orders_v2.0.0.yaml
│
├── scripts/
│   └── deploy.sh                          # One-command Docker deployment
│
├── src/
│   ├── governance/
│   │   ├── adaptive_governance_engine.py  # Central AI orchestrator (17 models)
│   │   ├── identity_resolution.py         # Fellegi-Sunter probabilistic linkage + golden records
│   │   ├── dpdp_compliance.py             # DPDP Act 2023 enforcement engine
│   │   └── data_contracts.py              # YAML data contracts + SLA enforcement
│   │
│   ├── quality/
│   │   ├── bayesian_scorer.py             # NIG conjugate prior + CUSUM change-point
│   │   ├── anomaly_detector.py            # Z-score, IQR, Isolation Forest
│   │   ├── adaptive_scorer.py             # Frequentist thresholds + weight learning
│   │   ├── quality_metrics.py             # 5 DQ dimensions
│   │   └── dq_framework.py               # Great Expectations integration
│   │
│   ├── pii_detection/
│   │   ├── pii_detector.py                # Regex + NER PII detection
│   │   ├── pii_masker.py                  # Hash / redact / tokenize masking
│   │   └── adaptive_pii_tuner.py          # F1-optimal threshold tuning + drift
│   │
│   ├── evaluation/
│   │   ├── __init__.py                    # EvaluationFramework + AblationStudy
│   │   └── evaluation_framework.py        # Re-export module
│   │
│   ├── transformation/
│   │   ├── bronze_to_silver.py            # PII masking + quarantine + metadata
│   │   └── silver_to_gold.py              # CLV, RFM, churn, revenue aggregations
│   │
│   ├── ingestion/
│   │   ├── data_loader.py                 # Raw → Bronze with schema drift detection
│   │   ├── data_generator.py              # Large-scale synthetic Indian e-commerce data
│   │   └── streaming_simulator.py         # Micro-batch producer + Structured Streaming
│   │
│   └── utils/
│       ├── spark_utils.py                 # SparkSession builder with Delta Lake
│       └── schemas.py                     # Shared PySpark schemas
│
├── data/                                  # Generated at runtime (gitignored)
│   ├── raw/                               # Source Parquet files
│   ├── bronze/                            # Bronze Delta tables
│   ├── silver/                            # Silver Delta tables
│   ├── gold/                              # Gold Delta tables
│   ├── quarantine/                        # Failed DQ records
│   └── streaming/                         # Streaming landing zone
│
├── docs/                                  # Documentation
│   ├── deployment_guide.md                # Step-by-step deployment
│   ├── architecture.md                    # Architecture decisions
│   └── dpdp_compliance.md                 # DPDP Act 2023 compliance
│
├── tests/                                 # Unit + integration tests
├── notebooks/                             # Jupyter exploration (4 notebooks)
└── models/                                # Trained ML models
```

---

## Evaluation Framework

The framework includes a rigorous evaluation suite for dissertation-grade research validation:

### Baseline Comparisons (`src/evaluation/`)
- **Fixed threshold** (85%) vs **Frequentist** (μ − kσ) vs **Bayesian** (NIG posterior)
- Per-strategy: precision, recall, F1, false-positive rate
- Paired comparison with statistical significance

### Statistical Significance Testing
- **Paired t-test** (parametric) with Bonferroni correction
- **Wilcoxon signed-rank** (non-parametric) for robustness
- **Cohen's d** effect size (small/medium/large classification)
- **Bootstrap 95% confidence intervals** (10,000 resamples)

### Ablation Study
Systematic removal of each component to measure marginal contribution:
- Bayesian vs frequentist threshold → ΔF1
- CUSUM removal → Δ detection latency
- NER removal → Δ PII recall
- Isolation Forest removal → Δ anomaly detection
- Identity Resolution removal → Δ duplicate rate

### PII Benchmarking
- Per-entity-type F1 (Aadhaar, PAN, email, phone, credit card, etc.)
- Regex-only vs NER-only vs Hybrid comparison
- Detection latency profiling
- Adaptive vs static threshold impact

### Multi-Run Experiments
- N runs with different random seeds for reproducibility
- Mean ± 95% CI for all metrics
- Seed-controlled `numpy`, `sklearn`, `torch` for determinism

---

## Prerequisites

| Requirement | Minimum |
|---|---|
| macOS / Linux | macOS 12+ or Ubuntu 20.04+ |
| RAM | 16 GB (Docker needs 12 GB allocated) |
| Disk | 50 GB free |
| Docker Desktop | 4.25+ with Compose V2 |
| Docker Memory | 12 GB minimum (Settings → Resources) |

> **Note**: Python, Java, Spark are all containerised — no local installation required.

---

## Quick Start

### 1. Clone

```bash
git clone https://github.com/KartikayRaniwala/adaptive-governance-framework.git
cd adaptive-governance-framework
```

### 2. Configure Docker Resources

Open **Docker Desktop → Settings → Resources**:
- CPUs: 6+
- Memory: **12 GB minimum** (16 GB recommended)
- Swap: 4 GB

### 3. Deploy (One Command)

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

This will:
1. Build all Docker images (Spark, Airflow, JupyterLab)
2. Start 6 services (PostgreSQL, Spark Master/Worker, Airflow, JupyterLab)
3. Wait for health checks
4. Clear stale data and caches
5. **Automatically trigger the pipeline**

### 4. Monitor

Open Airflow at [http://localhost:8081](http://localhost:8081) (admin / admin) and watch all 12 tasks turn green.

**Expected runtime: ~5–8 minutes** (first run downloads NER model; subsequent runs ~3–5 min).

### 5. Stop

```bash
docker compose down        # Stop containers
docker compose down -v     # Stop + remove volumes
```

---

## Access Points

| Service | URL | Credentials |
|---|---|---|
| **Airflow Web UI** | [http://localhost:8081](http://localhost:8081) | admin / admin |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | — |
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | token: `governance` |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

---

## Pipeline Output

After a successful run, the following data is produced:

### Data Volumes

| Layer | Table | Approximate Rows |
|---|---|---|
| Bronze | orders, customers, products, reviews, order_items | 500K, 103K, 10K, 200K, 500K |
| Silver | orders, customers, reviews | ~490K, 103K, 200K |
| Gold | revenue_aggregates, customer_rfm, customer_clv, churn_features, golden_customers | ~13K, 102K, 102K, 100K, ~100K |
| Quarantine | orders | ~10K |
| Streaming | clickstream | 20K |

### Governance Metrics

| Metric | Typical Value |
|---|---|
| DQ Score | ~92–93% |
| Bayesian Threshold | ~85% (NIG posterior credible interval) |
| Frequentist Threshold | ~85% (μ − kσ) |
| CUSUM Shift | none (stable) |
| Decision | PASS |
| Z-Score Anomalies | ~2,500 (0.5%) |
| IQR Anomalies | ~25,000 (5%) |
| Isolation Forest Anomalies | ~24,500 (5%) |
| Identity Resolution | 103K → ~100K (2,600+ duplicates resolved via Fellegi-Sunter) |
| PII Post-Masking | 0 remaining |
| Contract Enforcement | ~435K valid, ~55K quarantined |
| DPDP Compliance | Active (retention, residency, audit trail) |

### Reports

Governance reports are saved as timestamped JSON files in:
```
data/metrics/governance_reports/silver_orders_YYYYMMDD_HHMMSS.json
```

Each report contains the full evaluation output from all 17 AI models, including Bayesian threshold, CUSUM result, and dimension floor status.

---

## Configuration

Central configuration is in `config/config.yaml`. Key sections:

| Section | Purpose |
|---|---|
| `spark` | Spark session settings |
| `storage` | Medallion layer paths |
| `data_quality` | DQ thresholds, quarantine settings |
| `pii_detection` | NER model, confidence threshold, entity types |
| `data_contracts` | Contract directory, SLA enforcement |
| `identity_resolution` | Match thresholds, Fellegi-Sunter m/u parameters |
| `streaming` | Landing directory, trigger interval |
| `dpdp` | Retention policies, consent configuration |

---

## Key Modules

### Adaptive Governance Engine (`src/governance/adaptive_governance_engine.py`)
The central "brain" that orchestrates all AI components. A single `evaluate()` call runs 10 steps:
1. Compute DQ metrics with Bayesian dimension weights
2. Z-score + IQR + Isolation Forest anomaly detection
3. Compute Bayesian adaptive threshold (NIG posterior) + frequentist comparison
3b. CUSUM change-point detection
4. Bayesian surprise early warning analysis
5. Batch-level anomaly detection
6. Record run in both scorers for future learning
7. PII drift check
8. PII threshold re-tuning
9. Regression-based weight learning
10. Final pass/fail/warn decision (with 60% dimension floor)

### Bayesian DQ Scorer (`src/quality/bayesian_scorer.py`)
Novel Bayesian conjugate-prior scoring engine:
- **Normal-Inverse-Gamma (NIG)** posterior for DQ threshold adaptation
- **CUSUM** change-point detection for distribution shift alerting
- Posterior-variance dimension weighting (higher uncertainty → higher attention)
- Bayesian surprise for early warning (deviation from posterior predictive)

### Anomaly Detector (`src/quality/anomaly_detector.py`)
Three complementary detection methods:
- **Z-Score**: Parametric, assumes normal distribution
- **IQR Fences**: Non-parametric, robust to skew
- **Isolation Forest**: ML-based, catches multi-dimensional anomalies

### PII Detector (`src/pii_detection/pii_detector.py`)
Dual-mode PII detection:
- **Regex**: 8 Indian PII patterns (Aadhaar, PAN, phone, email, credit card, IPv4, address, IFSC)
- **NER**: Hugging Face `dslim/bert-base-NER` with chunked processing (450-char windows)

### Adaptive PII Tuner (`src/pii_detection/adaptive_pii_tuner.py`)
Feedback-driven PII threshold optimisation:
- Records detection feedback (TP/FP/FN/TN)
- Grid search over 50 candidate thresholds per entity type
- Maximises F1 score
- Detects PII-type drift via FN rate monitoring

### Identity Resolution (`src/governance/identity_resolution.py`)
Customer deduplication with Fellegi-Sunter probabilistic linkage:
- Log-likelihood ratio scoring with m/u probabilities
- Jaro-Winkler fuzzy matching for string similarity
- Blocking keys for scalable candidate pair generation
- Golden record creation (most recent canonical profile)

### DPDP Compliance Engine (`src/governance/dpdp_compliance.py`)
Full enforcement of the Digital Personal Data Protection Act 2023:
- **Section 6**: Consent state machine (PENDING → GRANTED → WITHDRAWN)
- **Section 11**: Automated retention enforcement with policy-based deletion
- **Section 12**: Cascading erasure across Bronze/Silver/Gold/Quarantine + VACUUM
- **Section 13**: Queryable audit trail for grievance redressal
- **Section 16**: Cross-border data transfer validation

### Evaluation Framework (`src/evaluation/`)
Dissertation-grade evaluation with:
- Baseline comparison (fixed vs frequentist vs Bayesian)
- Statistical significance tests (paired t-test, Wilcoxon, Cohen's d)
- Ablation study (marginal contribution of each component)
- PII benchmarking (per-entity F1)
- Multi-run experiments with seed control and confidence intervals

---

## References

### Foundational Frameworks & Literature

1. **DAMA International** (2017). *DAMA-DMBOK: Data Management Body of Knowledge* (2nd ed.). Technics Publications.
2. **Dehghani, Z.** (2022). *Data Mesh: Delivering Data-Driven Value at Scale*. O'Reilly Media.
3. **Chambers, B. & Zaharia, M.** (2018). *Spark: The Definitive Guide — Big Data Processing Made Simple*. O'Reilly Media.
4. **Harvard Business Review** (2023). "Why Your Data Governance Should Be Adaptive, Not Rigid." *HBR Digital Articles*.
5. **Redman, T.C.** (2001). *Data Quality: The Field Guide*. Digital Press.
6. **Wang, R.Y. & Strong, D.M.** (1996). "Beyond Accuracy: What Data Quality Means to Data Consumers." *Journal of Management Information Systems*, 12(4), 5–33.

### Bayesian & Statistical Methods

7. **Murphy, K.P.** (2007). "Conjugate Bayesian Analysis of the Gaussian Distribution." *Technical Report*, University of British Columbia.
8. **Murphy, K.P.** (2012). *Machine Learning: A Probabilistic Perspective*. MIT Press.
9. **Page, E.S.** (1954). "Continuous Inspection Schemes." *Biometrika*, 41(1/2), 100–115.
10. **Adams, R.P. & MacKay, D.J.C.** (2007). "Bayesian Online Changepoint Detection." *arXiv preprint arXiv:0710.3742*.
11. **Shewhart, W.A.** (1931). *Economic Control of Quality of Manufactured Product*. Van Nostrand.
12. **Itti, L. & Baldi, P.** (2009). "Bayesian Surprise Attracts Human Attention." *Vision Research*, 49(10), 1295–1306.
13. **Hastie, T., Tibshirani, R. & Friedman, J.** (2009). *The Elements of Statistical Learning* (2nd ed.). Springer.

### Anomaly Detection

14. **Liu, F.T., Ting, K.M. & Zhou, Z.H.** (2008). "Isolation Forest." *Proceedings of the Eighth IEEE International Conference on Data Mining (ICDM)*, 413–422.
15. **Grubbs, F.E.** (1969). "Procedures for Detecting Outlying Observations in Samples." *Technometrics*, 11(1), 1–21.
16. **Tukey, J.W.** (1977). *Exploratory Data Analysis*. Addison-Wesley.

### NLP & PII Detection

17. **Devlin, J., Chang, M.W., Lee, K. & Toutanova, K.** (2019). "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding." *Proceedings of NAACL-HLT 2019*, 4171–4186.
18. **Lample, G., Ballesteros, M., Subramanian, S., Kawakami, K. & Dyer, C.** (2016). "Neural Architectures for Named Entity Recognition." *Proceedings of NAACL-HLT 2016*.
19. **Li, J., Sun, A., Han, J. & Li, C.** (2020). "A Survey on Deep Learning for Named Entity Recognition." *IEEE TKDE*, 34(1), 50–70.

### Record Linkage & Identity Resolution

20. **Fellegi, I.P. & Sunter, A.B.** (1969). "A Theory for Record Linkage." *Journal of the American Statistical Association*, 64(328), 1183–1210.
21. **Jaro, M.A.** (1989). "Advances in Record-Linkage Methodology as Applied to Matching the 1985 Census of Tampa, Florida." *JASA*, 84(406), 414–421.
22. **Winkler, W.E.** (1990). "String Comparator Metrics and Enhanced Decision Rules in the Fellegi-Sunter Model of Record Linkage." *Proceedings of the Section on Survey Research Methods*, APA, 354–359.
23. **Christen, P.** (2012). *Data Matching: Concepts and Techniques for Record Linkage, Entity Resolution, and Duplicate Detection*. Springer.

### Regulatory & Industry Reports

24. **Government of India** (2023). *The Digital Personal Data Protection Act, 2023* (DPDP Act). Ministry of Electronics and Information Technology.
25. **European Parliament** (2016). *General Data Protection Regulation (GDPR)*. Regulation (EU) 2016/679.
26. **Gartner** (2024). *Magic Quadrant for Augmented Data Quality Solutions*. Gartner Research.
27. **McKinsey & Company** (2020). "The Data-Driven Enterprise of 2025." *McKinsey Analytics*.

### Technical Documentation & Best Practices

28. **Apache Software Foundation**. *Apache Spark Documentation*. https://spark.apache.org/docs/latest/
29. **Delta Lake Project**. *Delta Lake Documentation*. https://docs.delta.io/latest/
30. **Google Cloud**. *Whitepapers: Best Practices for Retail Analytics*. https://cloud.google.com/whitepapers
31. **Great Expectations**. *Great Expectations Documentation*. https://docs.greatexpectations.io/
32. **Hugging Face**. *Transformers Documentation*. https://huggingface.co/docs/transformers/

### Data Quality & Governance Standards

33. **ISO 8000-61:2016**. *Data Quality — Part 61: Data Quality Management: Process Reference Model*. International Organization for Standardization.
34. **ISO/IEC 25012:2008**. *Software Engineering — Software Product Quality Requirements and Evaluation (SQuaRE) — Data Quality Model*. ISO/IEC.
35. **Pipino, L.L., Lee, Y.W. & Wang, R.Y.** (2002). "Data Quality Assessment." *Communications of the ACM*, 45(4), 211–218.
36. **Batini, C., Cappiello, C., Francalanci, C. & Maurino, A.** (2009). "Methodologies for Data Quality Assessment and Improvement." *ACM Computing Surveys*, 41(3), 1–52.

---

## License

© 2026 Kartikay Raniwala & Shreenam Tiwari. All rights reserved.
This project is submitted as part of a dissertation and may not be reproduced without permission.
