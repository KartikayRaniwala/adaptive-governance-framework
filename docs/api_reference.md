# API Reference
## Adaptive Data Governance Framework — Module Documentation

---

## Table of Contents

- [src.utils](#srcutils)
- [src.ingestion](#srcingestion)
- [src.pii_detection](#srcpii_detection)
- [src.transformation](#srctransformation)
- [src.quality](#srcquality)
- [src.governance](#srcgovernance)
- [Adaptive AI Components](#adaptive-ai-components)

---

## `src.utils`

### `spark_utils`

| Function | Description |
|----------|-------------|
| `get_spark_session(app_name, master, config_overrides)` | Create or retrieve a SparkSession pre-configured with Delta Lake, AQE, and optimised shuffle/partition settings. |
| `stop_spark()` | Gracefully stop the active SparkSession. |

### `config_loader`

| Function / Class | Description |
|-------------------|-------------|
| `load_config(path)` | Load and validate `config.yaml` into a `FrameworkConfig` Pydantic model. |
| `load_raw_config(path)` | Load raw YAML as a plain dict without validation. |
| `FrameworkConfig` | Top-level Pydantic model with nested `SparkConfig`, `StorageConfig`, `DataQualityConfig`, `PIIDetectionConfig`, `ComplianceConfig`, `MedallionConfig`. |

### `logger`

| Function | Description |
|----------|-------------|
| `setup_logger(level, json_logs, log_file)` | Initialise Loguru logger with console + optional file sink. |
| `set_correlation_id(cid)` | Set a correlation ID that will be injected into every log record. |
| `get_correlation_id()` | Retrieve the current correlation ID. |

### `data_generator`

| Function | Description |
|----------|-------------|
| `generate_customers(n)` | Pandas DataFrame with Indian customer profiles (name, email, phone, address, Aadhaar, PAN). |
| `generate_products(n)` | Products with category, price, seller, weight, dimensions. |
| `generate_orders(n)` | Orders with statuses, timestamps, amounts, delivery instructions with injected PII. |
| `generate_reviews(n)` | Customer reviews with embedded PII in review text. |
| `generate_order_items(n)` | Line-level order items with price, freight, quantity. |
| `generate_all(output_dir, ...)` | Generate all datasets and write to CSV / Parquet. |

### `roi_calculator`

| Class | Description |
|-------|-------------|
| `GovernanceROICalculator` | Calculate financial ROI of the governance framework. |

**Key methods:**

| Method | Returns |
|--------|---------|
| `calculate_rto_savings()` | Monthly + annual savings from reduced Return-to-Origin rates. |
| `calculate_personalization_uplift(before, after)` | Revenue uplift from better conversion. |
| `calculate_compliance_cost_avoidance()` | Penalty + churn cost avoidance under DPDP Act. |
| `calculate_operational_efficiency(hourly_rate)` | Savings from automated governance. |
| `generate_roi_report()` | Pandas DataFrame with 3-year ROI summary. |

---

## `src.ingestion`

### `data_loader`

| Class | Description |
|-------|-------------|
| `DataLoader(spark, base_data_path)` | Multi-format data ingestion with Bronze layer writes. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `load_from_csv(path, schema, delimiter)` | Read CSV file into Spark DataFrame. |
| `load_from_parquet(path)` | Read Parquet file. |
| `load_from_json(path, schema)` | Read JSON file. |
| `load_incremental(path, watermark_column, last_watermark)` | Incremental load with watermark filter. |
| `detect_schema_drift(incoming, existing)` | Compare schemas and return added / removed / type-changed columns. |
| `write_to_bronze(df, table_name, partition_by)` | Write DataFrame to Bronze Delta table with metadata columns. |

### `schema_registry`

| Class | Description |
|-------|-------------|
| `SchemaRegistry(registry_dir)` | Register, version, and retrieve PySpark StructType schemas stored as JSON. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `register_schema(name, schema)` | Save a new version of a named schema. |
| `get_schema(name, version)` | Retrieve a specific version (default: latest). |
| `get_latest_version(name)` | Get the highest registered version number. |
| `compare_schemas(name, incoming)` | Drift report between incoming and latest registered schema. |

---

## `src.pii_detection`

### `pii_detector`

| Class | Description |
|-------|-------------|
| `PIIDetector(use_ner, model_path, confidence_threshold)` | Detect PII using regex patterns and optional DistilBERT NER. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `detect_pii(text)` | Return list of `{"entity_type", "value", "start", "end", "confidence"}` dicts. |
| `has_pii(text)` | Boolean check. |
| `detect_pii_types(text)` | Return set of entity-type strings found. |
| `create_spark_detect_udf()` | Factory for a Spark UDF that returns detected PII as JSON string. |

### `pii_masker`

| Class | Description |
|-------|-------------|
| `PIIMasker(strategy, salt)` | Mask PII in text using hash / redact / tokenize. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `mask_text(text, entities)` | Return masked text given entity list from `PIIDetector`. |
| `mask_text_with_report(text, entities)` | Return `(masked_text, report_dict)` with counts by entity type. |
| `create_spark_mask_udf()` | Factory for a Spark UDF. |

### `train_ner_model`

| Function | Description |
|----------|-------------|
| `train_ner_model(output_dir, epochs, batch_size, lr)` | Fine-tune DistilBERT for NER on synthetic Indian PII data and save model + tokenizer + label mapping. |

---

## `src.transformation`

### `bronze_to_silver`

| Class | Description |
|-------|-------------|
| `BronzeToSilverTransformer(spark, bronze_path, silver_path, quarantine_path)` | Clean, validate, deduplicate, and PII-mask Bronze data. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `deduplicate(df, key_columns, order_column)` | Remove duplicates via window function. |
| `validate_not_null(df, columns)` | Split into valid / quarantine DataFrames. |
| `validate_value_ranges(df, rules)` | Validate min/max ranges. |
| `mask_pii_columns(df, columns, strategy)` | Apply PII masking UDF to columns. |
| `transform_orders(table_name, ...)` | End-to-end pipeline for an orders table. |
| `write_to_silver(df, table_name)` | Write to Silver Delta table. |
| `write_quarantine(df, table_name, reason)` | Write rejected rows with reason metadata. |

### `silver_to_gold`

| Class | Description |
|-------|-------------|
| `SilverToGoldTransformer(spark, silver_path, gold_path)` | Aggregate and enrich Silver data for Gold layer. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `aggregate_revenue(df, granularities)` | Revenue by date granularity (daily / weekly / monthly). |
| `compute_rfm(df, reference_date)` | RFM segmentation with quintile scores. |
| `compute_clv(df, months)` | Customer Lifetime Value. |
| `compute_churn_features(df, inactive_days)` | Churn labels + feature columns. |
| `transform_all(table_name)` | End-to-end Gold pipeline. |

### `scd_manager`

| Class | Description |
|-------|-------------|
| `SCDType2Manager(spark, table_path, key_columns, tracked_columns)` | SCD Type 2 via Delta Lake MERGE with hash-based change detection. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `initialise(df)` | Create the initial dimension table snapshot. |
| `apply_changes(updates_df)` | Merge updates: expire changed rows, insert new versions. |

---

## `src.quality`

### `data_quality_engine`

| Class | Description |
|-------|-------------|
| `DataQualityEngine(spark, config_path)` | Great Expectations wrapper for Spark DataFrames. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `create_expectation_suite(suite_name, table_name, schema, business_rules)` | Build GX suite from schema dict. |
| `validate_dataframe(df, suite_name, quarantine_on_failure)` | Validate and optionally quarantine. Returns `(valid_df, quarantine_df, results)`. |
| `profile_dataset(df, output_path)` | Statistical profiling saved as JSON. |

### `quality_metrics`

| Class | Description |
|-------|-------------|
| `QualityMetrics(spark)` | Calculate six DQ dimensions and persist scores. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `compute_completeness(df, columns)` | % non-null. |
| `compute_uniqueness(df, columns)` | % distinct. |
| `compute_validity(df, column, valid_values/regex)` | % passing format checks. |
| `compute_timeliness(df, column, max_age_hours)` | % records within freshness SLA. |
| `compute_consistency(df, col_a, col_b, relation)` | Cross-column logical validation. |
| `compute_accuracy(df, column, ref_df, ref_column)` | Referential integrity proxy. |
| `compute_overall_score(scores, weights)` | Weighted average DQ score. |
| `save_metrics(scores, table_name, path)` | Persist as JSON + Delta table. |

### `anomaly_detector`

| Class | Description |
|-------|-------------|
| `AnomalyDetector(spark, z_threshold, iqr_factor, contamination, history_path)` | Statistical and ML-based anomaly detection on PySpark DataFrames. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `zscore_detect(df, numeric_columns)` | Flag rows where numeric values exceed ±z standard deviations. Returns `(flagged_df, report)`. |
| `iqr_detect(df, numeric_columns)` | Flag rows outside IQR fences [Q1 − k·IQR, Q3 + k·IQR]. Returns `(flagged_df, report)`. |
| `isolation_forest_detect(df, feature_columns, sample_fraction)` | Train scikit-learn Isolation Forest on a sample and score all rows. Returns `(scored_df, report)`. |
| `detect_batch_anomaly(current_metrics, label)` | Compare current batch DQ score against historical rolling Z-score to flag statistical outlier runs. |

### `adaptive_scorer`

| Class | Description |
|-------|-------------|
| `AdaptiveDQScorer(history_dir, baseline_window, sensitivity, min_threshold, max_threshold)` | Self-tuning DQ scorer that learns thresholds and weights from pipeline history. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `compute_adaptive_threshold(label)` | Dynamic pass/fail threshold = max(min, rolling_mean − sensitivity·rolling_std). |
| `learn_dimension_weights(label)` | Inverse-mean weighting: problematic dimensions get higher weight. |
| `learn_weights_regression(label, target_score)` | Linear regression to learn optimal dimension weights from historical correlations. |
| `check_early_warning(current_score, label)` | Detect declining trends even if score is above threshold. Returns alert level (none/info/warning/critical). |
| `record_run(metrics, label)` | Append current run to the adaptive history for future learning. |

---

## `src.governance`

### `data_contracts`

| Class | Description |
|-------|-------------|
| `DataContract(name, version, schema, ...)` | Declarative YAML-based data contract with SLA constraints. |
| `ContractRegistry(contracts_dir)` | Load, register, and look up data contracts from a YAML directory. |
| `ContractEnforcer(spark, registry, quarantine_path)` | Enforce contracts on DataFrames, quarantining violations. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `enforce(df, contract_name)` | Returns `(valid_df, quarantine_df, report)`. |

### `identity_resolution`

| Class | Description |
|-------|-------------|
| `IdentityResolver(spark, match_threshold)` | MDM entity resolution with fuzzy and exact matching. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `exact_match_dedup(df, match_columns, id_column)` | Deterministic dedup on exact column values; assigns `_golden_id`. |
| `fuzzy_match_link(df, name_col_first, name_col_last, ...)` | Jaro-Winkler + Soundex probabilistic record linkage. |
| `create_golden_records(resolved_df, id_column, recency_col)` | Pick canonical record per cluster using recency and consent merging. |

### `data_mesh`

| Class | Description |
|-------|-------------|
| `DataProduct(name, domain, owner, ...)` | Represents a data product within a domain mesh. |
| `DataMeshGovernor(spark, config_path)` | Federated domain management and cross-domain policy enforcement. |

### `adaptive_governance_engine`

| Class | Description |
|-------|-------------|
| `AdaptiveGovernanceEngine(spark, data_root, config)` | Central AI orchestrator that ties anomaly detection, adaptive scoring, PII tuning, and feedback loops into a single evaluation. |

**Key methods:**

| Method | Description |
|--------|-------------|
| `evaluate(df, label, required_columns, validity_rules, numeric_columns)` | Full adaptive evaluation: DQ metrics → anomaly detection → adaptive threshold → learned weights → early warning → batch anomaly → PASS/WARN/FAIL decision. |
| `get_adaptive_pii_thresholds()` | Return tuned per-entity-type PII confidence thresholds. |
| `retune_pii()` | Force re-tuning of PII confidence thresholds from accumulated feedback. |
| `pii_drift_check()` | Detect PII detection pattern drift between baseline and recent periods. |

---

## Adaptive AI Components

The framework's "adaptive" nature comes from four AI/ML components that work together:

### 1. Anomaly Detection (`src/quality/anomaly_detector.py`)

| Method | Algorithm | Use Case |
|--------|-----------|----------|
| Z-Score | Parametric (mean ± kσ) | Fast column-level outlier detection |
| IQR Fences | Non-parametric (Q1 − 1.5·IQR, Q3 + 1.5·IQR) | Robust to skewed distributions |
| Isolation Forest | scikit-learn ensemble | Multivariate anomaly detection |
| Batch Z-Score | Rolling history comparison | Cross-run DQ score anomaly detection |

### 2. Adaptive DQ Scoring (`src/quality/adaptive_scorer.py`)

- **Dynamic thresholds**: Instead of a hard-coded 85%, the threshold adapts based on the rolling mean and standard deviation of the last N pipeline runs.
- **Learned dimension weights**: Dimensions that historically score lower (more problematic) are weighted higher to surface weaknesses.
- **Linear regression weights**: A scikit-learn `LinearRegression` model learns which DQ dimensions best predict overall quality.
- **Early-warning system**: Detects declining trends (3 consecutive drops, negative slope) even when scores are still passing.

### 3. Feedback-Driven PII Tuning (`src/pii_detection/adaptive_pii_tuner.py`)

- **Feedback loop**: Records PII detection outcomes (true/false positive/negative) as ground-truth observations.
- **F1-optimal threshold search**: Tests a grid of candidate confidence thresholds and selects the one maximising F1 per entity type.
- **PII drift detection**: Compares recent false-negative rates against a historical baseline to alert when detection quality degrades.

### 4. Adaptive Governance Engine (`src/governance/adaptive_governance_engine.py`)

Orchestrates all three components into a single `evaluate()` call that:
1. Computes DQ metrics with learned weights
2. Runs statistical anomaly detection
3. Applies adaptive threshold (not hard-coded)
4. Checks early-warning trend signals
5. Detects batch-level anomalies
6. Records the run for future self-learning
7. Returns a **PASS / WARN / FAIL** decision
