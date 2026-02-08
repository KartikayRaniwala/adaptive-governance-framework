# 🤖 MASTER GITHUB COPILOT AGENT PROMPT
## Adaptive Data Governance Framework - Complete Implementation

### 🎯 OBJECTIVE
Build a production-ready, end-to-end **Adaptive Data Governance Framework** for E-Commerce platforms using PySpark, Delta Lake, and AI-powered PII detection. This is a dissertation-grade implementation requiring enterprise architecture patterns.

---

### 📋 PROJECT CONTEXT
**Domain:** Data Governance & Big Data Engineering
**Use Case:** DPDP Act 2023 compliant E-Commerce data processing
**Architecture:** Medallion (Bronze-Silver-Gold) + AI PII Detection + Airflow Orchestration
**Scale:** Handles 100M+ records, petabyte-ready
**Deployment:** Dockerized Spark cluster (1 master + 2 workers)

---

### 🏗️ IMPLEMENTATION REQUIREMENTS

#### 1. DATA INGESTION LAYER (`src/ingestion/`)

**File:** `data_loader.py`
- Multi-source data ingestion with schema inference and Delta Lake integration.
- Support for: CSV, Parquet, JSON.
- Incremental loading with watermark tracking.
- Auto-detect schema drift and log to Delta Lake transaction log.

**File:** `schema_registry.py`
- Schema versioning and registration.
- Drift comparison between incoming vs. existing schema.

#### 2. PII DETECTION ENGINE (`src/pii_detection/`)

**File:** `pii_detector.py`
- Transformer-based NER model (DistilBERT) for detecting PII in unstructured text.
- Fine-tuned for: EMAIL, PHONE, AADHAAR, PAN, CREDIT_CARD.
- Operates as Spark UDF for distributed processing.
- Confidence scoring with configurable threshold.

**File:** `pii_masker.py`
- Masking strategies: HASH (SHA-256), REDACT (***), TOKENIZE (format-preserving).
- Maintain referential integrity through consistent hashing.

**File:** `train_ner_model.py`
- Fine-tuning script for DistilBERT NER model on synthetic Indian PII data.
- BIO labelling scheme with HuggingFace Trainer API.

#### 3. DATA TRANSFORMATION LAYER (`src/transformation/`)

**File:** `bronze_to_silver.py`
- Deduplication via window functions.
- Great Expectations validation suite.
- PII masking UDFs.
- Data lineage metadata.

**File:** `silver_to_gold.py`
- Pre-aggregated metrics: revenue (daily/weekly/monthly), CLV, RFM segmentation.
- Broadcast joins for dimension tables.

**File:** `scd_manager.py`
- SCD Type 2 via Delta Lake MERGE.
- Hash-based change detection, version tracking.

#### 4. DATA QUALITY FRAMEWORK (`src/quality/`)

**File:** `data_quality_engine.py`
- Great Expectations suite builder from schema dict.
- Validate DataFrames with pass/fail gating.

**File:** `quality_metrics.py`
- Six dimensions: completeness, uniqueness, validity, timeliness, consistency, accuracy.
- Configurable weights with overall score computation.

#### 5. ORCHESTRATION (`airflow/dags/`)

**File:** `medallion_pipeline_dag.py`
- Daily DAG: generate → ingest → transform → quality gate → aggregate.
- Retry logic with exponential backoff, SLA monitoring (< 2 hours).

**File:** `pii_audit_dag.py`
- Weekly PII compliance audit scanning all Silver tables.

#### 6. UTILITY MODULES (`src/utils/`)

- `spark_utils.py` — Spark session factory with Delta Lake configuration.
- `config_loader.py` — YAML config loading with Pydantic validation.
- `logger.py` — Structured JSON logging with correlation IDs (Loguru).
- `data_generator.py` — Synthetic Indian e-commerce data (Faker); 5% nulls, 2% schema drift, PII injection.
- `roi_calculator.py` — Financial ROI calculator for governance investment.

---

### 🔧 TECHNICAL SPECIFICATIONS

#### Spark Configuration
```python
spark = SparkSession.builder \
    .appName("AdaptiveGovernance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
    .getOrCreate()
```

#### Delta Lake Schema Evolution
```python
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .option("optimizeWrite", "true") \
    .save("/path/to/delta/table")
```

---

### 📊 DELIVERABLES

```
adaptive-governance-framework/
├── README.md
├── COPILOT_MASTER_PROMPT.md
├── docker-compose.yml
├── Dockerfile.jupyter
├── requirements.txt
├── config/
│   └── config.yaml
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── data_loader.py
│   │   └── schema_registry.py
│   ├── pii_detection/
│   │   ├── __init__.py
│   │   ├── pii_detector.py
│   │   ├── pii_masker.py
│   │   └── train_ner_model.py
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   └── scd_manager.py
│   ├── quality/
│   │   ├── __init__.py
│   │   ├── data_quality_engine.py
│   │   └── quality_metrics.py
│   └── utils/
│       ├── __init__.py
│       ├── spark_utils.py
│       ├── config_loader.py
│       ├── logger.py
│       ├── data_generator.py
│       └── roi_calculator.py
├── airflow/
│   └── dags/
│       ├── medallion_pipeline_dag.py
│       └── pii_audit_dag.py
├── tests/
│   ├── __init__.py
│   ├── test_pii_detection.py
│   ├── test_data_quality.py
│   └── test_medallion_pipeline.py
├── scripts/
│   ├── deploy.sh
│   ├── download_data.sh
│   └── benchmark.py
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_pii_model_training.ipynb
│   └── 03_dq_analysis.ipynb
└── docs/
    ├── architecture.md
    ├── api_reference.md
    ├── deployment_guide.md
    └── dpdp_compliance.md
```

---

### 🚀 VALIDATION CRITERIA

**Success Metrics:**
- ✅ All Docker containers start successfully
- ✅ Spark UI accessible at http://localhost:8080
- ✅ Airflow DAG runs without errors
- ✅ PII detection F1-score > 0.92
- ✅ Data quality score > 90% for Silver layer
- ✅ End-to-end pipeline completes in < 2 hours
- ✅ Delta Lake time travel works (audit trail)
- ✅ All tests pass with >80% code coverage

---

### 🔐 COMPLIANCE CHECKLIST (DPDP Act 2023)

- PII automatically detected and masked
- Consent management flags in customer table
- Data retention policies enforced (7 years)
- Audit logs for all data access
- Right to erasure (delete customer data)
- Encryption at rest (Delta Lake)
- Anonymization for analytics datasets
