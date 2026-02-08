# Architecture Guide
## Adaptive Data Governance Framework — System Design Deep-Dive

---

## 1. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     ADAPTIVE GOVERNANCE LAYER                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   BRONZE     │  │    SILVER    │  │     GOLD     │      │
│  │   (Raw)      │─▶│  (Validated) │─▶│  (Enriched)  │      │
│  │              │  │              │  │              │      │
│  │ - Schema     │  │ - DQ Checks  │  │ - Business   │      │
│  │   Validation │  │ - PII Mask   │  │   Metrics    │      │
│  │ - Delta Lake │  │ - Dedup      │  │ - ML Ready   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │          AI-POWERED PII DETECTION ENGINE             │    │
│  │   (DistilBERT NER + Regex + Great Expectations DQ)  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │           APACHE AIRFLOW ORCHESTRATION              │    │
│  │  (DAGs for Bronze→Silver→Gold Pipeline + PII Audit) │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Medallion Architecture

### 2.1 Bronze Layer (Raw)
| Aspect | Detail |
|--------|--------|
| **Format** | Delta Lake (Parquet + transaction log) |
| **Write mode** | Append-only — immutable audit trail |
| **Metadata columns** | `_ingested_at`, `_source_file`, `_batch_id` |
| **Schema enforcement** | Schema-on-read with drift detection |
| **Retention** | 90 days via Delta Lake VACUUM |

### 2.2 Silver Layer (Validated)
| Aspect | Detail |
|--------|--------|
| **Format** | Delta Lake |
| **Deduplication** | Window-function based on primary key + order by timestamp |
| **PII handling** | Columns are masked (hash / redact / tokenize) before write |
| **Data quality gate** | Overall DQ score must exceed 85 % |
| **SCD Type 2** | Dimension tables tracked with `valid_from`, `valid_to`, `is_current` |

### 2.3 Gold Layer (Enriched)
| Aspect | Detail |
|--------|--------|
| **Format** | Delta Lake |
| **Aggregations** | Revenue (daily/weekly/monthly), RFM, CLV, churn features |
| **Optimisation** | Broadcast joins for small dimension tables, Z-ordering |

---

## 3. Data Flow

```
External Sources (CSV / Parquet / JSON)
        │
        ▼
  ┌─────────────┐
  │ DataLoader   │  src/ingestion/data_loader.py
  │  + Schema    │  src/ingestion/schema_registry.py
  │   Registry   │
  └──────┬──────┘
         │  write_to_bronze()
         ▼
  ┌──────────────┐
  │ Bronze Layer │  data/bronze/<table>/
  └──────┬───────┘
         │
         ▼
  ┌────────────────────────┐
  │ BronzeToSilverTransformer │
  │  - deduplicate()           │
  │  - validate_not_null()     │
  │  - mask_pii_columns()      │
  │  - write_quarantine()      │
  └──────┬─────────────────┘
         │  write_to_silver()
         ▼
  ┌──────────────┐
  │ Silver Layer │  data/silver/<table>/
  └──────┬───────┘
         │
         ▼
  ┌────────────────────────┐
  │ SilverToGoldTransformer │
  │  - aggregate_revenue()  │
  │  - compute_rfm()        │
  │  - compute_clv()        │
  └──────┬─────────────────┘
         │  write_to_gold()
         ▼
  ┌──────────────┐
  │  Gold Layer  │  data/gold/<table>/
  └──────────────┘
```

---

## 4. PII Detection & Masking Pipeline

```
  Input Text ──▶ Regex Scanner ──▶ NER Model (DistilBERT) ──▶ Entity List
                                                                    │
                                                                    ▼
                                                             Masking Engine
                                                           ┌────────────────┐
                                                           │ hash  (SHA-256)│
                                                           │ redact  (***)  │
                                                           │ tokenize (FPE) │
                                                           └────────────────┘
                                                                    │
                                                                    ▼
                                                             Masked Text
```

**Supported PII types:**
- EMAIL, PHONE_NUMBER, AADHAAR, PAN, CREDIT_CARD, IPV4, PERSON (via NER)

---

## 5. Data Quality Framework

Six quality dimensions are computed for every dataset:

| Dimension | Description | Weight |
|-----------|-------------|--------|
| Completeness | % of non-null values | 0.25 |
| Uniqueness | % of distinct values in key columns | 0.20 |
| Validity | % of values passing format / range checks | 0.20 |
| Timeliness | Age of newest record relative to expectation | 0.15 |
| Consistency | Cross-column logical checks | 0.10 |
| Accuracy | Proxy — referential integrity + range adherence | 0.10 |

Quality gate in Airflow fails the pipeline when overall score < 85 %.

---

## 6. Orchestration (Airflow)

### `medallion_pipeline_dag` — Daily at 02:00 UTC
```
generate_synthetic_data
       │
       ▼
ingest_to_bronze
       │
       ▼
bronze_to_silver
       │
       ▼
data_quality_check  ──(fail if DQ < 85 %)──▶ quarantine
       │
       ▼
silver_to_gold
       │
       ▼
log_completion
```

### `pii_audit_dag` — Weekly (Sundays)
```
scan_silver_pii ──▶ generate_audit_report
```

---

## 7. Infrastructure (Docker Compose)

| Service | Image | Ports |
|---------|-------|-------|
| Spark Master | apache/spark:3.5.0 | 8080, 7077 |
| Spark Worker | apache/spark:3.5.0 | — |
| JupyterLab | Custom (Dockerfile.jupyter) | 8888 |
| PostgreSQL | postgres:15-alpine | 5432 |
| Airflow Webserver | apache/airflow:2.8.0-python3.11 | 8081 |
| Airflow Scheduler | apache/airflow:2.8.0-python3.11 | — |

All containers share a common Docker network (`governance-network`) and mount the project data volume.

---

## 8. Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Delta Lake over plain Parquet** | ACID transactions, time travel, schema evolution |
| **DistilBERT over full BERT** | 2× faster inference with < 3% accuracy loss |
| **Regex + NER** | Regex catches structured PII (Aadhaar, PAN); NER handles unstructured (names) |
| **Great Expectations** | Declarative DQ with built-in profiling; wide community support |
| **Airflow** | Industry-standard orchestration; native Spark operator support |
| **Loguru** | Structured JSON logging out of the box; simpler than stdlib `logging` |
| **Pydantic for config** | Validated, typed configuration prevents runtime surprises |
