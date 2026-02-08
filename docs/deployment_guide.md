# Deployment Guide

## Adaptive Data Governance Framework — Complete Deployment Instructions

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Docker Configuration](#2-docker-configuration)
3. [Clone and Deploy](#3-clone-and-deploy)
4. [Verify Services](#4-verify-services)
5. [Run the Pipeline](#5-run-the-pipeline)
6. [Monitor Pipeline Execution](#6-monitor-pipeline-execution)
7. [Verify Pipeline Output](#7-verify-pipeline-output)
8. [Access Points](#8-access-points)
9. [Re-running the Pipeline](#9-re-running-the-pipeline)
10. [Stopping and Cleanup](#10-stopping-and-cleanup)
11. [Docker Services Architecture](#11-docker-services-architecture)
12. [Troubleshooting](#12-troubleshooting)
13. [Environment Variables](#13-environment-variables)

---

## 1. Prerequisites

| Requirement | Minimum | Recommended |
|---|---|---|
| macOS | 12 (Monterey) | 14 (Sonoma) |
| RAM | 16 GB | 32 GB |
| Disk | 50 GB free | 100 GB free |
| Docker Desktop | 4.25+ | Latest |
| Docker Compose | V2 (bundled with Docker Desktop) | — |

> **No local Python, Java, or Spark installation required.** Everything runs inside Docker containers.

### Install Docker Desktop

```bash
# macOS (Homebrew)
brew install --cask docker

# Or download from https://www.docker.com/products/docker-desktop/
```

Start Docker Desktop and ensure it is running (whale icon in menu bar).

---

## 2. Docker Configuration

**This step is critical.** The pipeline processes 500K+ orders with Spark and ML models.

Open **Docker Desktop → Settings → Resources** and set:

| Resource | Value | Why |
|---|---|---|
| CPUs | 6+ cores | Spark parallelism |
| Memory | **12 GB minimum** (16 GB ideal) | Spark + Airflow + NER model |
| Swap | 4 GB | Overflow for ML models |
| Disk | 50 GB | Delta Lake tables + Docker images |

Click **Apply & Restart**.

### Verify Docker

```bash
docker --version          # Docker version 24.x+
docker compose version    # Docker Compose version v2.x+
```

---

## 3. Clone and Deploy

### Clone the Repository

```bash
git clone https://github.com/KartikayRaniwala/adaptive-governance-framework.git
cd adaptive-governance-framework
```

### One-Command Deployment

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

**What `deploy.sh` does (in order):**

1. **Pre-flight checks** — Verifies Docker daemon is running, Compose V2 available
2. **Creates directories** — 24 required directories (data layers, logs, checkpoints, metrics)
3. **Sets environment** — Creates `.env` file with Airflow UID/GID
4. **Fixes permissions** — `chmod -R 777 airflow/logs`
5. **Cleans stale data** — Wipes all data layers (bronze, silver, gold, quarantine, streaming, metrics)
6. **Builds Docker images** — `docker compose build` (first build takes 10-15 minutes)
7. **Starts PostgreSQL** — Waits for health check
8. **Starts all services** — Spark Master/Worker, Airflow (webserver + scheduler), JupyterLab
9. **Health checks** — Polls Spark UI (8080), Airflow (8081), JupyterLab (8888)
10. **Clears Python caches** — Removes `__pycache__` and `.pyc` inside containers
11. **Triggers pipeline** — Unpauses DAG and triggers `medallion_pipeline_dag`
12. **Prints summary** — Access URLs and expected pipeline flow

### First Build

The first build downloads ~3 GB of Docker layers and Python packages. Subsequent builds are cached and take <30 seconds.

```
Expected first-run output:
  ✓ Docker daemon is running
  ✓ docker compose V2 available
  ...
  ✓ All services started
  ✓ Spark Master UI: http://localhost:8080
  ✓ Airflow UI: http://localhost:8081
  ✓ JupyterLab: http://localhost:8888
  ✓ Pipeline triggered!
```

---

## 4. Verify Services

After deployment, all 6 services should be running:

```bash
docker compose ps
```

Expected output:

| Service | Container | Status | Ports |
|---|---|---|---|
| PostgreSQL | `governance-postgres` | healthy | 5432 |
| Spark Master | `spark-master` | running | 8080, 7077 |
| Spark Worker | `spark-worker` | running | 8081 |
| Airflow Webserver | `governance-airflow-webserver` | healthy | 8081 |
| Airflow Scheduler | `governance-airflow-scheduler` | running | — |
| JupyterLab | `governance-jupyter` | running | 8888 |

### Check Container Logs

```bash
docker compose logs -f airflow-webserver   # Airflow logs
docker compose logs -f spark-master        # Spark logs
docker compose logs -f jupyterlab          # Jupyter logs
```

---

## 5. Run the Pipeline

### Automatic (via deploy.sh)

The `deploy.sh` script automatically triggers the pipeline. Just open Airflow to monitor.

### Manual Trigger

If you need to re-trigger:

**Option A: Airflow UI**
1. Open [http://localhost:8081](http://localhost:8081)
2. Login: `admin` / `admin`
3. Find `medallion_pipeline_dag`
4. Click the **Play** button (▶) → **Trigger DAG**

**Option B: CLI**
```bash
docker exec governance-airflow-webserver airflow dags trigger medallion_pipeline_dag
```

---

## 6. Monitor Pipeline Execution

### Airflow UI

1. Open [http://localhost:8081](http://localhost:8081)
2. Click on `medallion_pipeline_dag`
3. Switch to **Graph** view to see task dependencies
4. Click any task → **Log** to see detailed output

### Expected Task Order and Timing

| Task | Expected Duration | Depends On |
|---|---|---|
| `start` | Instant | — |
| `generate_synthetic_data` | 30–60s | start |
| `ingest_to_bronze` | 30–45s | generate_synthetic_data |
| `streaming_ingestion` | 60–90s | ingest_to_bronze |
| `bronze_to_silver` | 30–45s | ingest_to_bronze |
| `data_quality_check` | 30–60s | bronze_to_silver |
| `silver_to_gold` | 30–45s | data_quality_check |
| `pii_scan_summary` | 30–120s | silver_to_gold |
| `log_completion` | 10–20s | pii_scan_summary + streaming_ingestion |
| `end` | Instant | log_completion |

**Total expected runtime: 5–8 minutes** (first run includes NER model download).

### All Tasks Should Turn Green ✓

If any task turns red (failed), click on it → **Log** to see the error.

---

## 7. Verify Pipeline Output

### Check via Airflow Task Logs

Click on `log_completion` → **Log** to see the full summary:

```
  PIPELINE EXECUTION COMPLETE — FULL SUMMARY
  ======================================================================
  Bronze Layer:
    orders                                500,000 rows
    customers                             103,000 rows
    ...

  Governance Metrics:
    DQ Score:              92.81
    Decision:              PASS
    Adaptive Threshold:    85.0
    Combined Anomalies:    52,028
      Z-Score Anomalies:   2,528
      IQR Anomalies:       25,000
      Isolation Forest:    24,500

  AI / ML Models Executed:
    ✓ Z-Score Anomaly Detection (statistical)
    ✓ IQR Fence Anomaly Detection (statistical)
    ✓ Isolation Forest Anomaly Detection (sklearn ML)
    ✓ Adaptive DQ Threshold (rolling baseline + trend)
    ✓ Dimension Weight Learning (inverse-mean + regression)
    ✓ Early Warning System (trend monitoring)
    ✓ Batch Anomaly Detection (cross-run Z-score)
    ✓ PII Detection — Regex (8 patterns)
    ✓ PII Detection — NER (DistilBERT dslim/bert-base-NER)
    ✓ PII Confidence Tuner (F1-optimal threshold search)
    ✓ PII Drift Detection (baseline vs recent FN rates)
    ✓ Identity Resolution (Jaro-Winkler fuzzy matching)
```

### Check Data Files

```bash
# List Delta tables
ls -la data/bronze/
ls -la data/silver/
ls -la data/gold/
ls -la data/quarantine/

# Check governance reports
ls -la data/metrics/governance_reports/
cat data/metrics/governance_reports/silver_orders_*.json | head -100
```

---

## 8. Access Points

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **Airflow** | [http://localhost:8081](http://localhost:8081) | admin / admin | Pipeline monitoring, task logs, DAG management |
| **Spark UI** | [http://localhost:8080](http://localhost:8080) | — | Spark job monitoring, executor status |
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | token: `governance` | Interactive data exploration, notebook analysis |
| **PostgreSQL** | localhost:5432 | airflow / airflow | Airflow metadata (not user-facing) |

### Using JupyterLab

1. Open [http://localhost:8888](http://localhost:8888)
2. Enter token: `governance`
3. Navigate to `/opt/framework/notebooks/`
4. Create a new notebook or use existing ones
5. PySpark is pre-configured — `from src.utils.spark_utils import get_spark_session` works

---

## 9. Re-running the Pipeline

### Clean Re-run (Recommended)

Use `deploy.sh` again — it cleans all data, caches, and re-triggers:

```bash
./scripts/deploy.sh
```

### Quick Re-trigger (Without Cleanup)

```bash
docker exec governance-airflow-webserver airflow dags trigger medallion_pipeline_dag
```

> **Note**: Without cleanup, Delta tables will be overwritten but old metrics/history accumulate. This is fine — the adaptive models use this history to learn.

---

## 10. Stopping and Cleanup

### Stop Containers (Preserve Data)

```bash
docker compose down
```

### Stop and Remove Volumes

```bash
docker compose down -v
```

### Full Cleanup (Remove All)

```bash
docker compose down -v --rmi all
rm -rf data/bronze data/silver data/gold data/quarantine data/streaming data/metrics
```

### Teardown via Script

```bash
./scripts/deploy.sh --down
```

---

## 11. Docker Services Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network: governance-network        │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  PostgreSQL   │    │ Spark Master │    │ Spark Worker │  │
│  │  Port: 5432   │    │ UI: 8080     │    │              │  │
│  │  Airflow DB   │    │ Master: 7077 │    │ 4 GB memory  │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Airflow      │    │  Airflow     │    │  JupyterLab  │  │
│  │  Webserver    │    │  Scheduler   │    │  Port: 8888  │  │
│  │  Port: 8081   │    │  DAG runner  │    │  Token: gov. │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
│  Shared Volumes:                                             │
│    ./data:/opt/framework/data                                │
│    ./src:/opt/framework/src                                  │
│    ./airflow:/opt/airflow                                    │
│    ./config:/opt/framework/config                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 12. Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|---|---|---|
| `port already in use` | Another process on 8080/8081/8888 | `lsof -i :8081` → `kill <PID>` |
| `Airflow DB not initialized` | First run didn't init DB | `docker exec governance-airflow-webserver airflow db init` |
| `Out of memory` | Docker memory < 12 GB | Docker Desktop → Settings → Resources → Memory → 12+ GB |
| `ModuleNotFoundError: src.*` | PYTHONPATH not set | Verify `PYTHONPATH=/opt/framework` in docker-compose.yml |
| `Delta Lake JAR missing` | Spark packages not loaded | Verify `spark.jars.packages` in `src/utils/spark_utils.py` |
| `Task failed: data_quality_check` | DQ score below adaptive threshold | Check logs — may indicate genuine data quality issue |
| `NER model download timeout` | Network issue on first run | Re-run — model is cached after first download |
| `Streaming query error` | Checkpoint corruption | Delete `data/streaming/_checkpoints/` and re-run |
| `Permission denied` on logs | macOS Docker permissions | `chmod -R 777 airflow/logs` |
| Build fails on Apple Silicon | ARM64 compatibility | Use `platform: linux/amd64` in docker-compose.yml |

### Checking Container Health

```bash
# Service status
docker compose ps

# Container resource usage
docker stats --no-stream

# Check specific container logs
docker compose logs --tail=100 airflow-scheduler
```

### Resetting Everything

```bash
# Nuclear option: remove everything and start fresh
docker compose down -v --rmi all
rm -rf data/ airflow/logs/ __pycache__
./scripts/deploy.sh
```

---

## 13. Environment Variables

These are set in `docker-compose.yml` and can be overridden in a `.env` file:

| Variable | Default | Description |
|---|---|---|
| `SPARK_MASTER_URL` | `local[*]` (Airflow) | Spark execution mode |
| `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | Airflow metadata DB |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Airflow executor type |
| `JUPYTER_TOKEN` | `governance` | JupyterLab access token |
| `PYTHONPATH` | `/opt/framework` | Enables `from src.xxx import` |
| `JAVA_HOME` | `/usr/lib/jvm/java-17-openjdk-*` | Java for Spark |
| `DATA_ROOT` | `/opt/framework/data` | Base data lake path |

---

## Quick Reference

```bash
# Deploy
./scripts/deploy.sh

# Monitor
open http://localhost:8081          # Airflow UI

# Re-trigger
docker exec governance-airflow-webserver airflow dags trigger medallion_pipeline_dag

# View logs
docker compose logs -f airflow-scheduler

# Stop
docker compose down

# Full cleanup
docker compose down -v --rmi all
```
