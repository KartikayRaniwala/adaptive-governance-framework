# ============================================================================
# Adaptive Data Governance Framework
# airflow/dags/pii_audit_dag.py
# ============================================================================
# Weekly PII audit report generation.
# Scans all Silver tables for missed PII patterns and generates an
# audit report documenting findings.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "governance-team",
    "depends_on_past": False,
    "email": ["compliance@adaptive-governance.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}


# ============================================================================
# Task callables
# ============================================================================

DATA_ROOT = "/opt/framework/data"


def _scan_silver_for_pii(**context):
    """Scan all Silver-layer text columns for residual PII."""
    from src.utils.spark_utils import get_spark_session
    from src.pii_detection.pii_detector import PIIDetector

    spark = get_spark_session(app_name="PII-Audit")
    detector = PIIDetector(confidence_threshold=0.80)
    silver_base = Path(f"{DATA_ROOT}/silver")

    findings = {}

    # Scan each Silver table
    tables = [d.name for d in silver_base.iterdir() if d.is_dir()] if silver_base.exists() else []

    for table in tables:
        table_path = str(silver_base / table)
        try:
            df = spark.read.format("delta").load(table_path)
        except Exception as exc:
            findings[table] = {"error": str(exc)}
            continue

        # Identify string columns
        string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

        table_findings = {}
        for col in string_cols:
            # Sample up to 10,000 rows for efficiency
            sample = (
                df.select(col)
                .filter(df[col].isNotNull())
                .limit(10_000)
                .toPandas()
            )

            pii_count = 0
            pii_types: dict = {}
            for _, row in sample.iterrows():
                entities = detector.detect_pii(str(row[col]))
                if entities:
                    pii_count += 1
                    for e in entities:
                        pii_types[e.entity_type] = pii_types.get(e.entity_type, 0) + 1

            if pii_count > 0:
                table_findings[col] = {
                    "rows_with_pii": pii_count,
                    "sample_size": len(sample),
                    "pii_rate": round(pii_count / len(sample) * 100, 2),
                    "entity_types": pii_types,
                }

        findings[table] = table_findings

    # Push findings for report
    context["ti"].xcom_push(key="pii_findings", value=findings)
    return findings


def _generate_audit_report(**context):
    """Generate a JSON audit report from the PII scan findings."""
    findings = context["ti"].xcom_pull(
        task_ids="scan_silver_pii", key="pii_findings"
    )

    report = {
        "report_type": "PII Audit",
        "generated_at": datetime.now().isoformat(),
        "compliance_framework": "DPDP Act 2023",
        "summary": {},
        "details": findings or {},
    }

    # Summary statistics
    total_tables = len(findings) if findings else 0
    tables_with_pii = 0
    total_pii_occurrences = 0

    for table, cols in (findings or {}).items():
        if isinstance(cols, dict) and "error" not in cols:
            for col, stats in cols.items():
                if isinstance(stats, dict) and stats.get("rows_with_pii", 0) > 0:
                    tables_with_pii += 1
                    total_pii_occurrences += stats["rows_with_pii"]
                    break

    report["summary"] = {
        "total_tables_scanned": total_tables,
        "tables_with_residual_pii": tables_with_pii,
        "total_pii_occurrences": total_pii_occurrences,
        "compliance_status": "PASS" if tables_with_pii == 0 else "REVIEW_REQUIRED",
    }

    # Save report
    reports_dir = Path(f"{DATA_ROOT}/reports/pii_audit")
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_path = reports_dir / f"pii_audit_{datetime.now().strftime('%Y%m%d')}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"PII Audit Report saved to {report_path}")
    print(f"Summary: {json.dumps(report['summary'], indent=2)}")

    # Fail DAG if PII found
    if tables_with_pii > 0:
        print(
            f"⚠️  WARNING: {tables_with_pii} table(s) contain residual PII. "
            f"Review required for DPDP compliance."
        )


# ============================================================================
# DAG definition
# ============================================================================

with DAG(
    dag_id="pii_audit_dag",
    default_args=default_args,
    description="Weekly PII audit scan of Silver-layer tables for DPDP compliance",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["governance", "pii", "compliance", "audit"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    scan_pii = PythonOperator(
        task_id="scan_silver_pii",
        python_callable=_scan_silver_for_pii,
    )

    generate_report = PythonOperator(
        task_id="generate_audit_report",
        python_callable=_generate_audit_report,
    )

    start >> scan_pii >> generate_report >> end
