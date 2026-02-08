# ============================================================================
# Adaptive Data Governance Framework
# src/quality/data_quality_engine.py
# ============================================================================
# Data Quality Engine using Great Expectations.
# Implements declarative data contracts for the Medallion Architecture.
# ============================================================================

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

try:
    from great_expectations.dataset import SparkDFDataset
except ImportError:
    SparkDFDataset = None  # GE >= 0.18 removed this; custom validation used


class DataQualityEngine:
    """Adaptive Data Quality Engine for E-Commerce Governance.

    Responsibilities:
      - Schema validation against data contracts
      - Statistical profiling and anomaly detection
      - Business-rule validation
      - Quarantine management for failed records

    Parameters
    ----------
    spark : SparkSession
    config_path : str
        Path to the YAML configuration file.
    """

    def __init__(
        self,
        spark: SparkSession,
        config_path: str = "config/config.yaml",
    ):
        self.spark = spark
        self.config = self._load_config(config_path)

    @staticmethod
    def _load_config(path: str) -> Dict:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                return yaml.safe_load(f) or {}
        return {}

    # ------------------------------------------------------------------
    # Expectation suite builder
    # ------------------------------------------------------------------

    def create_expectation_suite(
        self,
        suite_name: str,
        schema: Dict,
        business_rules: Optional[List[Dict]] = None,
    ) -> ExpectationSuite:
        """Build a Great Expectations suite from a schema dict.

        Parameters
        ----------
        suite_name : str
            Logical name for the suite.
        schema : dict
            ``{column_name: {type, nullable, unique, min_value, max_value, regex}}``.
        business_rules : list[dict] | None
            Extra ``{"type": ..., "params": ...}`` expectations.

        Returns
        -------
        ExpectationSuite
        """
        suite = ExpectationSuite(expectation_suite_name=suite_name)

        # Column-ordered list expectation
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={"column_list": list(schema.keys())},
            )
        )

        for col_name, col_def in schema.items():
            # Not-null
            if not col_def.get("nullable", False):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={"column": col_name},
                    )
                )
            # Unique
            if col_def.get("unique"):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_unique",
                        kwargs={"column": col_name},
                    )
                )
            # Range
            if col_def.get("min_value") is not None:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_between",
                        kwargs={
                            "column": col_name,
                            "min_value": col_def["min_value"],
                            "max_value": col_def.get("max_value"),
                        },
                    )
                )
            # Regex
            if col_def.get("regex"):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_match_regex",
                        kwargs={"column": col_name, "regex": col_def["regex"]},
                    )
                )

        # Business rules
        if business_rules:
            for rule in business_rules:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type=rule["type"],
                        kwargs=rule["params"],
                    )
                )

        logger.info(
            "Expectation suite '{}' created with {} expectations.",
            suite_name, len(suite.expectations),
        )
        return suite

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_dataframe(
        self,
        df: DataFrame,
        suite: ExpectationSuite,
    ) -> Dict:
        """Validate a Spark DataFrame against an expectation suite.

        Returns
        -------
        dict
            Validation result dictionary including ``success``,
            ``dq_score``, and individual expectation results.
        """
        if SparkDFDataset is None:
            raise RuntimeError(
                "great_expectations.dataset.SparkDFDataset is not available "
                "in this version of Great Expectations. Use GE < 0.18 or "
                "switch to the GX 0.18+ validation API."
            )

        ge_df = SparkDFDataset(df)

        results_list = []
        for exp in suite.expectations:
            try:
                method = getattr(ge_df, exp.expectation_type)
                result = method(**exp.kwargs)
                results_list.append({
                    "expectation": exp.expectation_type,
                    "kwargs": exp.kwargs,
                    "success": result.success,
                })
            except Exception as exc:
                results_list.append({
                    "expectation": exp.expectation_type,
                    "kwargs": exp.kwargs,
                    "success": False,
                    "error": str(exc),
                })

        total = len(results_list)
        passed = sum(1 for r in results_list if r["success"])
        dq_score = passed / total if total else 0.0

        logger.info(
            "Validation complete — passed={p}/{t}, DQ score={s:.2%}",
            p=passed, t=total, s=dq_score,
        )

        return {
            "success": all(r["success"] for r in results_list),
            "dq_score": dq_score,
            "passed": passed,
            "total": total,
            "results": results_list,
            "timestamp": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Dataset profiling
    # ------------------------------------------------------------------

    def profile_dataset(self, df: DataFrame, output_path: str) -> Dict:
        """Generate statistical profile and save as JSON.

        Parameters
        ----------
        df : DataFrame
        output_path : str
            Path to save the JSON profile.

        Returns
        -------
        dict
        """
        row_count = df.count()
        profile = {
            "row_count": row_count,
            "column_count": len(df.columns),
            "columns": {},
            "timestamp": datetime.now().isoformat(),
        }

        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            distinct_count = df.select(col).distinct().count()
            col_stats: Dict = {
                "dtype": str(df.schema[col].dataType),
                "null_count": null_count,
                "null_pct": round(null_count / row_count * 100, 2) if row_count else 0,
                "distinct_count": distinct_count,
            }

            dtype_str = df.schema[col].dataType.simpleString()
            if dtype_str in ("int", "bigint", "long", "double", "float"):
                summary = df.select(col).summary("mean", "stddev", "min", "max").collect()
                col_stats.update({
                    "mean": float(summary[0][1]) if summary[0][1] else None,
                    "stddev": float(summary[1][1]) if summary[1][1] else None,
                    "min": float(summary[2][1]) if summary[2][1] else None,
                    "max": float(summary[3][1]) if summary[3][1] else None,
                })

            profile["columns"][col] = col_stats

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(profile, f, indent=2)

        logger.info("Dataset profile saved → {}", output_path)
        return profile


# ============================================================================
# Schema definitions (orders example)
# ============================================================================

OLIST_ORDERS_SCHEMA: Dict = {
    "order_id": {"type": "string", "nullable": False, "unique": True},
    "customer_id": {"type": "string", "nullable": False},
    "order_status": {"type": "string", "nullable": False},
    "order_purchase_timestamp": {"type": "timestamp", "nullable": False},
    "order_approved_at": {"type": "timestamp", "nullable": True},
    "order_delivered_customer_date": {"type": "timestamp", "nullable": True},
    "order_estimated_delivery_date": {"type": "timestamp", "nullable": False},
    "order_total_amount": {"type": "double", "nullable": False, "min_value": 0.0},
}

ECOMMERCE_ORDERS_SCHEMA: Dict = {
    "order_id": {"type": "string", "nullable": False, "unique": True},
    "customer_id": {"type": "string", "nullable": False},
    "product_category": {"type": "string", "nullable": False},
    "order_value": {"type": "double", "nullable": False, "min_value": 0.0},
    "delivery_instructions": {"type": "string", "nullable": True},
    "order_timestamp": {"type": "string", "nullable": False},
    "delivery_city": {"type": "string", "nullable": False},
    "delivery_pincode": {"type": "string", "nullable": False, "regex": r"^\d{6}$"},
}
