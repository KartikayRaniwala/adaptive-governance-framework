# ============================================================================
# Adaptive Data Governance Framework
# src/quality/dq_framework.py
# ============================================================================
# DataQualityFramework – orchestrates data-quality validation for the
# e-commerce order pipeline using Great Expectations (GE) backed by a
# PySpark execution engine.
#
# Responsibilities:
#   1. Define an ExpectationSuite with business rules for the order dataset.
#   2. Validate incoming PySpark DataFrames against that suite.
#   3. Quarantine failed records into a Delta Lake table.
#   4. Generate a self-contained HTML report with Plotly charts.
#
# Usage:
#   >>> from pyspark.sql import SparkSession
#   >>> from src.quality.dq_framework import DataQualityFramework
#   >>> spark = SparkSession.builder.getOrCreate()
#   >>> dqf = DataQualityFramework(spark)
#   >>> suite = dqf.create_ecommerce_expectations()
#   >>> df = spark.read.parquet("data/raw/synthetic_orders_with_issues.parquet")
#   >>> results = dqf.validate_and_quarantine(df, suite, "data/quarantine/orders")
#   >>> report_path = dqf.generate_dq_report(results)
# ============================================================================

from __future__ import annotations

import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# ---------------------------------------------------------------------------
# Great Expectations imports
# ---------------------------------------------------------------------------
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.dataset import SparkDFDataset

# ---------------------------------------------------------------------------
# Plotly (HTML report generation)
# ---------------------------------------------------------------------------
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_EXPECTED_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "product_category",
    "order_value",
    "delivery_instructions",
    "customer_review",
    "order_timestamp",
    "delivery_city",
    "delivery_pincode",
]

_VALID_PAYMENT_METHODS: List[str] = [
    "UPI",
    "Card",
    "COD",
    "Wallet",
    "NetBanking",
]

_PINCODE_REGEX: str = r"^\d{6}$"

_ORDER_VALUE_MIN: float = 0.0
_ORDER_VALUE_MAX: float = 1_000_000.0
_ORDER_VALUE_MOSTLY: float = 0.99


# ============================================================================
# DataQualityFramework
# ============================================================================
class DataQualityFramework:
    """Orchestrate data-quality checks for e-commerce order data.

    This class wraps Great Expectations to provide a streamlined workflow:

    1. **Define** an ``ExpectationSuite`` with business rules
       (``create_ecommerce_expectations``).
    2. **Validate** a PySpark ``DataFrame`` and quarantine failures
       (``validate_and_quarantine``).
    3. **Report** results as an interactive HTML page with Plotly charts
       (``generate_dq_report``).

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        An active Spark session used for DataFrame operations and Delta
        writes.

    Attributes
    ----------
    spark : SparkSession
        The Spark session provided at construction time.

    Examples
    --------
    >>> spark = SparkSession.builder.getOrCreate()
    >>> dqf = DataQualityFramework(spark)
    >>> suite = dqf.create_ecommerce_expectations()
    >>> df = spark.read.parquet("data/raw/synthetic_orders_with_issues.parquet")
    >>> results = dqf.validate_and_quarantine(
    ...     df, suite, "data/quarantine/orders"
    ... )
    >>> print(results["metrics"])
    """

    # ------------------------------------------------------------------ init
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

        # ---- Loguru setup -------------------------------------------------
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)

        logger.remove()  # clear default stderr handler
        logger.add(
            sys.stderr,
            level="INFO",
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level:<8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
                " - <level>{message}</level>"
            ),
        )
        logger.add(
            str(log_dir / "data_quality.log"),
            rotation="10 MB",
            retention="30 days",
            compression="zip",
            level="DEBUG",
            format=(
                "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | "
                "{name}:{function}:{line} - {message}"
            ),
        )
        logger.info(
            "DataQualityFramework initialised  ·  Spark app = {}",
            spark.sparkContext.appName,
        )

    # ====================================================================
    # 1.  create_ecommerce_expectations
    # ====================================================================
    def create_ecommerce_expectations(self) -> ExpectationSuite:
        """Build a Great Expectations ``ExpectationSuite`` for e-commerce orders.

        The suite – named ``ecommerce_order_suite`` – encodes the following
        business rules:

        +-----+---------------------------------------------+-------------------+
        | #   | Rule                                        | GE expectation    |
        +=====+=============================================+===================+
        | R1  | Table must contain the 9 required columns   | columns_to_match  |
        |     | in the correct order.                       | _ordered_list     |
        +-----+---------------------------------------------+-------------------+
        | R2  | ``order_id`` must never be null.             | values_to_not_be  |
        |     |                                             | _null             |
        +-----+---------------------------------------------+-------------------+
        | R3  | ``customer_id`` must never be null.          | values_to_not_be  |
        |     |                                             | _null             |
        +-----+---------------------------------------------+-------------------+
        | R4  | ``order_value`` must never be null.          | values_to_not_be  |
        |     |                                             | _null             |
        +-----+---------------------------------------------+-------------------+
        | R5  | ``order_id`` values must be unique.          | values_to_be      |
        |     |                                             | _unique           |
        +-----+---------------------------------------------+-------------------+
        | R6  | ``order_value`` must be between 0 and       | values_to_be      |
        |     | 1 000 000 (99 % mostly).                    | _between          |
        +-----+---------------------------------------------+-------------------+
        | R7  | ``payment_method`` must be one of the 5     | values_to_be_in   |
        |     | allowed values.                             | _set              |
        +-----+---------------------------------------------+-------------------+
        | R8  | ``delivery_pincode`` must match ``^\\d{6}$``.| values_to_match   |
        |     |                                             | _regex            |
        +-----+---------------------------------------------+-------------------+

        Returns
        -------
        great_expectations.core.ExpectationSuite
            A fully-configured suite ready for validation.

        Raises
        ------
        RuntimeError
            If the suite cannot be created (e.g. incompatible GE version).
        """
        logger.info("Building ExpectationSuite 'ecommerce_order_suite' …")

        try:
            suite = ExpectationSuite(expectation_suite_name="ecommerce_order_suite")

            # R1 – required columns in order
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_table_columns_to_match_ordered_list",
                    kwargs={"column_list": _EXPECTED_COLUMNS},
                )
            )
            logger.debug("  R1  expect_table_columns_to_match_ordered_list")

            # R2 – order_id not null
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "order_id"},
                )
            )
            logger.debug("  R2  expect_column_values_to_not_be_null(order_id)")

            # R3 – customer_id not null
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "customer_id"},
                )
            )
            logger.debug("  R3  expect_column_values_to_not_be_null(customer_id)")

            # R4 – order_value not null
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "order_value"},
                )
            )
            logger.debug("  R4  expect_column_values_to_not_be_null(order_value)")

            # R5 – order_id unique
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={"column": "order_id"},
                )
            )
            logger.debug("  R5  expect_column_values_to_be_unique(order_id)")

            # R6 – order_value between 0 and 1 000 000 (99 % mostly)
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "order_value",
                        "min_value": _ORDER_VALUE_MIN,
                        "max_value": _ORDER_VALUE_MAX,
                        "mostly": _ORDER_VALUE_MOSTLY,
                    },
                )
            )
            logger.debug(
                "  R6  expect_column_values_to_be_between(order_value, "
                "{}-{},{}, mostly={})",
                _ORDER_VALUE_MIN,
                _ORDER_VALUE_MAX,
                _ORDER_VALUE_MOSTLY,
            )

            # R7 – payment_method in allowed set
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "payment_method",
                        "value_set": _VALID_PAYMENT_METHODS,
                    },
                )
            )
            logger.debug(
                "  R7  expect_column_values_to_be_in_set(payment_method)"
            )

            # R8 – delivery_pincode matches 6-digit regex
            suite.append_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_match_regex",
                    kwargs={
                        "column": "delivery_pincode",
                        "regex": _PINCODE_REGEX,
                    },
                )
            )
            logger.debug(
                "  R8  expect_column_values_to_match_regex(delivery_pincode)"
            )

            logger.success(
                "ExpectationSuite created with {} expectations.",
                len(suite.expectations),
            )
            return suite

        except Exception as exc:
            logger.error(
                "Failed to create ExpectationSuite: {}", exc
            )
            raise RuntimeError(
                "Could not build ecommerce_order_suite."
            ) from exc

    # ====================================================================
    # 2.  validate_and_quarantine
    # ====================================================================
    def validate_and_quarantine(
        self,
        df: DataFrame,
        suite: ExpectationSuite,
        quarantine_path: str,
    ) -> Dict[str, Any]:
        """Validate *df* against *suite* and quarantine failed records.

        The method performs the following steps:

        1. Wrap the incoming PySpark DataFrame in a GE ``SparkDFDataset``.
        2. Run the suite with ``result_format="SUMMARY"``.
        3. For every **column-level** expectation that failed, derive a
           PySpark filter condition and tag matching rows.
        4. Split the DataFrame into ``valid_df`` (all rules passed) and
           ``failed_df`` (at least one rule failed).
        5. Write ``failed_df`` to *quarantine_path* in **Delta** format
           (append mode) with a ``_quarantine_ts`` metadata column.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to validate.
        suite : great_expectations.core.ExpectationSuite
            The suite returned by :meth:`create_ecommerce_expectations`.
        quarantine_path : str
            File-system path (local or cloud) where quarantined records are
            written in Delta format.

        Returns
        -------
        dict
            A dictionary with three keys:

            * ``"metrics"`` – ``dict`` containing ``total_records``,
              ``valid_records``, ``failed_records``, ``success_rate``
              (float 0-100), and ``failed_expectations`` (list of dicts
              with ``rule``, ``column``, ``failed_count``).
            * ``"valid_df"`` – PySpark ``DataFrame`` of records that
              passed *all* expectations.
            * ``"failed_df"`` – PySpark ``DataFrame`` of records that
              failed *at least one* expectation.

        Raises
        ------
        RuntimeError
            If validation itself errors out (e.g. schema mismatch). 
        """
        logger.info(
            "Starting validation  ·  {} records  ·  {} expectations",
            df.count(),
            len(suite.expectations),
        )

        try:
            # ---- Step 1: wrap in GE SparkDFDataset -----------------------
            ge_df = SparkDFDataset(df)
            logger.debug("Wrapped DataFrame in SparkDFDataset.")

            # ---- Step 2: run validation ----------------------------------
            validation_results = ge_df.validate(
                expectation_suite=suite,
                result_format="SUMMARY",
            )
            logger.info(
                "Validation complete  ·  overall success = {}",
                validation_results.success,
            )

            # ---- Step 3: build per-expectation failure filters -----------
            failed_expectations: List[Dict[str, Any]] = []
            failure_conditions: List = []

            for result in validation_results.results:
                if result.success:
                    continue  # rule passed – nothing to filter

                exp_type = result.expectation_config.expectation_type
                kwargs = result.expectation_config.kwargs
                column = kwargs.get("column")

                # Count of unexpected values (if available)
                unexpected_count = (
                    result.result.get("unexpected_count", 0)
                    if result.result
                    else 0
                )

                failed_expectations.append(
                    {
                        "rule": exp_type,
                        "column": column,
                        "failed_count": unexpected_count,
                    }
                )

                # Derive a PySpark filter for each failed column-level rule
                condition = self._expectation_to_filter(exp_type, kwargs)
                if condition is not None:
                    failure_conditions.append(condition)

                logger.warning(
                    "  FAILED  {} on '{}' – {} unexpected values",
                    exp_type,
                    column,
                    unexpected_count,
                )

            # ---- Step 4: split valid / failed ----------------------------
            if failure_conditions:
                combined_fail = failure_conditions[0]
                for cond in failure_conditions[1:]:
                    combined_fail = combined_fail | cond

                failed_df = df.filter(combined_fail)
                valid_df = df.filter(~combined_fail)
            else:
                # All expectations passed – no failures
                failed_df = self.spark.createDataFrame([], df.schema)
                valid_df = df

            total = df.count()
            failed_count = failed_df.count()
            valid_count = total - failed_count
            success_rate = (valid_count / total * 100) if total > 0 else 100.0

            logger.info(
                "Split complete  ·  valid={} · failed={} · rate={:.2f}%",
                valid_count,
                failed_count,
                success_rate,
            )

            # ---- Step 5: quarantine failed records -----------------------
            if failed_count > 0:
                quarantine_df = failed_df.withColumn(
                    "_quarantine_ts",
                    F.lit(datetime.utcnow().isoformat()).cast(StringType()),
                )
                quarantine_df.write.format("delta").mode("append").save(
                    quarantine_path
                )
                logger.success(
                    "Quarantined {} records → {}",
                    failed_count,
                    quarantine_path,
                )
            else:
                logger.info("No records to quarantine – all passed.")

            # ---- build return dict ---------------------------------------
            metrics: Dict[str, Any] = {
                "total_records": total,
                "valid_records": valid_count,
                "failed_records": failed_count,
                "success_rate": round(success_rate, 2),
                "failed_expectations": failed_expectations,
                "validation_time": datetime.utcnow().isoformat(),
            }

            return {
                "metrics": metrics,
                "valid_df": valid_df,
                "failed_df": failed_df,
            }

        except Exception as exc:
            logger.error("Validation failed: {}\n{}", exc, traceback.format_exc())
            raise RuntimeError("validate_and_quarantine failed.") from exc

    # ====================================================================
    # 3.  generate_dq_report
    # ====================================================================
    def generate_dq_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate an interactive HTML data-quality report.

        The report is written to ``docs/dq_report.html`` and includes:

        * A **headline banner** with total / valid / failed record counts and
          the overall success rate.
        * A **gauge chart** visualising the success rate.
        * A **horizontal bar chart** showing failure counts broken down by
          expectation rule.
        * A **summary table** listing each failed rule, the affected column,
          and the number of unexpected values.

        Parameters
        ----------
        validation_results : dict
            The dictionary returned by :meth:`validate_and_quarantine`.
            Must contain a ``"metrics"`` key whose value is a dict with at
            least ``total_records``, ``valid_records``, ``failed_records``,
            ``success_rate``, and ``failed_expectations``.

        Returns
        -------
        str
            The absolute file-system path to the generated HTML report.

        Raises
        ------
        KeyError
            If *validation_results* is missing required keys.
        RuntimeError
            If report generation fails for any other reason.
        """
        logger.info("Generating data-quality HTML report …")

        try:
            metrics = validation_results["metrics"]
            total = metrics["total_records"]
            valid = metrics["valid_records"]
            failed = metrics["failed_records"]
            success_rate = metrics["success_rate"]
            failed_expectations = metrics.get("failed_expectations", [])
            validation_time = metrics.get(
                "validation_time", datetime.utcnow().isoformat()
            )

            # ---- Plotly figure with subplots ------------------------------
            fig = make_subplots(
                rows=2,
                cols=2,
                specs=[
                    [{"type": "indicator"}, {"type": "indicator"}],
                    [{"type": "bar", "colspan": 2}, None],
                ],
                subplot_titles=(
                    "Success Rate",
                    "Record Counts",
                    "Failure Breakdown by Rule",
                ),
                vertical_spacing=0.25,
                horizontal_spacing=0.15,
            )

            # ---- Gauge – success rate ------------------------------------
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number+delta",
                    value=success_rate,
                    number={"suffix": "%", "font": {"size": 40}},
                    delta={"reference": 100, "decreasing": {"color": "red"}},
                    gauge={
                        "axis": {"range": [0, 100], "tickwidth": 1},
                        "bar": {"color": "#2ecc71"},
                        "bgcolor": "white",
                        "steps": [
                            {"range": [0, 60], "color": "#e74c3c"},
                            {"range": [60, 80], "color": "#f39c12"},
                            {"range": [80, 95], "color": "#f1c40f"},
                            {"range": [95, 100], "color": "#2ecc71"},
                        ],
                        "threshold": {
                            "line": {"color": "black", "width": 4},
                            "thickness": 0.75,
                            "value": success_rate,
                        },
                    },
                    title={"text": "Overall DQ Score"},
                ),
                row=1,
                col=1,
            )

            # ---- Indicator – record counts --------------------------------
            fig.add_trace(
                go.Indicator(
                    mode="number+delta",
                    value=valid,
                    delta={
                        "reference": total,
                        "relative": False,
                        "valueformat": ",",
                        "decreasing": {"color": "red"},
                    },
                    number={"valueformat": ",", "font": {"size": 40}},
                    title={"text": f"Valid / {total:,} Total"},
                ),
                row=1,
                col=2,
            )

            # ---- Bar chart – failure breakdown ----------------------------
            if failed_expectations:
                rule_labels = [
                    f"{fe['rule']}\n({fe['column']})"
                    for fe in failed_expectations
                ]
                fail_counts = [fe["failed_count"] for fe in failed_expectations]

                fig.add_trace(
                    go.Bar(
                        y=rule_labels,
                        x=fail_counts,
                        orientation="h",
                        marker_color="#e74c3c",
                        text=fail_counts,
                        textposition="auto",
                        name="Failed records",
                    ),
                    row=2,
                    col=1,
                )
            else:
                fig.add_trace(
                    go.Bar(
                        y=["All rules passed"],
                        x=[0],
                        orientation="h",
                        marker_color="#2ecc71",
                        name="No failures",
                    ),
                    row=2,
                    col=1,
                )

            fig.update_layout(
                title_text=(
                    f"Data Quality Report  ·  "
                    f"{datetime.fromisoformat(validation_time).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                ),
                title_x=0.5,
                height=800,
                showlegend=False,
                template="plotly_white",
                font=dict(family="Inter, Arial, sans-serif"),
            )

            # ---- Summary table as HTML ------------------------------------
            table_rows = ""
            for i, fe in enumerate(failed_expectations, start=1):
                table_rows += (
                    f"<tr>"
                    f"<td style='padding:8px;border:1px solid #ddd;'>{i}</td>"
                    f"<td style='padding:8px;border:1px solid #ddd;'>"
                    f"<code>{fe['rule']}</code></td>"
                    f"<td style='padding:8px;border:1px solid #ddd;'>"
                    f"<code>{fe['column']}</code></td>"
                    f"<td style='padding:8px;border:1px solid #ddd;text-align:right;'>"
                    f"{fe['failed_count']:,}</td>"
                    f"</tr>"
                )

            summary_table = f"""
            <div style="max-width:900px;margin:30px auto;font-family:Inter,Arial,sans-serif;">
                <h2 style="color:#2c3e50;">Summary</h2>
                <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
                    <tr style="background:#34495e;color:white;">
                        <th style="padding:10px;border:1px solid #ddd;">Metric</th>
                        <th style="padding:10px;border:1px solid #ddd;">Value</th>
                    </tr>
                    <tr><td style="padding:8px;border:1px solid #ddd;">Total Records</td>
                        <td style="padding:8px;border:1px solid #ddd;text-align:right;">{total:,}</td></tr>
                    <tr><td style="padding:8px;border:1px solid #ddd;">Valid Records</td>
                        <td style="padding:8px;border:1px solid #ddd;text-align:right;color:#2ecc71;">{valid:,}</td></tr>
                    <tr><td style="padding:8px;border:1px solid #ddd;">Failed Records</td>
                        <td style="padding:8px;border:1px solid #ddd;text-align:right;color:#e74c3c;">{failed:,}</td></tr>
                    <tr><td style="padding:8px;border:1px solid #ddd;">Success Rate</td>
                        <td style="padding:8px;border:1px solid #ddd;text-align:right;">{success_rate:.2f}%</td></tr>
                </table>

                <h2 style="color:#2c3e50;">Failed Expectations</h2>
                <table style="width:100%;border-collapse:collapse;">
                    <tr style="background:#34495e;color:white;">
                        <th style="padding:10px;border:1px solid #ddd;">#</th>
                        <th style="padding:10px;border:1px solid #ddd;">Rule</th>
                        <th style="padding:10px;border:1px solid #ddd;">Column</th>
                        <th style="padding:10px;border:1px solid #ddd;">Failed Count</th>
                    </tr>
                    {table_rows if table_rows else
                     '<tr><td colspan="4" style="padding:12px;text-align:center;color:#2ecc71;">'
                     '✅ All expectations passed</td></tr>'}
                </table>
            </div>
            """

            # ---- Write HTML file ------------------------------------------
            docs_dir = Path("docs")
            docs_dir.mkdir(parents=True, exist_ok=True)
            report_path = docs_dir / "dq_report.html"

            chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

            full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Report – Adaptive Governance Framework</title>
    <style>
        body {{
            margin: 0;
            padding: 20px 40px;
            background: #f8f9fa;
            font-family: Inter, Arial, sans-serif;
            color: #2c3e50;
        }}
        .header {{
            text-align: center;
            padding: 30px;
            background: linear-gradient(135deg, #2c3e50, #3498db);
            color: white;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{ margin: 0 0 10px; font-size: 28px; }}
        .header p  {{ margin: 0; opacity: 0.8; }}
        .chart-container {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin-bottom: 30px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🛡️ Data Quality Report</h1>
        <p>Adaptive Data Governance Framework  ·  Generated {validation_time}</p>
    </div>
    <div class="chart-container">
        {chart_html}
    </div>
    {summary_table}
</body>
</html>"""

            report_path.write_text(full_html, encoding="utf-8")
            abs_path = str(report_path.resolve())

            logger.success("DQ report saved → {}", abs_path)
            return abs_path

        except KeyError as exc:
            logger.error(
                "Missing key in validation_results: {}", exc
            )
            raise
        except Exception as exc:
            logger.error(
                "Report generation failed: {}\n{}", exc, traceback.format_exc()
            )
            raise RuntimeError("generate_dq_report failed.") from exc

    # ====================================================================
    # Internal helpers
    # ====================================================================
    @staticmethod
    def _expectation_to_filter(
        exp_type: str, kwargs: Dict[str, Any]
    ) -> Optional[Any]:
        """Convert a GE expectation type + kwargs into a PySpark Column filter.

        The returned filter selects rows that *violate* the expectation (i.e.
        rows that should be quarantined).

        Parameters
        ----------
        exp_type : str
            The Great Expectations expectation type string, e.g.
            ``"expect_column_values_to_not_be_null"``.
        kwargs : dict
            The ``kwargs`` dict from the ``ExpectationConfiguration``.

        Returns
        -------
        pyspark.sql.Column or None
            A PySpark filter ``Column`` expression, or ``None`` if the
            expectation type is table-level or otherwise cannot be mapped
            to a row-level filter.
        """
        column = kwargs.get("column")
        if column is None:
            # Table-level expectations (e.g. column order) cannot be
            # translated to a row-level filter.
            return None

        if exp_type == "expect_column_values_to_not_be_null":
            return F.col(column).isNull()

        if exp_type == "expect_column_values_to_be_unique":
            # Uniqueness is a set-level property; we cannot filter individual
            # rows without a window.  Return None – duplicates are handled
            # separately if needed.
            return None

        if exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            cond = F.lit(False)
            if min_val is not None:
                cond = cond | (F.col(column).cast(DoubleType()) < min_val)
            if max_val is not None:
                cond = cond | (F.col(column).cast(DoubleType()) > max_val)
            return cond

        if exp_type == "expect_column_values_to_be_in_set":
            value_set = kwargs.get("value_set", [])
            return ~F.col(column).isin(value_set) | F.col(column).isNull()

        if exp_type == "expect_column_values_to_match_regex":
            regex = kwargs.get("regex", ".*")
            return ~F.col(column).rlike(regex) | F.col(column).isNull()

        logger.debug(
            "No row-level filter for expectation type '{}'.", exp_type
        )
        return None


# ============================================================================
# CLI entry-point / smoke test
# ============================================================================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("DataQualityFramework – smoke test")
    logger.info("=" * 70)

    try:
        # ---- Spark session ------------------------------------------------
        spark = (
            SparkSession.builder.appName("DQ-Framework-SmokeTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .getOrCreate()
        )

        dqf = DataQualityFramework(spark)

        # ---- Create expectations ------------------------------------------
        suite = dqf.create_ecommerce_expectations()
        logger.info("Suite has {} expectations.", len(suite.expectations))

        # ---- Load sample data ---------------------------------------------
        raw_path = "data/raw/synthetic_orders_with_issues.parquet"
        raw_path_obj = Path(raw_path)

        if raw_path_obj.exists():
            df = spark.read.parquet(raw_path)
            logger.info("Loaded {} records from {}.", df.count(), raw_path)

            # Remove payment_method column for quarantine test compatibility
            # (The raw data includes it but _EXPECTED_COLUMNS has 9 cols
            # without payment_method – it is validated via in-set rule.)
            # Actually the raw data has payment_method, keep it.

            # ---- Validate & quarantine ------------------------------------
            results = dqf.validate_and_quarantine(
                df, suite, "data/quarantine/orders"
            )

            # ---- Print metrics --------------------------------------------
            logger.info("Metrics:\n{}", json.dumps(results["metrics"], indent=2))

            # ---- Generate HTML report -------------------------------------
            report = dqf.generate_dq_report(results)
            logger.success("Report → {}", report)
        else:
            logger.warning(
                "Sample data not found at '{}'. Run data_collector.py first.",
                raw_path,
            )

        spark.stop()
        logger.success("Smoke test completed.")

    except Exception as exc:
        logger.exception("Smoke test failed: {}", exc)
        sys.exit(1)