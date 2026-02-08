# ============================================================================
# Adaptive Data Governance Framework
# src/transformation/silver_to_gold.py
# ============================================================================
# Silver → Gold aggregation pipeline.
# Business metrics, KPIs, and ML-ready feature engineering:
#   - Daily / weekly / monthly revenue by category
#   - Customer Lifetime Value (CLV)
#   - RFM (Recency, Frequency, Monetary) segmentation
#   - Churn-prediction features
# ============================================================================

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional

from loguru import logger
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


# ============================================================================
# SilverToGoldTransformer
# ============================================================================

class SilverToGoldTransformer:
    """Orchestrate the Silver → Gold aggregation step.

    Parameters
    ----------
    spark : SparkSession
    silver_path : str
        Root of the Silver Delta Lake layer.
    gold_path : str
        Root of the Gold Delta Lake layer.
    """

    def __init__(
        self,
        spark: SparkSession,
        silver_path: str = "data/silver",
        gold_path: str = "data/gold",
    ):
        self.spark = spark
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)

    # ------------------------------------------------------------------
    # Revenue aggregation
    # ------------------------------------------------------------------

    def aggregate_revenue(
        self,
        orders_df: DataFrame,
        granularities: Optional[List[str]] = None,
    ) -> DataFrame:
        """Pre-aggregate revenue by category at multiple granularities.

        Parameters
        ----------
        orders_df : DataFrame
            Silver-layer orders with ``order_timestamp``, ``product_category``,
            ``order_value``.
        granularities : list[str] | None
            Temporal granularities (``"daily"``, ``"weekly"``, ``"monthly"``).
            Defaults to all three.
        """
        if granularities is None:
            granularities = ["daily", "weekly", "monthly"]

        df = orders_df.withColumn(
            "order_date", F.to_date("order_timestamp")
        )

        results = []
        for grain in granularities:
            if grain == "daily":
                group_col = F.col("order_date")
            elif grain == "weekly":
                group_col = F.date_trunc("week", "order_date")
            else:
                group_col = F.date_trunc("month", "order_date")

            agg = (
                df
                .withColumn("period", group_col)
                .groupBy("period", "product_category")
                .agg(
                    F.sum("order_value").alias("total_revenue"),
                    F.count("order_id").alias("total_orders"),
                    F.avg("order_value").alias("avg_order_value"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                )
                .withColumn("granularity", F.lit(grain))
            )
            results.append(agg)

        from functools import reduce
        combined = reduce(DataFrame.union, results)

        logger.info(
            "Revenue aggregation complete — {} rows across {} granularities",
            combined.count(), len(granularities),
        )
        return combined

    # ------------------------------------------------------------------
    # RFM Segmentation
    # ------------------------------------------------------------------

    def compute_rfm(
        self,
        orders_df: DataFrame,
        reference_date: Optional[str] = None,
    ) -> DataFrame:
        """Calculate RFM (Recency, Frequency, Monetary) per customer.

        Parameters
        ----------
        orders_df : DataFrame
            Silver orders with ``customer_id``, ``order_timestamp``,
            ``order_value``.
        reference_date : str | None
            Reference date for recency (defaults to ``current_date()``).
        """
        ref = F.lit(reference_date) if reference_date else F.current_date()

        rfm = (
            orders_df
            .withColumn("order_date", F.to_date("order_timestamp"))
            .groupBy("customer_id")
            .agg(
                F.datediff(ref, F.max("order_date")).alias("recency_days"),
                F.count("order_id").alias("frequency"),
                F.sum("order_value").alias("monetary"),
            )
        )

        # Quintile scoring (1 = best)
        for metric, asc in [("recency_days", True), ("frequency", False), ("monetary", False)]:
            window = Window.orderBy(F.col(metric).asc() if asc else F.col(metric).desc())
            rfm = rfm.withColumn(
                f"{metric}_score",
                F.ntile(5).over(window),
            )

        rfm = rfm.withColumn(
            "rfm_segment",
            F.concat_ws(
                "",
                F.col("recency_days_score").cast("string"),
                F.col("frequency_score").cast("string"),
                F.col("monetary_score").cast("string"),
            ),
        )

        logger.info("RFM segmentation complete — {} customers", rfm.count())
        return rfm

    # ------------------------------------------------------------------
    # Customer Lifetime Value
    # ------------------------------------------------------------------

    def compute_clv(
        self,
        orders_df: DataFrame,
        avg_customer_lifespan_years: float = 3.0,
    ) -> DataFrame:
        """Estimate Customer Lifetime Value per customer.

        CLV = avg_order_value × purchase_frequency × lifespan
        """
        clv = (
            orders_df
            .groupBy("customer_id")
            .agg(
                F.avg("order_value").alias("avg_order_value"),
                F.count("order_id").alias("total_orders"),
                F.datediff(F.max("order_timestamp"), F.min("order_timestamp")).alias("customer_tenure_days"),
            )
            .withColumn(
                "purchase_frequency_yearly",
                F.when(
                    F.col("customer_tenure_days") > 0,
                    F.col("total_orders") / (F.col("customer_tenure_days") / 365.0),
                ).otherwise(F.col("total_orders")),
            )
            .withColumn(
                "estimated_clv",
                F.col("avg_order_value")
                * F.col("purchase_frequency_yearly")
                * F.lit(avg_customer_lifespan_years),
            )
        )

        logger.info("CLV computation complete — {} customers", clv.count())
        return clv

    # ------------------------------------------------------------------
    # Churn features
    # ------------------------------------------------------------------

    def compute_churn_features(
        self,
        orders_df: DataFrame,
        churn_threshold_days: int = 90,
    ) -> DataFrame:
        """Engineer churn-prediction features per customer.

        A customer is flagged as *churned* if their last order is
        older than *churn_threshold_days*.
        """
        features = (
            orders_df
            .groupBy("customer_id")
            .agg(
                F.max("order_timestamp").alias("last_order_date"),
                F.count("order_id").alias("order_count"),
                F.sum("order_value").alias("total_spend"),
                F.avg("order_value").alias("avg_spend"),
                F.stddev("order_value").alias("spend_stddev"),
                F.countDistinct("product_category").alias("category_diversity"),
            )
            .withColumn(
                "days_since_last_order",
                F.datediff(F.current_date(), F.to_date("last_order_date")),
            )
            .withColumn(
                "is_churned",
                F.when(
                    F.col("days_since_last_order") > churn_threshold_days, 1
                ).otherwise(0),
            )
        )

        logger.info("Churn features computed — {} customers", features.count())
        return features

    # ------------------------------------------------------------------
    # Write to Gold
    # ------------------------------------------------------------------

    def write_to_gold(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Persist a Gold-layer DataFrame as a Delta table."""
        path = str(self.gold_path / table_name)

        enriched = df.withColumn("_gold_processed_at", F.current_timestamp())

        writer = (
            enriched.write
            .format("delta")
            .mode("overwrite")
            .option("optimizeWrite", "true")
        )

        if partition_cols:
            # Use static partition overwrite to avoid the
            # DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE error
            # that occurs when Delta's dynamic partition overwrite mode
            # conflicts with schema evolution.
            writer = (
                writer
                .option("partitionOverwriteMode", "static")
                .partitionBy(*partition_cols)
            )
        else:
            # overwriteSchema is safe only when NOT using partitionBy
            writer = writer.option("overwriteSchema", "true")

        writer.save(path)
        logger.info("Written Gold table — {t}, rows={r}", t=table_name, r=df.count())
        return path

    # ------------------------------------------------------------------
    # End-to-end pipeline
    # ------------------------------------------------------------------

    def transform_all(self) -> None:
        """Run the full Silver → Gold pipeline for orders.

        Produces:
        - ``gold/revenue_aggregates``
        - ``gold/customer_rfm``
        - ``gold/customer_clv``
        - ``gold/churn_features``
        """
        logger.info("Starting Silver → Gold transformation")

        orders = self.spark.read.format("delta").load(
            str(self.silver_path / "orders")
        )

        # Revenue
        revenue = self.aggregate_revenue(orders)
        self.write_to_gold(revenue, "revenue_aggregates", partition_cols=["granularity"])

        # RFM
        rfm = self.compute_rfm(orders)
        self.write_to_gold(rfm, "customer_rfm")

        # CLV
        clv = self.compute_clv(orders)
        self.write_to_gold(clv, "customer_clv")

        # Churn features
        churn = self.compute_churn_features(orders)
        self.write_to_gold(churn, "churn_features")

        logger.info("Silver → Gold transformation complete.")
