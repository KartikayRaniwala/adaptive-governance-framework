# ============================================================================
# Adaptive Data Governance Framework
# src/utils/spark_utils.py
# ============================================================================
# Spark session factory with optimised configurations for Delta Lake,
# adaptive query execution, and the governance pipeline.
# ============================================================================

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession
from loguru import logger


def get_spark_session(
    app_name: str = "AdaptiveGovernance",
    master: Optional[str] = None,
    enable_delta: bool = True,
    executor_memory: str = "4g",
    driver_memory: str = "2g",
    shuffle_partitions: int = 200,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """Create or retrieve a SparkSession with governance-optimised settings.

    Parameters
    ----------
    app_name : str
        Name that appears in the Spark UI.
    master : str | None
        Spark master URL.  If *None* the ``SPARK_MASTER_URL`` env-var is
        checked, falling back to ``local[*]`` for local development.
    enable_delta : bool
        Whether to configure Delta Lake extensions.
    executor_memory : str
        Executor JVM heap size (e.g. ``"4g"``).
    driver_memory : str
        Driver JVM heap size.
    shuffle_partitions : int
        Default number of shuffle partitions.
    extra_configs : dict | None
        Arbitrary ``key: value`` Spark configs merged last.

    Returns
    -------
    SparkSession
        A configured, ready-to-use session.
    """

    if master is None:
        master = os.getenv("SPARK_MASTER_URL", "local[*]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # -- Memory --------------------------------------------------------
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.memory", driver_memory)
        # -- Adaptive Query Execution (AQE) --------------------------------
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # -- Miscellaneous --------------------------------------------------
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    # -- Delta Lake --------------------------------------------------------
    if enable_delta:
        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.retentionDurationCheck.enabled",
                "false",
            )
            .config(
                "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
                "true",
            )
            .config(
                "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
                "true",
            )
        )

    # -- Extra user-supplied overrides -------------------------------------
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    # -- Build session (with Delta pip JARs if available) -------------------
    if enable_delta:
        try:
            from delta import configure_spark_with_delta_pip
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            spark = builder.getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession initialised — app={app}, master={master}, delta={delta}",
        app=app_name,
        master=master,
        delta=enable_delta,
    )
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop a SparkSession."""
    if spark is not None:
        spark.stop()
        logger.info("SparkSession stopped.")
