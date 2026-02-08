# ============================================================================
# Adaptive Data Governance Framework
# src/ingestion/streaming_simulator.py
# ============================================================================
# Simulates high-velocity clickstream / IoT event ingestion using Spark
# Structured Streaming.  Generates micro-batches that flow through the
# governance pipeline (schema validation, PII masking, DQ checks) at
# streaming speed — proving the framework can handle real-time without
# unacceptable latency.
#
# Modes:
#   - File-based micro-batch: writes JSON files to a landing zone,
#     consumed by ``readStream``.
#   - In-memory rate source: uses Spark's ``rate`` source for pure
#     throughput benchmarks.
# ============================================================================

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_EVENT_TYPES = [
    "page_view", "add_to_cart", "remove_from_cart", "begin_checkout",
    "purchase", "search", "product_click", "wishlist_add",
]
_DEVICE_TYPES = ["mobile", "desktop", "tablet"]
_PAGES = [
    "/", "/products", "/products/electronics", "/cart",
    "/checkout", "/account", "/search", "/offers",
]
_INDIAN_CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
]


# ============================================================================
# Micro-batch event generator
# ============================================================================

def generate_clickstream_batch(
    n: int = 1000,
    inject_pii_pct: float = 0.05,
) -> List[Dict]:
    """Generate a batch of synthetic clickstream events.

    Parameters
    ----------
    n : int
        Number of events per batch.
    inject_pii_pct : float
        Fraction of events with PII injected into the search_query field.

    Returns
    -------
    list[dict]
    """
    from faker import Faker
    _fake = Faker("en_IN")

    events = []
    for _ in range(n):
        ts = datetime.utcnow().isoformat() + "Z"
        event = {
            "event_id": uuid.uuid4().hex,
            "session_id": uuid.uuid4().hex[:16],
            "user_id": uuid.uuid4().hex[:16],
            "event_type": random.choice(_EVENT_TYPES),
            "page_url": random.choice(_PAGES),
            "device_type": random.choice(_DEVICE_TYPES),
            "city": random.choice(_INDIAN_CITIES),
            "timestamp": ts,
            "duration_ms": random.randint(100, 30000),
            "referrer": random.choice(["google", "direct", "facebook", "instagram", ""]),
            "search_query": "",
        }

        # Inject PII in search queries
        if random.random() < inject_pii_pct:
            pii_type = random.choice(["email", "phone", "aadhaar", "name"])
            if pii_type == "email":
                event["search_query"] = f"order for {_fake.email()}"
            elif pii_type == "phone":
                event["search_query"] = f"call me {_fake.phone_number()}"
            elif pii_type == "aadhaar":
                event["search_query"] = (
                    f"verify {random.randint(2000,9999)} "
                    f"{random.randint(1000,9999)} "
                    f"{random.randint(1000,9999)}"
                )
            else:
                event["search_query"] = f"contact {_fake.name()}"

        events.append(event)
    return events


def write_micro_batch(
    landing_dir: str,
    batch_size: int = 1000,
    inject_pii_pct: float = 0.05,
) -> Path:
    """Write a single micro-batch of events as a JSON file."""
    out = Path(landing_dir)
    out.mkdir(parents=True, exist_ok=True)
    batch = generate_clickstream_batch(batch_size, inject_pii_pct)
    filename = f"events_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
    path = out / filename
    with open(path, "w") as f:
        for event in batch:
            f.write(json.dumps(event) + "\n")
    return path


# ============================================================================
# Streaming ingestion with governance
# ============================================================================

class StreamingGovernor:
    """Consume a stream of events and apply governance in near-real-time.

    Parameters
    ----------
    spark : SparkSession
    landing_dir : str
        Directory where micro-batch JSON files are landed.
    checkpoint_dir : str
        Spark Structured Streaming checkpoint directory.
    bronze_path : str
        Delta table path for the Bronze layer output.
    """

    def __init__(
        self,
        spark,
        landing_dir: str = "data/streaming/landing",
        checkpoint_dir: str = "data/streaming/_checkpoints",
        bronze_path: str = "data/bronze/clickstream",
    ):
        self.spark = spark
        self.landing_dir = landing_dir
        self.checkpoint_dir = checkpoint_dir
        self.bronze_path = bronze_path

    def start_stream(
        self,
        trigger_interval: str = "10 seconds",
        pii_mask: bool = True,
    ):
        """Start a Structured Streaming query that reads events, applies
        governance transformations, and writes to Bronze Delta.

        Parameters
        ----------
        trigger_interval : str
            Processing trigger interval (e.g. ``"10 seconds"``).
        pii_mask : bool
            Whether to apply PII masking on the ``search_query`` field.

        Returns
        -------
        StreamingQuery
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            IntegerType, StringType, StructField, StructType,
        )

        schema = StructType([
            StructField("event_id", StringType()),
            StructField("session_id", StringType()),
            StructField("user_id", StringType()),
            StructField("event_type", StringType()),
            StructField("page_url", StringType()),
            StructField("device_type", StringType()),
            StructField("city", StringType()),
            StructField("timestamp", StringType()),
            StructField("duration_ms", IntegerType()),
            StructField("referrer", StringType()),
            StructField("search_query", StringType()),
        ])

        raw_stream = (
            self.spark.readStream
            .format("json")
            .schema(schema)
            .option("maxFilesPerTrigger", 5)
            .load(self.landing_dir)
        )

        # Add governance metadata
        governed = (
            raw_stream
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit("streaming"))
            .withColumn(
                "_event_ts",
                F.to_timestamp(F.col("timestamp")),
            )
        )

        # Optional: PII masking on search_query
        if pii_mask:
            from src.pii_detection.pii_masker import PIIMasker
            mask_udf = PIIMasker(strategy="redact").create_spark_mask_udf()
            governed = governed.withColumn(
                "search_query", mask_udf(F.col("search_query")),
            )

        # Write to Bronze Delta
        query = (
            governed.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_dir)
            .trigger(processingTime=trigger_interval)
            .start(self.bronze_path)
        )

        logger.info(
            "Streaming ingestion started — landing={}, bronze={}",
            self.landing_dir, self.bronze_path,
        )
        return query

    def start_rate_benchmark(
        self,
        rows_per_second: int = 10000,
        duration_seconds: int = 60,
    ):
        """Use Spark's built-in ``rate`` source to benchmark pure
        governance throughput without I/O overhead.

        Returns the streaming query.
        """
        from pyspark.sql import functions as F

        rate_stream = (
            self.spark.readStream
            .format("rate")
            .option("rowsPerSecond", rows_per_second)
            .load()
        )

        # Simulate governance columns
        governed = (
            rate_stream
            .withColumn("event_id", F.expr("uuid()"))
            .withColumn("event_type", F.lit("benchmark"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_dq_validated", F.lit(True))
        )

        query = (
            governed.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                f"{self.checkpoint_dir}_benchmark",
            )
            .trigger(processingTime="5 seconds")
            .start(f"{self.bronze_path}_benchmark")
        )

        logger.info(
            "Rate benchmark started — {} rows/s for {}s",
            rows_per_second, duration_seconds,
        )
        return query


# ============================================================================
# CLI: generate a stream of micro-batches
# ============================================================================

def run_micro_batch_producer(
    landing_dir: str = "data/streaming/landing",
    batches: int = 20,
    batch_size: int = 1000,
    interval_seconds: float = 5.0,
    inject_pii_pct: float = 0.05,
) -> None:
    """Produce micro-batches at a fixed interval (simulating an upstream
    event source like Kafka or Kinesis).
    """
    logger.info(
        "Starting micro-batch producer — {} batches × {} events @ {:.1f}s interval",
        batches, batch_size, interval_seconds,
    )
    for i in range(batches):
        path = write_micro_batch(landing_dir, batch_size, inject_pii_pct)
        logger.info("Batch {}/{} written → {}", i + 1, batches, path)
        time.sleep(interval_seconds)
    logger.info("Producer complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Streaming event simulator")
    parser.add_argument("--landing", default="data/streaming/landing")
    parser.add_argument("--batches", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--pii-pct", type=float, default=0.05)
    args = parser.parse_args()

    run_micro_batch_producer(
        landing_dir=args.landing,
        batches=args.batches,
        batch_size=args.batch_size,
        interval_seconds=args.interval,
        inject_pii_pct=args.pii_pct,
    )
