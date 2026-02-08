#!/usr/bin/env python3
# ============================================================================
# Adaptive Data Governance Framework
# scripts/benchmark.py
# ============================================================================
# Performance benchmarking script for the full Medallion pipeline.
# ============================================================================

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List

from loguru import logger


def benchmark_pipeline(record_count: int, base_dir: str = "data") -> Dict:
    """Run a complete Bronze → Silver → Gold cycle and measure throughput.

    Parameters
    ----------
    record_count : int
        Number of order records to generate.
    base_dir : str
        Root data directory.

    Returns
    -------
    dict
        Timing and throughput results.
    """
    from src.utils.spark_utils import get_spark_session
    from src.utils.data_generator import generate_orders
    from src.ingestion.data_loader import DataLoader
    from src.transformation.bronze_to_silver import BronzeToSilverTransformer
    from src.transformation.silver_to_gold import SilverToGoldTransformer

    spark = get_spark_session(app_name="Benchmark")

    # -- Generate --------------------------------------------------------
    logger.info("Generating {:,} records …", record_count)
    start = time.time()
    pdf = generate_orders(n=record_count)
    gen_time = time.time() - start

    # -- Bronze ----------------------------------------------------------
    logger.info("Writing to Bronze …")
    raw_path = f"{base_dir}/raw/benchmark_orders.parquet"
    pdf.to_parquet(raw_path, index=False)
    loader = DataLoader(spark, base_data_path=base_dir)
    df = loader.load_from_parquet(raw_path)

    bronze_start = time.time()
    loader.write_to_bronze(df, table_name="benchmark_orders")
    bronze_time = time.time() - bronze_start

    # -- Silver ----------------------------------------------------------
    logger.info("Transforming to Silver …")
    silver_start = time.time()
    transformer = BronzeToSilverTransformer(
        spark,
        bronze_path=f"{base_dir}/bronze",
        silver_path=f"{base_dir}/silver",
        quarantine_path=f"{base_dir}/quarantine",
    )
    silver_df = transformer.transform_orders(
        table_name="benchmark_orders",
        pii_columns=["delivery_instructions"],
        masking_strategy="redact",
    )
    silver_time = time.time() - silver_start

    # -- Gold ------------------------------------------------------------
    logger.info("Aggregating to Gold …")
    gold_start = time.time()
    gold_t = SilverToGoldTransformer(
        spark,
        silver_path=f"{base_dir}/silver",
        gold_path=f"{base_dir}/gold",
    )
    revenue = gold_t.aggregate_revenue(silver_df, granularities=["daily", "monthly"])
    gold_t.write_to_gold(revenue, "benchmark_revenue")
    gold_time = time.time() - gold_start

    total_time = time.time() - start

    result = {
        "record_count": record_count,
        "generation_time_s": round(gen_time, 2),
        "bronze_time_s": round(bronze_time, 2),
        "silver_time_s": round(silver_time, 2),
        "gold_time_s": round(gold_time, 2),
        "total_time_s": round(total_time, 2),
        "throughput_rec_per_s": round(record_count / total_time, 0),
    }

    logger.info("\n" + "=" * 60)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 60)
    for k, v in result.items():
        logger.info(f"  {k}: {v:>12}")
    logger.info("=" * 60)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark the medallion pipeline")
    parser.add_argument("--counts", nargs="+", type=int, default=[10_000, 50_000, 100_000])
    parser.add_argument("--output", default="data/benchmark_results.json")
    args = parser.parse_args()

    results: List[Dict] = []
    for count in args.counts:
        res = benchmark_pipeline(count)
        results.append(res)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Results saved to {}", args.output)


if __name__ == "__main__":
    main()
